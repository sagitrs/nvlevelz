#ifndef MULTITABLE_H
#define MULTITABLE_H
#include "trie_compressed.h"
//#include "skiplist_nonvolatile.h"
//#include "nvskiplist.h"
#include "l2skiplist.h"
#include "d4skiplist.h"
#include "skiplist_kvindram.h"
#include "memtable_dram_skiplist.h"
#include "nvm_manager.h"
#include "db/memtable.h"
#include "mem_node_cache.h"
//using std::shared_mutex;
#include "nvmemtable.h"
#include <unordered_set>
#include <leveldb/env.h>
#include "backgroundwriter.h"
#include "backgroundwriter_lockfree.h"
#include <deque>
#include "db/version_set.h"
#include "port/port_posix.h"
#include "info_collector.h"
//#include "linearhash.h"
#include "nvhash.h"
#include "nvhashtable.h"
#include "hashtablehelper.h"

namespace leveldb {

class DBImpl;
using std::unordered_set;
class nvMultiTableIterator;

struct Level0Version {
    std::vector<nvMemTable*> list_;
    int refs_;
    Level0Version() : list_(), refs_(0) {}
    void Ref(){ refs_ ++; }
    void Unref() { refs_ --; assert(refs_ >= 0); if (refs_ == 0) delete this; }
    bool Get(const LookupKey& lkey, std::string* value, Status* s) {
      Slice key = lkey.user_key();
      for (std::vector<nvMemTable*>::iterator p = list_.begin(); p != list_.end(); ++p) {
          nvMemTable* imm = *p;
          if ( key.compare(imm->LeftBound()) >= 0 && key.compare(imm->RightBound()) < 0 && imm->Get(lkey, value, s))
              return true;
      }
      return false;
    }
private:
    ~Level0Version() {
        for (std::vector<nvMemTable*>::iterator p = list_.begin(); p != list_.end(); ++p) {
            (*p)->Unref();
        }
    }
};

struct nvMultiTable {
public:
    enum FullType {NOT_FULL, MEM_FULL, LIST_FULL, GLOBAL_FULL};
private:
    Options options_;
    enum MemType {WITH_BUFFER_AND_LOG, MSL_ONLY};
    const MemType memType_;
    const InternalKeyComparator comparator_;
    Env * env_;
    NVM_Manager* mng_;
public:
    leveldb::port::RWLock rwlock_;
    CachePolicy cache_policy_;
private:
    const std::string dbname_;

    typedef CTrie IndexTree;
    IndexTree index_;

    const size_t writer_num_;
    BackgroundWriter_LockFree **writers_;
public:
    //std::deque<nvMemTable*> level0_;
    MyQueue leveli_;
    BackgroundHelper* helper_;
    static const ul PrepareListSize = 4;

    MyQueue level0_;
    int64_t bytes_;
    int32_t oprs_;
    bool isCompactingLevel0_;
    //InfoCollector ic_;
    InfoCollector global_ic_;
    //LinearHash cache_;
    nvHashTable *cache_, *imm_cache_;

private:
    //FullType full_type_;
    std::unordered_set<ull> file_in_use_;

    std::string prev_poped_;
    size_t node_total_;

    ull seq_;
    ull log_number_;
    bool clear_mark_;

    static std::string commonPrefix(const std::string& s1, const std::string& s2);
    void BuildWriters(NVM_Manager* mng, const Options& options, const string &dbname);
    //void Separate(nvMemTable* N1, std::string T1_bound, std::string T3_bound);

public:
    nvMultiTable(DBImpl* db, const Options& options, const string &dbname);
    ~nvMultiTable() {
        ReleaseAll();
        //for (int i = 0; i < writer_num_; ++i)
        //    delete writers_[i];
        //delete[] writers_;
    }
    size_t Hash(const Slice &s) {
        size_t x = 0;
        if (s.size() >= 1) {
            x += s[0];
            x += s[s.size() - 1];
        }
        if (s.size() >= 2) {
            x += s[1];
            x += s[s.size() - 2];
        }
        return x % writer_num_;
    }
    BackgroundWriter_LockFree* WhoIs(const Slice& lft_bound) {
        if (lft_bound.size() == 0)
            return writers_[0];
        return writers_[Hash(lft_bound)];
    }
    void ClearWriteBuffer();
    void FillLevelI() {
        size_t size = leveli_.Size();
        for (int i = size; i < PrepareListSize; ++i)
            PrepareNewHashTable();
    }
    void PrepareNewHashTable() {
        nvFixedHashTable* table = new nvFixedHashTable(mng_, cache_policy_, dbname_, log_number_++, false);
        leveli_.PushBack(&table);
        helper_->PushWorkToQueue(table, BackgroundHelper::WorkType::Clean);
    }
    nvMemTable* WhereIs(const Slice& key);
    bool GetInLevel0(const LookupKey& lkey, std::string* value, Status* s) {
      level0_.lock_.ReadLock();
      Slice key = lkey.user_key();
      ull head = level0_.Head(), tail = level0_.Tail();
      for (ull i = head; i != tail; i = (i + 1) % level0_.size_) {
          nvMemTable* imm = *reinterpret_cast<nvMemTable**>(level0_[i]);
          if (key.compare(imm->LeftBound()) < 0)
              continue;
          if (imm->RightBound() != "" && key.compare(imm->RightBound()) >= 0)
              continue;
          if (imm->Get(lkey, value, s)) {
              level0_.lock_.Unlock();
              return true;
          }
      }
      level0_.lock_.Unlock();

      /*
      for (std::deque<nvMemTable*>::iterator p = level0_.begin(); p != level0_.end(); ++p) {
          nvMemTable* imm = *p;
          if (key.compare(imm->LeftBound()) < 0)
              continue;
          if (imm->RightBound() != "" && key.compare(imm->RightBound()) >= 0)
              continue;
          if (imm->Get(lkey, value, s))
              return true;
      }*/
      return false;
    }
    bool DeleteKey(const string& key);
    bool HasRoomForNewMem();
    //void SetFull(nvMemTable* mem, FullType type);
    nvMemTable* GetMaxMemTable() {
        IndexIterator* iter = NewIndexIterator();
        ull maxSize = 0;
        nvMemTable* maxMem = nullptr;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            nvMemTable* mem = (iter->Data());
            if (mem->StorageUsage() >= options_.TEST_pop_limit && mem->StorageUsage() >= maxSize) {
                maxSize = mem->StorageUsage();
                maxMem = mem;
            }
        }
        delete iter;
        return maxMem;
    }
    void ForcePop(Version *current, const Slice* key);
    void Delete(const Slice& key);
    void Pop(Version* current, const Slice& key);
    void InitPop(nvMemTable* mem);
    void Inserts(nvMemTable* mem);
    nvMemTable* PopFromLevel0() {
        nvMemTable* mem = nullptr;
        level0_.PopFront(&mem);
        //nvMemTable* mem = level0_.front();
        //level0_.pop_front();
        return mem;
    }

    bool ReleaseAll();

    ll StorageUsage() const;
    void SetFileInUse(ull file_number, bool in_use);
    bool FileInUse(ull file_number) const;
    void Print();
    void Init(MemTable* mem);
    void ByteCount(int64_t size) {
        bytes_ += size;
    }
    void CheckIndexValid();

    friend class nvMultiTableIterator;
    Iterator* NewIterator();
    IndexIterator* NewIndexIterator();

};

class nvMultiTableIterator: public Iterator {
 public:
  virtual ~nvMultiTableIterator();
  explicit nvMultiTableIterator(nvMultiTable* mt);

  virtual bool Valid() const;
  virtual void Seek(const Slice& k);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();
  virtual Slice key() const;
  virtual Slice value() const;
  virtual Status status() const;

 private:

  nvMultiTable* mt_;
  IndexIterator *index_iter_;
  //nvSkiplist::Iterator* iter_;
  Iterator* iter_;
  int pos_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  nvMultiTableIterator(const nvMultiTableIterator&) = delete;
  void operator=(const nvMultiTableIterator&) = delete;

};

}

#endif
