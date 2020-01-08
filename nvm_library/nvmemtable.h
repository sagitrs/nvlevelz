#ifndef NVMEMTABLE_H
#define NVMEMTABLE_H
#include "leveldb/options.h"
#include "nvm_manager.h"
#include "db/dbformat.h"
#include "db/memtable.h"
//#include "nvskiplist.h"
#include "nvm_library/l2skiplist.h"
#include "trie_compressed.h"
#include "db/log_writer.h"
#include "leveldb/env.h"
#include "db/filename.h"
namespace leveldb {

class L2MemTableIterator;
struct L2MemTableVersion;

struct AbstractMemTable {
    virtual ~AbstractMemTable();
    virtual void Add(SequenceNumber seq, ValueType type,
             const Slice& key,
             const Slice& value) = 0;
    virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;
    virtual ull StorageUsage() const = 0;
};
struct nvMemTable : AbstractMemTable {
    enum ParameterType {CreatedNumber, CreatedTime, PreWriteSize, WrittenSize};
    virtual ~nvMemTable();

    virtual void Add(SequenceNumber seq, ValueType type,
             const Slice& key,
             const Slice& value) = 0;
    virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;


    virtual nvOffset FindNode(const Slice& key);
    virtual void Update(nvOffset node, SequenceNumber seq, ValueType type, const Slice& value);

    virtual bool Immutable() const = 0;
    virtual void SetImmutable(bool state) = 0;
    virtual ull StorageUsage() const = 0;
    virtual ull DataWritten() const = 0;

    virtual ull BlankStorageUsage() const = 0;
    virtual ull UpperStorage() const = 0;
    virtual ull LowerStorage() const = 0;

    virtual bool HasRoomFor(ull size) const = 0;
    virtual bool HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full) = 0;
    virtual bool PreWrite(const Slice& key, const Slice& value, bool nearly_full) = 0;

    virtual void Ref() = 0;
    virtual void Unref() = 0;

    virtual void Lock() = 0;
    virtual void Unlock() = 0;
    virtual void SharedLock() = 0;
    virtual bool TryLock() = 0;
    virtual bool TrySharedLock() = 0;

    virtual std::string Max() = 0;
    virtual std::string Min() = 0;

    virtual Iterator* NewIterator() = 0;
    virtual Iterator* NewOfficialIterator(ull seq) = 0;

    virtual void FillKey(std::vector<std::string> &divider, size_t size) = 0;
    virtual nvMemTable* Rebuild(const Slice& lft, ull log_number) = 0;

    virtual void Connect(nvMemTable* b, bool reverse) = 0;
    virtual void GarbageCollection() = 0;
    virtual double Garbage() const = 0;

    virtual std::string& LeftBound() = 0;
    virtual std::string& RightBound() = 0;
    virtual ull& Paramenter(ParameterType type) = 0;
    virtual SequenceNumber Seq() const = 0;

    virtual void Print() = 0;
    virtual MemTable* Immutable(ull seq) = 0;

    virtual void CheckValid() = 0;
};
/*
struct L2MemTable : nvMemTable {
    class DefaultComparator : public Comparator {
     public:
      DefaultComparator() { }

      virtual const char* Name() const {
        return "=leveldb.BytewiseComparator";
      }

      virtual int Compare(const Slice& a, const Slice& b) const {
        return a.compare(b);
      }

      virtual void FindShortestSeparator(
          std::string* start,
          const Slice& limit) const {
        // Find length of common prefix
        size_t min_length = std::min(start->size(), limit.size());
        size_t diff_index = 0;
        while ((diff_index < min_length) &&
               ((*start)[diff_index] == limit[diff_index])) {
          diff_index++;
        }

        if (diff_index >= min_length) {
          // Do not shorten if one string is a prefix of the other
        } else {
          uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
          if (diff_byte < static_cast<uint8_t>(0xff) &&
              diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
            (*start)[diff_index]++;
            start->resize(diff_index + 1);
            assert(Compare(*start, limit) < 0);
          }
        }
      }

      virtual void FindShortSuccessor(std::string* key) const {
        // Find first character that can be incremented
        size_t n = key->size();
        for (size_t i = 0; i < n; i++) {
          const uint8_t byte = (*key)[i];
          if (byte != static_cast<uint8_t>(0xff)) {
            (*key)[i] = byte + 1;
            key->resize(i+1);
            return;
          }
        }
        // *key is a run of 0xffs.  Leave it alone.
      }
    };
    static const DefaultComparator cmp_;
    NVM_Manager* mng_;
    std::string list_name_;

    MixedSkipList* list_;
    MemTable *hdd_cache_;
    ListType type_;

    ull seq_;
    port::RWLock lock_;

    bool immutable_;
    virtual bool Immutable() const { return immutable_; }
    virtual void SetImmutable(bool state) { immutable_ = state;  }

    std::string lft_bound_, rgt_bound_, common_;
    const ull MaxListSize;

    int refs_;


    L2MemTable(L2SkipList *list, std::string bound, NVM_Manager* mng, const std::string& dbname, ull seq) :
        mng_(mng), list_name_(dbname + "/" + NumToString(seq, 16) + ".nvskiplist"),
        list_(reinterpret_cast<MixedSkipList*>(list)), type_(kTypeD2SkipList),
        hdd_cache_(nullptr),
        seq_(seq), lft_bound_(bound), rgt_bound_(), common_(),
        MaxListSize(list->cp_.nvskiplist_size_),
        refs_(0)
    {
        mng_->bind_name(list_name_, list_->Location());
        hdd_cache_ = NewMemTable();
        hdd_cache_->Ref();
    }

    L2MemTable() = delete;
    bool HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full = false) {
        return list_->HasRoomForWrite(key, value);
    }
    bool HasRoomFor(ull size) const {
        return list_->StorageUsage() + size < MaxListSize;
    }
    std::string& LeftBound() { return lft_bound_; }
    std::string& RightBound() { return rgt_bound_; }
    SequenceNumber Seq() const { return seq_; }
    // Increase reference count.
    void Ref();

    // Drop reference count.  Delete if no more references exist.
    void Unref();

    void Lock() { lock_.WriteLock(); }
    void Unlock() { lock_.Unlock(); }
    void SharedLock() {lock_.ReadLock(); }
    // Add an entry into memtable that maps key to value at the
    // specified sequence number and with the specified type.
    // Typically value will be empty if type==kTypeDeletion.
    void Add(SequenceNumber seq, ValueType type,
             const Slice& key,
             const Slice& value) {
        list_->Add(key, value, seq, type == kTypeDeletion);
    }

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey& key, std::string* value, Status* s) {
        return list_->Get(key.user_key(), value, s);
    }

    void HDDCacheReset() {
        hdd_cache_->Unref();
        hdd_cache_ = NewMemTable();
        hdd_cache_->Ref();
    }

    void Connect(nvMemTable* b, bool reverse);

    ull BlankStorageUsage() const {
        return 57;
    }
    ull StorageUsage() const {
        return list_->StorageUsage();
    }
    virtual ull UpperStorage() const {
        return list_->UpperStorage();
    }
    virtual ull LowerStorage() const {
        return list_->LowerStorage();
    }
    void Rebuild(size_t count, std::vector<nvMemTable*> *package,
                 const string& dbname, Env* env, ull *log_number) {
        assert(false);
    }
    void GarbageCollection() { list_->GarbageCollection(); }
    double Garbage() const { list_->Garbage(); }

    std::string Max() {
        switch (type_) {
        case kTypeD2SkipList:
            return static_cast<L2SkipList*>(list_)->Max();
        case kTypeBpSkipList:
            return "";
            //return static_cast<BpSkipList*>(list_)->Min();
        }
        assert(false);
    }

    std::string Min() {
        switch (type_) {
        case kTypeL2SkipList:
            return static_cast<L2SkipList*>(list_)->Min();
        case kTypeBpSkipList:
            return "";
            //return static_cast<L2SkipList*>(list_)->Min();
        }
        assert(false);
    }

    void Print() {
        list_->Print();
    }
    Iterator* NewIterator() {
        return list_->NewIterator();
    }
    Iterator* NewOfficialIterator(ull seq) {
        return list_->NewOfficialIterator(seq);
    }
    static MemTable* NewMemTable() {
        MemTable* result = new MemTable(InternalKeyComparator(&cmp_));
        result->Ref();
        return result;
    }
    MemTable* Immutable(ull seq) {
        Iterator* iter = list_->NewIterator();
        MemTable* imm = NewMemTable();
        //ll delta = 0;
        imm->AppendStart();
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()){
            Slice key = iter->key();
            Slice value = iter->value();
            ValueType type = value.size() == 0 ? kTypeDeletion : kTypeValue;//iter->isDeletion() ? kTypeDeletion : kTypeValue;
            imm->Append( seq, type, key, value);
            //delta += iter->size();
        }
        imm->AppendStop();
        delete iter;
        return imm;
    }

    operator const char*() {
        return "L2MemTable";
    }

    void CheckValid() {
        list_->CheckValid();
    }
private:
    ~L2MemTable() {
        mng_->delete_name(list_name_);
        delete list_;
        list_ = nullptr;
        hdd_cache_->Unref();
    }
};
*/
}

#endif // NVMEMTABLE_H
