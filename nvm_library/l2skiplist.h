#ifndef MixedSkipList_H
#define MixedSkipList_H

#include <cstdio>
#include <string>
#include <cstdlib>
#include <cmath>
#include <ctime>
#include <vector>
#include "leveldb/slice.h"
#include "slice_static.h"
#include "util/random.h"
#include "global.h"
#include "nvm_manager.h"
#include <map>
#include <queue>
#include "skiplist_kvindram.h"
#include "mem_bloom_filter.h"
#include "mem_node_cache.h"
#include "nvm_library/arena.h"
#include <deque>
#include "mixedskiplist.h"

using std::deque;
using std::string;
#define BLANK_NODE nvnullptr
#define INFO_COLLECT    1

namespace leveldb {
struct L2SkipListIterator;
struct L2SkipListOfficialIterator;
struct L2SkipListBoostOfficialIterator;
struct L2SkipList;

struct UpperSkiplist {
public:
    struct Node {
        explicit Node(const char* k, size_t height) : height_(height), key(k) { }
        size_t height_;
        const char* key;
        // Accessors/mutators for links.  Wrapped in methods so we can
        // add the appropriate barriers as necessary.
        Node* Next(int n) {
          assert(n >= 0);
          // Use an 'acquire load' so that we observe a fully initialized
          // version of the returned Node.
          return reinterpret_cast<Node*>(next_[n].Acquire_Load());
        }
        void SetNext(int n, Node* x) {
          assert(n >= 0);
          // Use a 'release store' so that anybody who reads through this
          // pointer observes a fully initialized version of the inserted node.
          next_[n].Release_Store(x);
        }

        // No-barrier variants that can be safely used in a few locations.
        Node* NoBarrier_Next(int n) {
          assert(n >= 0);
          return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
        }
        void NoBarrier_SetNext(int n, Node* x) {
          assert(n >= 0);
          next_[n].NoBarrier_Store(x);
        }
        Slice GetKey() {
            uint32_t key_size;
            const char* entry = GetVarint32Ptr(key, key + 5, &key_size);
            return Slice(entry, key_size);
        }
        void GetValue(uint32_t *nvm_offset, const char* entry = nullptr) {
            uint32_t key_size;
            if (entry == nullptr) {
                entry = GetVarint32Ptr(key, key + 5, &key_size);
                entry += key_size;
            }
            *nvm_offset = DecodeFixed32(entry);
        }

        void MoveValue(int32_t offset) {
            uint32_t key_size;
            const char* key_start = GetVarint32Ptr(key, key + 5, &key_size);
            char *entry = const_cast<char*>(key_start + key_size);
            nvOffset v;
            v = DecodeFixed32(entry);
            EncodeFixed32(entry, v + offset);
        }
        void SetValue(uint32_t offset) {
            uint32_t key_size;
            const char* key_start = GetVarint32Ptr(key, key + 5, &key_size);
            char *entry = const_cast<char*>(key_start + key_size);
            EncodeFixed32(entry, offset);
        }

        byte GetHeight() const { return static_cast<byte>(height_); }
       private:
        // Array of length equal to the node height.  next_[0] is lowest level link.
        port::AtomicPointer next_[1];
    };
    explicit UpperSkiplist(MixedSkipList* parent, Arena* arena, byte min_height, nvOffset lower_head);
    void Insert(const char* key, byte height, Node** prev) {
        Node* x = prev[0]->NoBarrier_Next(0);

        // Our data structure does not allow duplicate insertion
        //assert(x == NULL || !Equal(key, x->key));

        if (height > GetMaxHeight()) {
          for (int i = GetMaxHeight(); i < height; i++) {
            prev[i] = head_;
          }
          //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

          // It is ok to mutate max_height_ without any synchronization
          // with concurrent readers.  A concurrent reader that observes
          // the new value of max_height_ will see either the old value of
          // new level pointers from head_ (NULL), or a new value set in
          // the loop below.  In the former case the reader will
          // immediately drop to the next level since NULL sorts after all
          // keys.  In the latter case the reader will use the new node.
          max_height_ = height;
        }

        x = NewNode(key, height);
        for (int i = 0; i < height; i++) {
          // NoBarrier_SetNext() suffices since we will add a barrier when
          // we publish a pointer to "x" in prev[i].
          x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
          prev[i]->SetNext(i, x);
        }
    }
    void Add(const Slice& key, uint32_t nvm_offset, byte level, Node** prev) {
        size_t key_size = key.size();
        size_t val_size = sizeof(uint32_t);//VarintLength(nvm_offset);
        const size_t encoded_len = VarintLength(key_size) + key_size + val_size;
        char* buf = arena_->Allocate(encoded_len);
        char* p = EncodeVarint32(buf, key_size);
        memcpy(p, key.data(), key_size);
        p += key_size;
        EncodeFixed32(p, nvm_offset);
        p += sizeof(uint32_t);
        assert(p - buf == encoded_len);
        Insert(buf, level, prev);
    }
    bool Locate(const Slice& key, uint32_t *prev, uint32_t *pnext, Node* *dram_prev);
    void Print();
    bool SizeOf(const Slice& key, nvOffset offset) {
        static const size_t basic = sizeof(Node*) + sizeof(port::AtomicPointer) * kMaxHeight;
        return basic + VarintLength(key.size()) + key.size() + sizeof(nvOffset);

    }
    bool HasRoomFor(const Slice& key, nvOffset offset, size_t limit) {
        return SizeOf(key, offset) + arena_->MemoryUsage() < limit;
    }
    inline byte GetMaxHeight() const { return max_height_; }

    void GetTail(Node** tail) {
        Node* x = head_, *next = nullptr;
        for (int height = kMaxHeight; height >= GetMaxHeight(); height --)
            tail[height] = head_;
        for (int height = GetMaxHeight()-1; height >= 0; height --) {
            while ((next = x->Next(height)) != nullptr)
                x = next;
            tail[height] = x;
        }
    }
    Node* GetHead() const { return head_; }
    void SetHead(Node* head) {
        for (int i = 0; i < kMaxHeight; ++i)
            head_->SetNext(i, head->Next(i));
    }
private:
 enum { kMaxHeight = 12 };
 L2SkipList* parent_;
 Arena *arena_;
 Node* head_;
 nvOffset lower_head_;
 byte max_height_, min_height_;


 Node* NewNode(const char* key, int height) {
     char* mem = arena_->AllocateAligned(
         sizeof(Node) + sizeof(size_t) + sizeof(port::AtomicPointer) * (height - 1));
     return new (mem) Node(key, height);
   }

 // No copying allowed
 UpperSkiplist(const UpperSkiplist&) = delete;
 void operator=(const UpperSkiplist&) = delete;

 friend struct MixedSkipListIterator;
 friend struct L2SkipListOfficialIterator;
 friend struct L2SkipListBoostOfficialIterator;
 friend struct L2SkipList;
};

struct LowerSkiplist {
private:
    L2SkipList* parent_;
    Random rnd_;
    int RandomHeight() {
        // Increase height with probability 1 in kBranching
        static const unsigned int kBranching = 4;
        int height = 1;
        while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
          height++;
        }
        assert(height > 0);
        assert(height <= kMaxHeight);
        return height;
    }
    nvOffset NewNode(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, byte height, nvOffset *pnext);

    enum { HeightOffset = 0, KeySize = 1, ValueOffset = 5, NextOffset = 9 };
    enum { kMaxHeight = 12 };
    // [height = 1][key_size = 4][value_offset = 4][Next_0~h = 4x][key_0~size]
    // [value_size][value_0~size]

    void Update(nvOffset x, const Slice& value, bool isDeletion) {
        if (isDeletion)
            SetValue(x, Slice());
        else
            SetValue(x, value);
    }
    nvOffset GetKeySize(nvOffset x) {
        return mng_->read_ul(NVADDR(x + KeySize));
    }
    nvOffset GetNext(nvOffset x, byte level) {
        return mng_->read_ul(NVADDR(x + NextOffset + level * 4));
    }
    byte GetHeight(nvOffset x) {
        return mng_->read_byte(NVADDR(x + HeightOffset));
    }

    void GetKey(nvOffset x, char* *data, nvOffset *size);
    Slice GetKey(nvOffset x, DRAM_Buffer* buffer);
    Slice GetOfficialKey(nvOffset x, DRAM_Buffer* buffer, ull seq);
    void SetNext(nvOffset x, byte level, nvOffset n) {
        //assert(n == nulloffset || GetHeight(n) > level && level <= kMaxHeight);
        mng_->write_ul(NVADDR(x + NextOffset + level * 4), n);
    }
    nvOffset GetValuePtr(nvOffset x, nvOffset *value_size){
        nvOffset y = mng_->read_ul(NVADDR(x + ValueOffset));
        if (y == nulloffset) {
            *value_size = 0;
            return nulloffset;
        }
        *value_size = mng_->read_ul(NVADDR(y));
        return y;
    }
    nvOffset GetValuePtr(nvOffset x){
        nvOffset y = mng_->read_ul(NVADDR(x + ValueOffset));
        if (y == nulloffset)
            return nulloffset;
        return y;
    }
    void GetValue(nvOffset x, char* *data, nvOffset *size);
    Slice GetValue(nvOffset x, DRAM_Buffer* buffer);
    void SetValuePtr(nvOffset x, nvOffset v) {
        mng_->write_ul(NVADDR(x + ValueOffset), v);
    }
    void SetValue(nvOffset x, const Slice& value) {
        nvOffset value_size = 0;
        nvOffset oldValue = GetValuePtr(x, &value_size);
        nvOffset newValue = (value.size() == 0 ? nulloffset : NewValue(value) );
        SetValuePtr(x, newValue);
        narena_->Dispose(NVADDR(oldValue), value_size + 4);
    }
    nvOffset NewValue(const Slice& value);

public:
    LowerSkiplist(MixedSkipList* parent, NVM_Manager* mng, NVM_Linear_Allocator* narena, byte height);
    nvOffset Locate(const Slice& key, nvOffset *prev, nvOffset *pnext);
    nvOffset Insert(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, nvOffset *prev, nvOffset *pnext, byte* height) {
        *height = RandomHeight();
        nvOffset x = NewNode(key, value, seq, isDeletion, *height, pnext);
        if (*height > max_height_) {
            for (byte i = max_height_; i < *height; ++i)
                SetNext(head_, i, x);
            max_height_ = *height;
        }
        for (byte i = 0; i < *height; ++i)
            SetNext(prev[i], i, x);
        return x;
    }
    void Update(nvOffset x, const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
        nvOffset old_v = GetValuePtr(x);
        nvOffset new_v = isDeletion ? nulloffset : NewValue(value);
        SetValuePtr(x, new_v);
        if (old_v == nulloffset) return;
        nvOffset size = mng_->read_ul(NVADDR(old_v));
        narena_->Dispose(old_v, size + 4);
        garbage_size_ += size + 4;
    }
    bool Get(const Slice& key, std::string* value, Status* s, nvOffset x);
    std::string Min() {
        char* data;
        nvOffset size;
        nvOffset x = GetNext(head_, 0);
        if (x == nulloffset) return "";
        GetKey(GetNext(head_, 0), &data, &size);
        string result;
        result.assign(data, size);
        delete[] data;
        return result;
    }

    std::string Max() {
        nvOffset x = head_, next = head_;
        for (int i = max_height_ - 1; i >= 0; --i) {
            while ((next = GetNext(x, i)) != nulloffset)
              x = next;
        }

        char* data;
        nvOffset size;
        if (x == nulloffset) return "";
        GetKey(x, &data, &size);
        string result;
        result.assign(data, size);
        delete[] data;
        return result;
    }

    void GetMiddle(string *key) {
        int top = max_height_;
        std::vector<nvOffset> a;
        nvOffset size;
        char * data;
        for (int level = top-1; level >= 0; --level){
            for (nvOffset x = GetNext(head_,level); x != nulloffset; x = GetNext(x,level))
                a.push_back(x);
            if (level == 0 || a.size() >= 16) {
                assert(a.size() > 0);
                nvOffset result = a[a.size()/2];
                GetKey(result, &data, &size);
                key->assign(data,size);
                delete data;
                return;
            } else
                a.clear();
        }
        assert(false);
    }
    void Print() {
        printf("Print Lower (table) :\n");
        printf("Height = %d ~%d, Storage Usage = %llu\n",
               start_height_, max_height_,
               narena_->StorageUsage());
    }
    nvAddr Location() const { return main_; }
    nvOffset Head() const { return head_; }
    void SetHead(nvOffset head) {
        for (int i = 0; i < kMaxHeight; ++i)
            SetNext(head_, i, GetNext(head,i));
    }
    void GetTail(nvOffset* tail) {
        nvOffset x = head_, next = nulloffset;
        for (int height = kMaxHeight; height >= max_height_; height --)
            tail[height] = nulloffset;
        for (int height = max_height_-1; height >= 0; height --) {
            while ((next = GetNext(x, height)) != nulloffset)
                x = next;
            tail[height] = x;
        }
    }
private:
    //x = table_.Insert(key, value, seq, isDeletion, prev, &level);
    NVM_Manager* mng_;
    NVM_Linear_Allocator* narena_;
    const nvAddr main_;
    inline nvAddr NVADDR(nvOffset offset) const { return main_ + offset; }

    nvOffset head_;
    byte max_height_;
    byte start_height_;

    ull garbage_size_;

    friend struct L2SkipListIterator;
    friend struct L2SkipListOfficialIterator;
    friend struct L2SkipListBoostOfficialIterator;
    friend struct L2SkipList;

    deque<char*> buffer_;
    static const int MAX_BUFFER_SIZE = 20;
};

struct L2SkipList : public MixedSkipList  {
private:
 bool CacheSave(byte height) {
     if (height > save_height_)
         return 1;
     if (height < save_height_)
         return 0;
     if (save_rate_ == 0)
         return 0;
     return rnd_.Next() % save_rate_ == 0;
 }
 static void ConnectLower(LowerSkiplist* a, LowerSkiplist *b, bool reverse,  nvOffset offset);
 static void ConnectUpper(UpperSkiplist* a, UpperSkiplist *b, bool reverse,  nvOffset offset);

public:
 struct InfoColloctor {
     ull insert_;
     ull query_;
     ull load_cache_line_in_mem_;
     ull load_cache_line_in_nvm_;
     ull load_cache_line_in_nvm_by_insert_, load_cache_line_in_nvm_by_query_;
     ull write_cache_line_in_nvm_;
     InfoColloctor() :
         insert_(0), query_(0),
         load_cache_line_in_mem_(0),load_cache_line_in_nvm_(0),
         load_cache_line_in_nvm_by_insert_(0), load_cache_line_in_nvm_by_query_(0)
     {}
 } ic_;
 // Create a new MixedSkipList object that will use "cmp" for comparing keys,
 // and will allocate memory using "*arena".  Objects allocated in the arena
 // must remain allocated for the lifetime of the MixedSkipList object.
 explicit L2SkipList(NVM_Manager *mng, const CachePolicy& cp);
 L2SkipList(const L2SkipList& b);
 ~L2SkipList () {

 }

 void Add(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
     byte level = 0;
     nvOffset x = 0;
     nvOffset prev[kMaxHeight];
     nvOffset next[kMaxHeight];
     UpperSkiplist::Node* dram_prev[kMaxHeight];
     cache_.Locate(key, prev, next, dram_prev);
     nvOffset l = table_.Locate(key, prev, next);
     if (l == nulloffset) {
        x = table_.Insert(key, value, seq, isDeletion, prev, next, &level);
         if (CacheSave(level) && cache_.HasRoomFor(key, x, cp_.node_cache_size_))
             cache_.Add(key, x, level, dram_prev);
     } else
         table_.Update(l, key, value, seq, isDeletion);
 }
 void Insert(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
     Add(key, value, seq, isDeletion);
     if (INFO_COLLECT)   ic_.insert_++;
 }

 void Inserts(MemTable* mem);
 void Connect(MixedSkipList* b, bool reverse);

 bool Get(const Slice& key, std::string* value, Status* s) {
     nvOffset prev[kMaxHeight];
     cache_.Locate(key, prev, nullptr, nullptr);
     byte st = cache_.GetMaxHeight() - 1;
     if (save_height_ < st) st = save_height_;
     if (INFO_COLLECT) ic_.query_++;
     return table_.Get(key, value, s, prev[st]);
 }
 std::string Min() { return table_.Min(); }
 std::string Max() { return table_.Max(); }
 nvAddr Location() const {
     return table_.Location();
 }
 void GetMiddle(std::string& key) {
     table_.GetMiddle(&key);
 }

 Iterator* NewIterator();
 Iterator* NewOfficialIterator(ull seq);

 void Print();
 void CheckValid();
 ll StorageUsage() const { return narena_.StorageUsage(); }
 ull UpperStorage() const { return cp_.node_cache_size_; }
 ull LowerStorage() const { return cp_.nvskiplist_size_; }
 void Dispose() {
     delete this;
 }
 double Garbage() const {
     return static_cast<double>(table_.garbage_size_) / narena_.StorageUsage();
 }
 void GarbageCollection();

 void SetCommonSize(size_t common_size) {
     common_size_ = common_size;
 }

 bool HasRoomForWrite(const Slice& key, const Slice& value) {
     return cache_.HasRoomFor(key, 0, cp_.node_cache_size_) &&
             narena_.StorageUsage() + (1+4+4*LowerSkiplist::kMaxHeight + 4+key.size()) + (4+value.size()) < cp_.nvskiplist_size_ ;
 }
private:
 enum { kMaxHeight = 12 };

 NVM_Manager* mng_;
 NVM_Linear_Allocator narena_;
 Arena arena_;    // Arena used for allocations of nodes

 const byte save_height_;
 const size_t save_rate_;  // if save_rate_ == 0, no node will be saved, otherwise node will be saved at percentage 1 / save_rate.
public:
 CachePolicy cp_;
 Random rnd_;

 LowerSkiplist table_;
 UpperSkiplist cache_;
private:

 size_t common_size_;
 // No copying allowed

 friend struct L2SkipListIterator;
 friend struct L2SkipListOfficialIterator;
 friend struct L2SkipListBoostOfficialIterator;
 // No copying allowed
 void operator=(const MixedSkipList&) = delete;
};

struct L2SkipListIterator : public Iterator {
 public:
    L2SkipListIterator(L2SkipList* msl) :
        x_(nulloffset), msl_(msl), buffer_(new DRAM_Buffer(8192)) {}
    virtual ~L2SkipListIterator() { delete buffer_; }

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const {
    return x_ != nulloffset;
  }

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() {
      x_ = msl_->table_.GetNext(msl_->table_.head_, 0);
  }

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() {
      assert(false);
  }

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) {
      msl_->cache_.Locate(target, nvm_prev, nvm_pnext, dram_prev);
      msl_->table_.Locate(target, nvm_prev, nvm_pnext);
      x_ = msl_->table_.GetNext(nvm_prev[0], 0);
   }
  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() {
      x_ = msl_->table_.GetNext(x_, 0);
  }

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() {
      assert(false);
  }

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const {
      return msl_->table_.GetKey(x_, buffer_);
  }

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice value() const {
      /*nvOffset size, y;

      y = msl_->table_.GetValuePtr(x_, &size);
      if (y == 0) return Slice();
      char * data = new char[size];
      msl_->mng_->read(reinterpret_cast<byte*>(data), msl_->table_.NVADDR(y + 4), size);
      return Slice(data, size);
      */
      char* data;
      nvOffset size;
      return msl_->table_.GetValue(x_, buffer_);
  }

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const {
      return Status();
  }

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.

    nvOffset x_;
 private:
  L2SkipList* msl_;
  DRAM_Buffer* buffer_;
  nvOffset nvm_prev[L2SkipList::kMaxHeight];
  nvOffset nvm_pnext[L2SkipList::kMaxHeight];
  UpperSkiplist::Node* dram_prev[L2SkipList::kMaxHeight];

  L2SkipListIterator(const L2SkipListIterator&) = delete;
  void operator=(const L2SkipListIterator&) = delete;
};

struct L2SkipListOfficialIterator : public Iterator {
 public:
    L2SkipListOfficialIterator(L2SkipList* msl, ull seq) :
        x_(nulloffset), msl_(msl), buffer_(new DRAM_Buffer(8192)), seq_(seq) {}
    virtual ~L2SkipListOfficialIterator() { delete buffer_; }

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const {
    return x_ != nulloffset;
  }

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() {
      x_ = msl_->table_.GetNext(msl_->table_.head_, 0);
  }

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() {
      assert(false);
  }

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) {
      msl_->cache_.Locate(target, nvm_prev, nvm_pnext, dram_prev);
      msl_->table_.Locate(target, nvm_prev, nvm_pnext);
      x_ = msl_->table_.GetNext(nvm_prev[0], 0);
   }
  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() {
      x_ = msl_->table_.GetNext(x_, 0);
  }

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() {
      assert(false);
  }

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const {
      return msl_->table_.GetOfficialKey(x_, buffer_, seq_);
  }

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice value() const {
      /*nvOffset size, y;

      y = msl_->table_.GetValuePtr(x_, &size);
      if (y == 0) return Slice();
      char * data = new char[size];
      msl_->mng_->read(reinterpret_cast<byte*>(data), msl_->table_.NVADDR(y + 4), size);
      return Slice(data, size);
      */
      char* data;
      nvOffset size;
      return msl_->table_.GetValue(x_, buffer_);
  }

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const {
      return Status();
  }

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.

    nvOffset x_;
 private:
  L2SkipList* msl_;
  DRAM_Buffer* buffer_;
  ull seq_;
  nvOffset nvm_prev[L2SkipList::kMaxHeight];
  nvOffset nvm_pnext[L2SkipList::kMaxHeight];
  UpperSkiplist::Node* dram_prev[L2SkipList::kMaxHeight];

  L2SkipListOfficialIterator(const L2SkipListOfficialIterator&) = delete;
  void operator=(const L2SkipListOfficialIterator&) = delete;
};

struct L2SkipListBoostOfficialIterator : public Iterator {
 private:
    nvOffset GetNext(nvOffset x, byte level) const {
        assert(x != nulloffset);
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem_ + x + LowerSkiplist::NextOffset + level * 4));
    }
    nvOffset GetValuePtr(nvOffset x) const {
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem_ + x + LowerSkiplist::ValueOffset));
    }
    byte GetHeight(nvOffset x) const {
        return mem_[x + LowerSkiplist::HeightOffset];
    }

    nvOffset GetKeySize(nvOffset x) const {
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem_ + x + LowerSkiplist::KeySize));
    }
    nvOffset GetKeyPtr(nvOffset x) const {
        return x + LowerSkiplist::NextOffset + GetHeight(x) * 4;
    }
    Slice GetKey(nvOffset x) const {
        return Slice(reinterpret_cast<char*>(mem_ + GetKeyPtr(x)), GetKeySize(x));
    }
    Slice GetOfficialKey(nvOffset x) const {
        assert(x != nulloffset);
        Slice key = GetKey(x);
        nvOffset v = GetValuePtr(x);

        char* lkey_data = reinterpret_cast<char*>(buffer_->Allocate(key.size() + 8));
        memcpy(lkey_data, key.data(), key.size());

        if (v == nulloffset) {
            EncodeFixed64(lkey_data + key.size(), (seq_ << 8) | kTypeDeletion);
            //reinterpret_cast<ull*>(data + size) =
        } else {
            EncodeFixed64(lkey_data + key.size(), (seq_ << 8) | kTypeValue);
        }
        return Slice(lkey_data, key.size() + 8);
    }
    nvOffset ValueGetSize(nvOffset valueptr) const {
        return DecodeFixed32(reinterpret_cast<const char*>(mem_ + valueptr));
    }
    Slice GetValue(nvOffset x) const {
        assert(x != nulloffset);
        nvOffset valueptr = GetValuePtr(x);
        if (valueptr != nulloffset)
            return Slice();
        return Slice(reinterpret_cast<char*>(mem_ + valueptr), ValueGetSize(valueptr));
    }
 public:
    L2SkipListBoostOfficialIterator(L2SkipList* msl, ull seq) :
        x_(nulloffset),
        msl_(msl),
        mem_size_(msl->narena_.StorageUsage()),
        mem_(new byte[mem_size_]),
        buffer_(new DRAM_Buffer(8192)),
        seq_(seq)
    {
        msl->mng_->read(mem_, msl->narena_.Main(), mem_size_);
    }
    virtual ~L2SkipListBoostOfficialIterator() { delete[] mem_; delete buffer_; }

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const {
    return x_ != nulloffset;
  }

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() {
      nvOffset head = msl_->table_.head_;
      x_ = GetNext(head, 0);
  }

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() {
      assert(false);
  }

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) {
        byte level = msl_->table_.max_height_ - 1;
        nvOffset next = nulloffset;
        int cmp;

        while (true) {
            next = GetNext(x_, level);
                cmp = next == nulloffset ? -1 : target.compare(GetKey(next));
            if (cmp > 0)
                x_ = next;      // Right.
            else {
                if (cmp == 0)
                     return;    // Found.
                else if (level == 0) {
                     x_ = nulloffset;   // Not Found.
                     return;
                } else {
                     // Switch to next list
                     level--;   // Down.
                }
            }
        }
   }
  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() {
      x_ = GetNext(x_, 0);
  }

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() {
      assert(false);
  }

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const {
      return GetOfficialKey(x_);
  }

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice value() const {
      return GetValue(x_);
  }

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const {
      return Status();
  }

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.

    nvOffset x_;
 private:
  L2SkipList* msl_;
  ull mem_size_;
  byte* mem_;
  DRAM_Buffer* buffer_;
  ull seq_;

  L2SkipListBoostOfficialIterator(const L2SkipListBoostOfficialIterator&) = delete;
  void operator=(const L2SkipListBoostOfficialIterator&) = delete;
};

};

#endif // MixedSkipList_H
