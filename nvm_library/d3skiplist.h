#ifndef D3SKIPLIST_H
#define D3SKIPLIST_H

#include "nvm_manager.h"
#include "mem_node_cache.h"
#include "d1skiplist.h"
#include "nvmemtable.h"

namespace leveldb {

struct D3MemTableIterator;

struct D3MemTableAllocator {
public:
    NVM_Manager* mng_;
    leveldb::LargePuzzleCache cache_;
    struct Block {
        nvAddr addr_;
        ull size_;
        Block(nvAddr addr, ull size) : addr_(addr), size_(size) {}
    };
    std::vector<Block> block_;
    size_t current_block_;

    ull total_size_, rest_size_;
    nvAddr alloc_ptr_;
    ull alloc_remaining_;

    enum {TotalSize = 0, NodeBound = 8, ValueBound = 16, HeadAddress = 24, MemTableInfoSize = 32 };

    D3MemTableAllocator(NVM_Manager* mng, ull size, ull buffer_size) :
        mng_(mng), cache_(mng, buffer_size),
        block_(), current_block_(0),
        total_size_(size), rest_size_(size - MemTableInfoSize),
        alloc_ptr_(0), alloc_remaining_(0)
    {
        while (size != 0) {
            nvAddr l = (size > MaxBlockSize ? MaxBlockSize : size);
            nvAddr addr = mng_->Allocate(l);
            block_.push_back(Block(addr, l));
            size -= l;
        }
        alloc_ptr_ = block_[current_block_].addr_;
        alloc_remaining_ = block_[current_block_].size_;
    }
    ~D3MemTableAllocator() {
        for (size_t i = 0; i < block_.size(); ++i) {
            mng_->Dispose(block_[i].addr_, block_[i].size_);
        }
    }
    nvAddr Allocate(nvAddr size) {
        nvAddr ans = cache_.Allocate(size);
        if (ans != nvnullptr) return ans;
        if (size > rest_size_) return nvnullptr;
        if (size > alloc_remaining_)
            return AllocateNewBlock(size);
        ans = alloc_ptr_;
        alloc_remaining_ -= size;
        rest_size_ -= size;
        alloc_ptr_ += size;
        return ans;
    }
    nvAddr AllocateNewBlock(nvAddr size) {
        if (current_block_ + 1 >= block_.size())
            return nvnullptr;
        rest_size_ -= alloc_remaining_;
        alloc_remaining_ = 0;
        alloc_ptr_ = nvnullptr;
        current_block_++;
        alloc_ptr_ = block_[current_block_].addr_;
        alloc_remaining_ = block_[current_block_].size_;
        return Allocate(size);
    }

    void Dispose(nvAddr addr, nvAddr size) {
        cache_.Dispose(addr, size);
    }

    nvAddr Garbage() const {
        return cache_.Garbage();
    }
    nvAddr Size() const { return total_size_; }
    nvAddr StorageUsage() const { return total_size_ - rest_size_; }
    nvAddr RestSpace() const { return rest_size_; }

    D3MemTableAllocator(const D3MemTableAllocator&) = delete;
    void operator=(const D3MemTableAllocator&) = delete;
};

struct D3MemTable {
private:
    NVM_Manager* mng_;
    D3MemTableAllocator arena_;
    nvAddr head_;
    byte max_height_;
    enum { HeightOffset = 0, KeySize = 1, ValueOffset = 9, NextOffset = 17 };
    enum { kMaxHeight = 12 };
    static const ull BlankMark = 1ULL << 63;
    //L2SkipList* msl_;

    string lft_bound_, rgt_bound_;
    ull seq_;
    int refs__;

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
    nvAddr Head() const { return head_; }
    nvAddr GetNext(nvAddr x, byte level) const {
        assert(x != nvnullptr);
       return mng_->read_ull(x + NextOffset + level * sizeof(nvAddr));
    }
    void SetNext(nvAddr x, byte level, nvAddr next) {
        assert(x != nvnullptr);
        mng_->write_ull(x + NextOffset + level * sizeof(nvAddr), next);
    }
    void SetValuePtr(nvAddr x, nvAddr newvalue) {
        mng_->write_ull(x + ValueOffset, newvalue);
    }
    byte GetHeight(nvAddr x) const {
        mng_->read_byte(x + HeightOffset);
    }
    ull GetKeySize(nvAddr x) const {
        return mng_->read_ull(x + KeySize);
    }
    nvAddr GetKeyPtr(nvAddr x) const {
        return x + NextOffset + GetHeight(x) * sizeof(nvAddr);
    }
    Slice GetKey(nvAddr x) const {
       return mng_->GetSlice(GetKeyPtr(x), GetKeySize(x));
    }
    void GetKey(nvAddr x, std::string* key) const {
        Slice k = GetKey(x);
        key->assign(k.data(), k.size());
    }
    nvAddr GetValuePtr(nvAddr x) {
        return mng_->read_addr(x + ValueOffset);
    }
    SequenceNumber PtrGetSequenceNumber(nvAddr ptr) {
        return mng_->read_addr(ptr + sizeof (nvAddr));
    }
    SequenceNumber Check(nvAddr x) {
        return (x & BlankMark ? x ^ BlankMark : nvnullptr);
    }
    Slice GetValue(nvAddr x, SequenceNumber *seq, nvAddr ptr = nvnullptr) {
        if (ptr == nvnullptr)
            ptr = GetValuePtr(x);
        *seq = Check(x);
        if (*seq == nvnullptr) {
            ull s[2];
            mng_->read(reinterpret_cast<byte*>(s), ptr, 2 * sizeof (nvAddr));
            *seq = s[1];
            return mng_->GetSlice(ptr + 2 * sizeof (nvAddr), s[0]);
        }
        return Slice();
    }
    nvAddr NewValue(const Slice& value, SequenceNumber seq) {
       if (value.size() == 0) return nvnullptr;
       nvAddr y = arena_.Allocate(value.size() + sizeof(nvAddr) + sizeof (nvAddr));
       assert(y != nvnullptr);
       const ull l = value.size() + sizeof (nvAddr) + sizeof (nvAddr);
       char v[l];
       ull * par = reinterpret_cast<ull*>(v);
       par[0] = value.size();
       par[1] = seq;
       memcpy(v + 2 * sizeof (nvAddr), value.data(), value.size());
       mng_->write(y, reinterpret_cast<const byte*>(v), l);
       return y;
    }
    nvAddr NewNode(const Slice& key, const Slice& value, ValueType type, SequenceNumber seq, byte height, nvAddr *next) {
       nvAddr total_size = 1 + sizeof(ull) + sizeof(nvAddr) + sizeof(nvAddr) * height + key.size();
       byte buf[total_size];
       nvAddr v = type == kTypeDeletion ? (BlankMark | seq) : NewValue(value, seq);
       nvAddr x = arena_.Allocate(total_size);
       assert( x != nvnullptr );

       byte* p = buf;
       *p = height;
       p ++;
       *reinterpret_cast<ull*>(p) = key.size();
       p += sizeof(ull);
       *reinterpret_cast<nvAddr*>(p) = v;
       p += sizeof(nvAddr);
       if (next)
           for (byte i = 0; i < height; ++i) {
               *reinterpret_cast<nvAddr*>(p) = next[i];
               p += sizeof(nvAddr);
           }
       else {
           memset(p, nvnullptr, height * sizeof(nvAddr));
           p += sizeof(nvAddr) * height;
       }
       if (key.size() > 0) memcpy(p, key.data(), key.size());
       mng_->write(x, buf, total_size);
       return x;
   }
public:
   D3MemTable(NVM_Manager* mng, ull size, ull garbage_cache_size)
       : mng_(mng), arena_(mng, size, garbage_cache_size),
         head_(nvnullptr),
         max_height_(1), refs__(0), rnd_(0xdeadbeef)
   {
       head_ = NewNode("", "", kTypeDeletion, 0, kMaxHeight, nullptr);
       for (byte i = 0; i < kMaxHeight; ++i)
           SetNext(head_, i, nvnullptr);
       SetHead(head_);
   }

   virtual ~D3MemTable() {}

   void SetHead(nvAddr head) {
       head_ = head;
   }
 // An iterator is either positioned at a key/value pair, or
 // not valid.  This method returns true iff the iterator is valid.
    bool Seek(const Slice& target, nvAddr &x_, byte level, nvAddr *prev = nullptr, nvAddr *pnext = nullptr) {
        //byte level = max_height_ - 1;
        nvAddr next = nvnullptr;
        int cmp;

        while (true) {
            next = GetNext(x_, level);
            cmp = next == nvnullptr ? -1 : target.compare(GetKey(next));
            if (cmp > 0)
                x_ = next;      // Right.
            else {
                if (prev) prev[level] = x_;
                if (pnext) pnext[level] = next;
                if (level == 0) {
                    if (cmp == 0) {
                        x_ = next;
                        return true;    // Found.
                    }
                    x_ = nvnullptr;   // Not Found.
                    return false;
                } else {
                    // Switch to next list
                    level--;   // Down.
                }
            }
        }
        assert(false);
        return nvnullptr;
    }
    nvAddr Insert(const Slice& key, const Slice& value, ValueType type, SequenceNumber seq, byte height, nvAddr *prev, nvAddr* next) {
        if (height > max_height_)
            for (byte i = max_height_; i < height; ++i) {
                prev[i] = head_;
                next[i] = nvnullptr;
            }

        nvAddr x = NewNode(key, value, type, seq, height, next);
        if (height > max_height_) {
            for (byte i = max_height_; i < height; ++i)
                SetNext(head_, i, x);
            max_height_ = height;
        }
        for (byte i = 0; i < height; ++i)
            SetNext(prev[i], i, x);
        return x;
    }
    void Update(nvAddr x, const Slice& value, ValueType type, SequenceNumber seq) {
        nvAddr old_v = GetValuePtr(x);
        nvAddr new_v = type == kTypeDeletion ? (BlankMark | seq) : NewValue(value, seq);
        SetValuePtr(x, new_v);
        if (old_v == nvnullptr) return;
        nvAddr size = mng_->read_ull(old_v);
        arena_.Dispose(old_v, size + sizeof(nvAddr) + sizeof(nvAddr));
    }
    void Add_(const Slice& key, const Slice& value, ValueType type, SequenceNumber seq, byte height) {
        nvAddr x = head_;
        nvAddr prev[kMaxHeight], pnext[kMaxHeight];
        if (!Seek(key, x, max_height_-1, prev, pnext))
            Insert(key, value, type, seq, height, prev, pnext);
        else
            Update(x, value, type, seq);
    }
    virtual void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) {
        Add_(key, value, type, seq, RandomHeight());
    }
    bool Get_(const Slice& key, std::string* value, Status* s) {
        nvAddr x_ = head_;
        if (Seek(key, x_, max_height_-1, nullptr, nullptr)) {
            //nvAddr ptr = GetValuePtr(x_);
            SequenceNumber seq = 0;
            Slice v = GetValue(x_, &seq, nvnullptr);
            if (v.size() > 0) {
                value->assign(v.data(), v.size());
                return true;
            } else {
                *s = Status::NotFound(Slice());
                return true;
            }
        }
        return false;
    }
    virtual bool Get(const LookupKey& lkey, std::string* value, Status* s) {
        return Get_(lkey.user_key(), value, s);
    }
    virtual ull StorageUsage() const {
        return arena_.StorageUsage();
    }
    nvAddr Tail() {
        nvAddr x = head_;
        for (int level = max_height_ - 1; level >= 0; level --) {
            for (nvAddr next = GetNext(x, level); next != nvnullptr; next = GetNext(x, level))
                x = next;
        }
        return x;
    }
    nvAddr Middle() {
        int top = max_height_;
        std::vector<nvAddr> a;
        for (int level = top-1; level >= 0; --level){
            for (nvAddr x = GetNext(head_,level); x != nvnullptr; x = GetNext(x,level))
                a.push_back(x);
            if (level == 0 || a.size() >= 16) {
                assert(a.size() > 0);
                nvAddr result = a[a.size()/2];
                return result;
            } else
                a.clear();
        }
        assert(false);
    }
    virtual bool HasRoomFor(nvAddr size) const { return size < arena_.RestSpace(); }

    virtual void CheckValid() {
        Slice prev, key;
        for (byte level = 0; level < max_height_; ++level) {
            prev = Slice();
            for (nvAddr x = GetNext(head_,level); x != nvnullptr; x = GetNext(x, level)) {
                key = GetKey(x);
                assert(key.compare(prev) > 0);
            }
        }
    }
    friend struct D3MemTableIterator;
    void operator=(const D3MemTable&) = delete;


    virtual ull BlankStorageUsage() const { assert(false); return 0; }
    virtual ull UpperStorage() const { assert(false); return 0; }
    virtual ull LowerStorage() const { assert(false); return 0; }

    virtual bool HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full) { assert(false); return false; }

    virtual void Ref() { refs__++; }
    virtual void Unref() { assert(refs__ > 0); if (--refs__ == 0) {delete this;} }

    virtual void Lock() { assert(false); }
    virtual void Unlock() { assert(false); }
    virtual void SharedLock() { assert(false); }

    virtual std::string Max() { assert(false); return ""; }
    virtual std::string Min() { assert(false); return ""; }

    virtual Iterator* NewIterator();
    virtual Iterator* NewOfficialIterator() { assert(false); return nullptr; }

    virtual void Rebuild(size_t count, std::vector<nvMemTable*> *package,
                 const string& dbname, Env* env, ull *log_number) { assert(false); }
    virtual void Connect(nvMemTable* b, bool reverse) { assert(false); }
    virtual void GarbageCollection() { assert(false); }
    virtual double Garbage() const { return 0; }

    virtual std::string& LeftBound() { assert(false); return lft_bound_; }
    virtual std::string& RightBound() { assert(false); return rgt_bound_; }
    virtual SequenceNumber Seq() const { assert(false); return seq_; }

    virtual void Print() { assert(false); }
    virtual MemTable* Immutable(ull seq) { assert(false); return nullptr; }

};

struct D3MemTableIterator : public Iterator {
 public:
    D3MemTableIterator(D3MemTable* mem, ull seq = 0) :
        mem_(mem), buffer_(new DRAM_Buffer(8192)), seq_(seq), x_(nvnullptr) {}
    virtual ~D3MemTableIterator() { delete buffer_; }

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const {
    return x_ != nvnullptr;
  }

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() {
      x_ = mem_->GetNext(mem_->head_, 0);
  }

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() {
      x_ = mem_->Tail();
  }

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) {
      x_ = mem_->head_;
      mem_->Seek(target, x_, mem_->max_height_-1, nullptr, nullptr);
   }
  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() {
      x_ = mem_->GetNext(x_, 0);
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
  Slice user_key() const {
      return mem_->GetKey(x_);
  }

  virtual Slice key() const {
      assert(x_ != nvnullptr);
      Slice key = user_key();
      nvAddr v = mem_->GetValuePtr(x_);
      SequenceNumber s = (v & D3MemTable::BlankMark ? v ^ D3MemTable::BlankMark : mem_->PtrGetSequenceNumber(v));

      char* lkey_data = reinterpret_cast<char*>(buffer_->Allocate(key.size() + 8));
      memcpy(lkey_data, key.data(), key.size());

      if (v == nulloffset) {
          EncodeFixed64(lkey_data + key.size(), (s << 8) | kTypeDeletion);
          //reinterpret_cast<ull*>(data + size) =
      } else {
          EncodeFixed64(lkey_data + key.size(), (s << 8) | kTypeValue);
      }
      return Slice(lkey_data, key.size() + 8);
  }

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice value() const {
      SequenceNumber seq;
      return mem_->GetValue(x_, &seq);
  }

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const {
      return Status();
  }

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.

 private:
  D3MemTable* mem_;
  DRAM_Buffer* buffer_;
  SequenceNumber seq_;
  nvAddr x_;

  D3MemTableIterator(const D3MemTableIterator&) = delete;
  void operator=(const D3MemTableIterator&) = delete;
};

}

#endif // D3SKIPLIST_H
