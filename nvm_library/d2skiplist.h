#ifndef D2SKIPLIST_H
#define D2SKIPLIST_H
#include "d1skiplist.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include <string>
#include "port/port_posix.h"

namespace leveldb {

struct D2MemTableIterator;
struct D2MemTable;

struct L2MemTableAllocator {
public:
    NVM_Manager* mng_;
    leveldb::PuzzleCache cache_;
    nvAddr main_;
    nvOffset total_size_, rest_size_;
    nvOffset node_bound_, value_bound_;
    static const ull BlockSize = (1<<20);
    enum {TotalSize = 0, NodeBound = 8, ValueBound = 16, HeadAddress = 24, MemTableInfoSize = 32 };
    nvOffset node_record_size_, value_record_size_;

    L2MemTableAllocator(NVM_Manager* mng, ul size, ul buffer_size) :
        mng_(mng), cache_(buffer_size),
        main_(mng->Allocate(size)),
        total_size_(size), rest_size_(size - MemTableInfoSize),
        node_bound_(MemTableInfoSize), value_bound_(total_size_), node_record_size_(BlockSize), value_record_size_(0)
    {
        mng->write_ull(main_ + NodeBound, node_record_size_);
        mng->write_ull(main_ + ValueBound, total_size_ - value_record_size_);
        mng->write_ull(main_ + NodeBound, total_size_);
        mng->write_ull(main_ + HeadAddress, nvnullptr);
        assert(size > BlockSize);
    }
    ~L2MemTableAllocator() {
        mng_->Dispose(main_, total_size_);
    }
    nvOffset AllocateNode(nvOffset size) {
        if (size > rest_size_) return nulloffset;
        nvOffset ans = node_bound_;
        node_bound_ += size;
        rest_size_ -= size;
        if (node_bound_ > node_record_size_) SetNodeRecord();
        return ans;
    }
    nvOffset AllocateValue(nvOffset size) {
        if (size > rest_size_) return nulloffset;
        nvOffset ans = cache_.Allocate(size);
        if (ans != nulloffset) return ans;
        value_bound_ -= size;
        rest_size_ -= size;
        if (value_bound_ + value_record_size_ > total_size_) SetValueRecord();
        return value_bound_;
    }

    void Reserve(nvOffset addr, nvOffset size) {
        cache_.Reserve(addr, size);
    }

    nvOffset Garbage() const {
        return cache_.lost_ + cache_.found_;
    }

    nvAddr Main() const { return main_; }
    nvOffset Size() const { return total_size_; }
    nvOffset StorageUsage() const { return total_size_ - rest_size_; }
    nvOffset RestSpace() const { return rest_size_; }

    void SetNodeRecord() {
        node_record_size_ = (node_bound_ / BlockSize + (node_bound_ % BlockSize > 0)) * BlockSize;
        mng_->write_ull(main_ + NodeBound, node_record_size_);
    }
    void SetValueRecord() {
        nvOffset value_size = total_size_ - node_bound_;
        value_record_size_ = (value_size / BlockSize + (value_size % BlockSize > 0)) * BlockSize;
        mng_->write_ull(main_ + ValueBound, total_size_ - value_record_size_);
    }
    void SetBound(nvOffset node, nvOffset value) {
        node_bound_ = node;
        value_bound_ = value;
        SetNodeRecord();
        SetValueRecord();
        rest_size_ = total_size_ - node_bound_ - (total_size_ - value_bound_);
        cache_.Clear();
    }

    L2MemTableAllocator(const L2MemTableAllocator&) = delete;
    void operator=(const L2MemTableAllocator&) = delete;
};

struct UpperD1Skiplist {
 private:
    MemTableAllocator arena_;
    nvOffset head_;
    byte max_height_;
    enum { HeightKeySize = 0, ValueOffset = 4, NextOffset = 8 };
    enum { kMaxHeight = 12 };
    //L2SkipList* msl_;
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
    nvOffset GetNext(nvOffset x, byte level) const {
        assert(x != nulloffset);
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem() + x + NextOffset + level * 4));
    }
    void SetNext(nvOffset x, byte level, nvOffset next) {
        assert(x != nulloffset);
        EncodeFixed32(reinterpret_cast<char*>(mem() + x + NextOffset + level * 4), next);
    }
    nvOffset GetValue(nvOffset x) const {
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem() + x + ValueOffset));
    }
    void SetValue(nvOffset x, nvOffset newvalue) {
        EncodeFixed32(
                    reinterpret_cast<char*>(
                        mem() + x + ValueOffset), newvalue);
    }
    byte GetHeight(nvOffset x) const {
        return mem()[x + HeightKeySize];
    }
    nvOffset GetKeySize(nvOffset x) const {
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem() + x + HeightKeySize)) >> 8;
    }
    nvOffset GetKeyPtr(nvOffset x) const {
        return x + NextOffset + GetHeight(x) * 4;
    }
    Slice GetKey(nvOffset x) const {
        return Slice(reinterpret_cast<char*>(mem() + GetKeyPtr(x)), GetKeySize(x));
    }
    nvOffset NewNode(const Slice& key, nvOffset value, byte height, nvOffset *prev) {
        nvOffset total_size = 4 + 4 + 4 * height + key.size();
        nvOffset x = arena_.AllocateNode(total_size);
        assert( x != nulloffset );

        byte *p = mem() + x;
        *reinterpret_cast<uint32_t*>(p) = (static_cast<nvOffset>(key.size()) << 8 | height);
        p += 4;
        *reinterpret_cast<uint32_t*>(p) = value;
        p += 4;
        if (prev)
            for (byte i = 0; i < height; ++i) {
                *reinterpret_cast<uint32_t*>(p) = GetNext(prev[i],i);
                p += 4;
            }
        else {
            memset(p, 0, height * 4);
            p += 4 * height;
        }
        memcpy(p, key.data(), key.size());
        return x;
    }
    nvOffset Insert(const Slice& key, nvOffset value, byte height, nvOffset *prev) {
        if (height > max_height_)
            for (byte i = max_height_; i < height; ++i)
                prev[i] = head_;

        nvOffset x = NewNode(key, value, height, prev);
        if (height > max_height_) {
            for (byte i = max_height_; i < height; ++i)
                SetNext(head_, i, x);
            max_height_ = height;
        }
        for (byte i = 0; i < height; ++i)
            SetNext(prev[i], i, x);
        return x;
    }
    void Update(nvOffset x, const Slice& key, nvOffset new_v) {
        SetValue(x, new_v);
    }

public:
    UpperD1Skiplist(uint32_t size) : arena_(size), head_(nulloffset), max_height_(1), rnd_(0xDEADBEEF) {
        head_ = NewNode("", 0, kMaxHeight, nullptr);
        for (byte i = 0; i < kMaxHeight; ++i)
            SetNext(head_, i, nulloffset);
    }
    virtual ~UpperD1Skiplist() {}

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  bool Seek(const Slice& target, nvOffset &x_, byte level, byte min_level, nvOffset *prev) {
        //byte level = max_height_ - 1;
        nvOffset next = nulloffset;
        int cmp;

        while (true) {
            next = GetNext(x_, level);
            if (next == nulloffset) {
                cmp = -1;
            } else {
                Slice key = GetKey(next);
                cmp = target.compare(key);
            }
            if (cmp > 0)
                x_ = next;      // Right.
            else {
                if (prev) prev[level] = x_;
                if (level <= min_level) {
                    if (prev)
                        for (byte h = 0; h < level; ++h)
                            prev[h] = x_;
                    if (cmp == 0) {
                        //x_ = next;
                        return true;    // Found.
                    }
                    //x_ = nulloffset;   // Not Found.
                    return false;
                } else {
                    // Switch to next list
                    level--;   // Down.
                }
            }
        }
        assert(false);
        return nulloffset;
   }
  void Add(const Slice& key, nvOffset value, byte height, nvOffset *prev) {
      //const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
      nvOffset x = head_;
      if (prev == nullptr) {
          nvOffset prev[kMaxHeight];
          bool exist = Seek(key, x, max_height_-1, 0, prev);
          assert(exist == false);
          Insert(key, value, height, prev);
      } else {
          Insert(key, value, height, prev);
      }
  }
  bool Get(const Slice& key, nvOffset* value) {
      nvOffset x_ = head_;
      Seek(key, x_, max_height_-1, 0, nullptr);
      if (x_ == head_)
          return false;         //please search from head.
      *value = GetValue(x_);
      return true;
  }
  ull StorageUsage() const {
      return arena_.StorageUsage();
  }
  bool HasRoomForWrite(const Slice& key, byte height) {
      return key.size() + height * 4 + 100 + arena_.StorageUsage() < arena_.Size();
  }
  byte* mem() const { return arena_.Main(); }
  nvOffset Head() const { return head_; }

  void CheckValid() {
      Slice prev, key;
      for (byte level = 0; level < max_height_; ++level) {
          prev = Slice();
          for (nvOffset x = GetNext(head_,level); x != nulloffset; x = GetNext(x, level)) {
              key = GetKey(x);
              assert(key.compare(prev) > 0);
          }
      }
  }
  friend struct D2MemTable;
  UpperD1Skiplist(const UpperD1Skiplist&) = delete;
  void operator=(const UpperD1Skiplist&) = delete;
};

struct LowerD1Skiplist {
private:
    NVM_Manager* mng_;
    L2MemTableAllocator arena_;
    nvOffset head_;
    byte max_height_;
    enum { HeightKeySize = 0, ValueOffset = 4, NextOffset = 8 };
    enum { kMaxHeight = 12 };
    enum { VersionNumber = 0, NextValueOffset = 8, ValueSize = 12, ValueDataOffset = 16 };
    //L2SkipList* msl_;
    nvAddr mem() const { return arena_.Main(); }
    nvOffset Head() const { return head_; }

    nvOffset GetNext(nvOffset x, byte level) const {
        return mng_->read_ul(mem() + x + NextOffset + level * 4);
        //return mng_->read_ul(mem() + x + NextOffset + level * 4);
    }
    void SetNext(nvOffset x, byte level, nvOffset next) {
        mng_->write_ul(mem() + x + NextOffset + level * 4, next);
    }
    nvOffset GetValuePtr(nvOffset x) const {
        return mng_->read_ul(mem() + x + ValueOffset);
        //return mng_->read_ul(mem() + x + ValueOffset);
    }
    void SetValuePtr(nvOffset x, nvOffset newvalue) {
        mng_->write_ul(mem() + x + ValueOffset, newvalue);
    }
    byte GetHeight(nvOffset x) const {
        return mng_->read_byte(mem() + x);
    }
    nvOffset GetKeySize(nvOffset x) const {
        return mng_->read_ul(mem() + x + HeightKeySize) >> 8;
    }
    nvOffset GetKeyPtr(nvOffset x) const {
        return x + NextOffset + GetHeight(x) * 4;
    }
    Slice GetKey(nvOffset x) const {
       return mng_->GetSlice(mem() + GetKeyPtr(x), GetKeySize(x));
    }
    void GetKey(nvOffset x, std::string* key) const {
        Slice k = GetKey(x);
        key->assign(k.data(), k.size());
    }

    nvOffset ValueGetSize(nvOffset valueptr) const {
        return mng_->read_ul(mem() + valueptr);
    }
    Slice GetValue(nvOffset x) const {
       //assert(x != nulloffset);
       nvOffset valueptr = GetValuePtr(x);
       if (valueptr == nulloffset)
           return Slice();
       nvOffset valuesize = ValueGetSize(valueptr);
       return  mng_->GetSlice(mem() + valueptr + 4, ValueGetSize(valueptr));
    }
    bool GetValue(nvOffset x, std::string* value) {
        Slice v = GetValue(x);
        if (v.size() == 0)
            return false;
        value->assign(v.data(), v.size());
        return true;
    }
    nvOffset NewValue(const Slice& value, SequenceNumber seq = 0, nvOffset next = nulloffset) {
       if (value.size() == 0) return nulloffset;
       nvOffset y = arena_.AllocateValue(value.size() + 4);
       assert(y != nulloffset);
       mng_->write_ul(mem() + y, value.size());
       mng_->write(mem() + y + 4, reinterpret_cast<const byte*>(value.data()), value.size());
       return y;
    }
    nvOffset NewNode(const Slice& key, const Slice& value, ValueType type, byte height, nvOffset *next) {
       nvOffset total_size = 4 + 4 + 4 * height + key.size();
       byte *buf = new byte[total_size];
       nvOffset v = type == kTypeDeletion ? nulloffset : NewValue(value);
       nvOffset x = arena_.AllocateNode(total_size);
       assert( x != nulloffset );

       byte* p = buf;
       *reinterpret_cast<uint32_t*>(p) = (static_cast<nvOffset>(key.size()) << 8 | height);
       p += 4;
       *reinterpret_cast<uint32_t*>(p) = v;
       p += 4;
       if (next)
           for (byte i = 0; i < height; ++i) {
               *reinterpret_cast<uint32_t*>(p) = next[i];
               p += 4;
           }
       else {
           memset(p, nulloffset, height * 4);
           p += 4 * height;
       }
       if (key.size() > 0) memcpy(p, key.data(), key.size());
       mng_->write(mem() + x, buf, total_size);
       delete[] buf;
       return x;
   }
public:
   LowerD1Skiplist(NVM_Manager* mng, uint32_t size, uint32_t garbage_cache_size)
       : mng_(mng), arena_(mng, size, garbage_cache_size),
         head_(nulloffset),
         max_height_(1)
   {
       head_ = NewNode("", "", kTypeDeletion, kMaxHeight, nullptr);
       for (byte i = 0; i < kMaxHeight; ++i)
           SetNext(head_, i, nulloffset);
       SetHead(head_);
   }
    LowerD1Skiplist(const LowerD1Skiplist& t)  // Garbage Collection.
        : mng_(t.mng_), arena_(mng_, t.arena_.Size(), t.arena_.cache_.Bytes()),
          head_(t.head_), max_height_(t.max_height_)
    {
        byte* buf = new byte[t.arena_.Size()];
        mng_->read(buf, t.arena_.Main(), t.arena_.Size());
        D1SkipList* d = new D1SkipList(buf, t.arena_.Size(), t.arena_.node_bound_, t.arena_.value_bound_, t.head_, t.max_height_);
        d->CheckValid();
        d->GarbageCollection();
        d->CheckValid();
        mng_->write(arena_.Main(), d->arena_.Main(), d->arena_.node_bound_);
        mng_->write(arena_.Main() + d->arena_.value_bound_,
                    d->arena_.Main() + d->arena_.value_bound_,
                    d->arena_.Size() - d->arena_.value_bound_);
        arena_.SetBound(d->arena_.node_bound_, d->arena_.value_bound_);
        CheckValid();
        delete d;
    }
   virtual ~LowerD1Skiplist() {}

   void SetHead(nvOffset head) {
       head_ = head;
       mng_->write_ull(mem() + L2MemTableAllocator::HeadAddress, head_);
   }
 // An iterator is either positioned at a key/value pair, or
 // not valid.  This method returns true iff the iterator is valid.
    bool Seek(const Slice& target, nvOffset &x_, byte level, nvOffset *prev = nullptr, nvOffset *pnext = nullptr) {
        //byte level = max_height_ - 1;
        static nvOffset __thread next = nulloffset;
        int cmp;

        while (true) {
            next = GetNext(x_, level);
            cmp = next == nulloffset ? -1 : target.compare(GetKey(next));
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
                    x_ = nulloffset;   // Not Found.
                    return false;
                } else {
                    // Switch to next list
                    level--;   // Down.
                }
            }
        }
        assert(false);
        return nulloffset;
    }
    nvOffset GetPrev(nvOffset x) {
        Slice target = GetKey(x);
        nvOffset next = nulloffset;
        byte level = max_height_-1;
        int cmp;
        x = head_;
        while (true) {
            next = GetNext(x, level);
            cmp = next == nulloffset ? -1 : target.compare(GetKey(next));
            if (cmp > 0)
                x = next;      // Right.
            else {
                if (level == 0) {
                    if (x == head_) return nulloffset;
                    return x;
                } else {
                    // Switch to next list
                    level--;   // Down.
                }
            }
        }
        assert(false);
        return nulloffset;
    }
    nvOffset Insert(const Slice& key, const Slice& value, ValueType type, byte height, nvOffset *prev, nvOffset* next) {
        if (height > max_height_)
            for (byte i = max_height_; i < height; ++i) {
                prev[i] = head_;
                next[i] = nulloffset;
            }

        nvOffset x = NewNode(key, value, type, height, next);
        if (height > max_height_) {
            for (byte i = max_height_; i < height; ++i)
                SetNext(head_, i, x);
            max_height_ = height;
        }
        for (byte i = 0; i < height; ++i)
            SetNext(prev[i], i, x);
        return x;
    }
    void Update(nvOffset x, const Slice& value, ValueType type) {
        nvOffset old_v = GetValuePtr(x);
        nvOffset new_v = type == kTypeDeletion ? nulloffset : NewValue(value);
        SetValuePtr(x, new_v);
        if (old_v == nulloffset) return;
#ifdef NO_READ_DELAY
        nvOffset size = *reinterpret_cast<nvOffset*>(arena_.main_ + old_v);
#else
        nvOffset size = mng_->read_ul(mem() + old_v);
#endif
        arena_.Reserve(old_v, size + 4);
    }
    void Add(const Slice& key, const Slice& value, ValueType type, byte height) {
        nvOffset x = head_;
        nvOffset prev[kMaxHeight], pnext[kMaxHeight];
        if (!Seek(key, x, max_height_-1, prev, pnext))
            Insert(key, value, type, height, prev, pnext);
        else
            Update(x, value, type);
    }
    bool Get(const LookupKey& lkey, std::string* value, Status* s) {
        Slice key = lkey.user_key();
        nvOffset x_ = head_;
        if (Seek(key, x_, max_height_-1, nullptr, nullptr)) {
            Slice v = GetValue(x_);
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
    ull StorageUsage() const {
        return arena_.StorageUsage();
    }
    nvOffset Tail() {
        nvOffset x = head_;
        for (int level = max_height_ - 1; level >= 0; level --) {
            for (nvOffset next = GetNext(x, level); next != nulloffset; next = GetNext(x, level))
                x = next;
        }
        return x;
    }
    nvOffset Middle() {
        int top = max_height_;
        std::vector<nvOffset> a;
        for (int level = top-1; level >= 0; --level){
            for (nvOffset x = GetNext(head_,level); x != nulloffset; x = GetNext(x,level))
                a.push_back(x);
            if (level == 0 || a.size() >= 16) {
                assert(a.size() > 0);
                nvOffset result = a[a.size()/2];
                return result;
            } else
                a.clear();
        }
        assert(false);
    }
    bool HasRoomFor(nvOffset size) const { return size < arena_.RestSpace(); }

    void CheckValid() {
        Slice prev, key;
        for (byte level = 0; level < max_height_; ++level) {
            prev = Slice();
            for (nvOffset x = GetNext(head_,level); x != nulloffset; x = GetNext(x, level)) {
                key = GetKey(x);
                assert(key.compare(prev) > 0);
            }
        }
    }
    friend struct D2MemTable;
    friend struct D2MemTableIterator;
    void operator=(const LowerD1Skiplist&) = delete;
};

struct D2MemTable : nvMemTable {
private:
    // D2MemTable: [TotalSize = 8][NodeBound = 8][ValueBound = 8][HeadAddress = 8]
    // [111111111111111111......................222222222222222222222]
    // ^                  ^                     ^                    ^
    // |                  |                     |                    |
    //MainAddress       M + NodeBound         M + ValueBound       M + TotalSize

    NVM_Manager* mng_;
    CachePolicy cp_;

    UpperD1Skiplist cache_;
    bool cache_enabled_;
    LowerD1Skiplist *table_;

    Random rnd_;
    int RandomHeight();

    byte st_height_;
    ul st_rate_;
    std::string dbname_;
    ull seq_;
    ull pre_write_;
    ull written_size_;
    ull created_time_;
    bool CacheSave(byte height);

    int refs__;

    port::RWLock lock_;
    bool immutable_;

    std::string lft_bound_;
    std::string rgt_bound_;

    std::string MemName(const Slice& dbname, ull seq);
    ull MaxSizeOf(size_t keysize, size_t valuesize) {
        return keysize + valuesize + (1 + 4 + 4 + 4 * kMaxHeight + 4);
    }
public:
    enum { kMaxHeight = 12 };

    D2MemTable(NVM_Manager* mng, const CachePolicy& cp, const Slice& dbname, ull seq);
    ~D2MemTable();
    void DeleteName();
    void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);
    nvOffset Seek(const Slice& key);
    bool Get(const Slice& key, std::string* value, Status* s);
    bool Get(const LookupKey& key, std::string* value, Status* s);
    void GarbageCollection();
    Iterator* NewIterator();
    Iterator* NewOfficialIterator(ull seq);

    void GetMid(std::string* key);
    void GetMin(std::string* key);
    void GetMax(std::string* key);

    double Garbage() const;
    ull StorageUsage() const;
    virtual ull DataWritten() const {
        return written_size_;
    }
    ull UpperStorage() const { return cache_.arena_.Size(); }
    ull LowerStorage() const { return table_->arena_.Size(); }

    nvAddr Location() const;

    bool HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full);
    virtual bool PreWrite(const Slice& key, const Slice& value, bool nearly_full);
    virtual bool HasRoomFor(ull size) const {
        return table_->HasRoomFor(size);
    }


    void Ref();
    void Unref();

    virtual bool Immutable() const { return immutable_; }
    virtual void SetImmutable(bool state) { immutable_ = state;  }

    void Print();
    void CheckValid();

    virtual ull BlankStorageUsage() const {
        return 89;
    }

    virtual std::string Max() {
        std::string s;
        GetMax(&s);
        return s;
    }
    virtual std::string Min() {
        std::string s;
        GetMin(&s);
        return s;
    }
    virtual void Lock() { lock_.WriteLock(); }
    virtual void Unlock() { lock_.Unlock(); }
    virtual void SharedLock() { lock_.ReadLock(); }
    virtual bool TryLock() {
        return lock_.TryWriteLock();
    }
    virtual bool TrySharedLock() {
        return lock_.TryReadLock();
    }
    std::string& LeftBound() { return lft_bound_; }
    std::string& RightBound() { return rgt_bound_; }
    SequenceNumber Seq() const { return seq_; }

    size_t CountOfLevel(byte level);
    virtual void FillKey(std::vector<std::string> &divider, size_t size);

    virtual nvMemTable* Rebuild(const Slice& lft, ull log_number) {
        D2MemTable* mem = new D2MemTable(mng_, cp_, dbname_, log_number);
        mem->LeftBound() = lft.ToString();
        //mem->Ref();
        return mem;
    }

    virtual void Connect(nvMemTable* b, bool reverse) {
        D2MemTable* d = static_cast<D2MemTable*>(b);
        assert(false);
    }

    virtual MemTable* Immutable(ull seq) { assert(false); }
    virtual ull& Paramenter(ParameterType type) {
        switch (type) {
        case ParameterType::CreatedNumber:
            return seq_;
        case ParameterType::CreatedTime:
            return created_time_;
        case ParameterType::PreWriteSize:
            return pre_write_;
        case ParameterType::WrittenSize:
            return written_size_;
        default:
            assert(false);
        }
    }

    // No copying allowed
    friend struct D2MemTableIterator;
    void operator=(const D2MemTable&) = delete;
};

struct D2MemTableIterator : public Iterator {
 public:
    D2MemTableIterator(D2MemTable* mem, ull seq = 0) :
        mem_(mem), buffer_(new DRAM_Buffer(8192)), seq_(seq), x_(nulloffset) {}
    virtual ~D2MemTableIterator() { delete buffer_; }

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const {
    return x_ != nulloffset;
  }

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() {
      x_ = mem_->table_->GetNext(mem_->table_->head_, 0);
  }

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() {
      x_ = mem_->table_->Tail();
  }

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) {
      x_ = mem_->Seek(target);
   }
  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() {
      x_ = mem_->table_->GetNext(x_, 0);
  }

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() {
      x_ = mem_->table_->GetPrev(x_);
  }

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  Slice user_key() const {
      return mem_->table_->GetKey(x_);
  }

  virtual Slice key() const {
      assert(x_ != nulloffset);
      Slice key = user_key();
      nvOffset v = mem_->table_->GetValuePtr(x_);

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

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice value() const {
      return mem_->table_->GetValue(x_);
  }

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const {
      return Status();
  }

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.

 private:
  D2MemTable* mem_;
  DRAM_Buffer* buffer_;
  SequenceNumber seq_;
  nvOffset x_;

  D2MemTableIterator(const D2MemTableIterator&) = delete;
  void operator=(const D2MemTableIterator&) = delete;
};

}


#endif // D2SKIPLIST_H
