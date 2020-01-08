#ifndef D4SKIPLIST_H
#define D4SKIPLIST_H
#include "d1skiplist.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include <string>
#include "port/port_posix.h"

namespace leveldb {

struct L4MemTableAllocator {
public:
    NVM_Manager* mng_;
    leveldb::PuzzleCache cache_;
    nvAddr main_;
    nvOffset total_size_, rest_size_;
    nvOffset node_bound_, value_bound_;
    static const ull BlockSize = (1<<20);
    enum {TotalSize = 0, NodeBound = 8, ValueBound = 16, HeadAddress = 24, MemTableInfoSize = 32 };
    nvOffset node_record_size_, value_record_size_;

    ~L4MemTableAllocator();
    L4MemTableAllocator(NVM_Manager* mng, ul size, ul buffer_size) :
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

    L4MemTableAllocator(const L4MemTableAllocator&) = delete;
    void operator=(const L4MemTableAllocator&) = delete;
};
    struct L4SkipList {
    private:
        NVM_Manager* mng_;
        L4MemTableAllocator arena_;
        nvOffset head_;
        byte max_height_;
        enum { HeightKeySize = 0, ReservedOffset = 4, ValueOffset = 8, NextOffset = 12 };
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
            return mng_->read_byte(mem() + x + HeightKeySize);
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
           nvOffset total_size = NextOffset + sizeof (nvOffset) * height + key.size();
           byte *buf = new byte[total_size];
           nvOffset v = type == kTypeDeletion ? nulloffset : NewValue(value);
           nvOffset x = arena_.AllocateNode(total_size);
           ul reserved = nulloffset;
           assert( x != nulloffset );

           byte* p = buf;
           *reinterpret_cast<uint32_t*>(p) = (static_cast<nvOffset>(key.size()) << 8 | height);
           p += 4;
           *reinterpret_cast<uint32_t*>(p) = reserved;
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
       ~L4SkipList();
       L4SkipList(NVM_Manager* mng, uint32_t size, uint32_t garbage_cache_size);
       L4SkipList(const L4SkipList& t);

       void SetHead(nvOffset head) {
           head_ = head;
           mng_->write_ull(mem() + L4MemTableAllocator::HeadAddress, head_);
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
        nvOffset GetReserved_(nvOffset x) const {
            return mng_->read_ul(mem() + x + ReservedOffset);
        }
        void SetReserved_(nvOffset x, ul r) const {
            mng_->write_ul(mem() + x + ReservedOffset, r);
        }
        Slice GetKey_(nvOffset x) {
            return GetKey(x);
        }
        nvOffset Add(const Slice& key, const Slice& value, ValueType type, byte height) {
            nvOffset x = head_;
            nvOffset prev[kMaxHeight], pnext[kMaxHeight];
            if (!Seek(key, x, max_height_-1, prev, pnext))
                return Insert(key, value, type, height, prev, pnext);
            Update(x, value, type);
            return nulloffset;
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
        bool Get_(nvOffset x, const Slice& key, std::string *value, Status *s) {
            //if (x == nulloffset) return false;
            int cmp;
            while ((cmp = GetKey(x).compare(key)) != 0) {
                x = GetReserved_(x);
                if (x == nulloffset || cmp > 0) return false;
            }
            Slice v = GetValue(x);
            if (v.size() > 0) {
                value->assign(v.data(), v.size());
                return true;
            } else {
                *s = Status::NotFound(Slice());
                return true;
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
        friend struct D4MemTable;
        friend struct D4MemTableIterator;
        friend struct D5MemTable;
        friend struct D5MemTableIterator;
        void operator=(const L4SkipList&) = delete;
    };
    struct L4Cache {
        typedef nvOffset ElementType;
        static const ul ElementSize = sizeof(ElementType);
        static const ul Reserved = 256;
        enum MetaOffset {
            LengthOffset = 0, ElementSizeOffset = 4, DataOffset = 16
        };
        NVM_Manager* arena_;
        const ul array_size_;
        const ul size_;
        const nvAddr main_;

        nvOffset LocalAddress(ul x) { return Reserved + x * ElementSize; }
        void MetaWrite4(nvOffset x, ul y) { arena_->write_ul(main_ + x, y); }
        ul MetaRead4(nvOffset x) { return arena_->read_ul(main_ + x); }
        void Init() {
            MetaWrite4(LengthOffset, array_size_);
            MetaWrite4(ElementSizeOffset, ElementSize);
            Clear();
        }
    public:
        ~L4Cache();
        L4Cache(NVM_Manager* arena, ul size) :
            arena_(arena),
            array_size_(size), size_(size * 4 + Reserved),
            main_(arena_->Allocate(size_)) {
            Init();
        }
        nvOffset Read(ul x) { return arena_->read_ul(main_ + LocalAddress(x)); }
        void Write(ul x, ul y) { arena_->write_ul(main_ + LocalAddress(x), y); }
        nvOffset Size() const { return array_size_; }
        void Clear() { arena_->write_zero(main_ + Reserved, size_ - Reserved); }
    };
    struct D4MemTable : nvMemTable {
    private:
        // D4MemTable: [TotalSize = 8][NodeBound = 8][ValueBound = 8][HeadAddress = 8]
        // [111111111111111111......................222222222222222222222]
        // ^                  ^                     ^                    ^
        // |                  |                     |                    |
        //MainAddress       M + NodeBound         M + ValueBound       M + TotalSize
        NVM_Manager* mng_;
        CachePolicy cp_;

        L4Cache cache_;
        L4SkipList table_;

        Random rnd_;
        int RandomHeight();

        std::string dbname_;
        ull seq_;
        ull pre_write_;
        ull written_size_;
        ull created_time_;

        int refs__;

        port::RWLock lock_;
        bool immutable_;

        std::string lft_bound_;
        std::string rgt_bound_;

        std::string MemName(const Slice& dbname, ull seq);
        std::string HashName(const Slice& dbname, ull seq);
        nvOffset Hash(const Slice& key) { return leveldb::Hash(key.data(), key.size(), 0xdeadbeef) % cp_.hash_range_ + 1; }
        ull MaxSizeOf(size_t keysize, size_t valuesize) {
            return keysize + valuesize + (1 + 4 + 4 + 4 * kMaxHeight + 4);
        }
        void DeleteName();
        nvOffset Seek(const Slice& key);
        bool Get(const Slice& key, std::string* value, Status* s);
        void GetMid(std::string* key);
        void GetMin(std::string* key);
        void GetMax(std::string* key);
    public:
        enum { kMaxHeight = 12 };
        virtual ~D4MemTable();
        D4MemTable(NVM_Manager* mng, const CachePolicy& cp, const Slice& dbname, ull seq);
        bool HashLocate(const Slice& key, nvOffset hash, nvOffset& node, nvOffset& next);
        void Insert(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value,
                                nvOffset hash, nvOffset prev, nvOffset next);
        void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);
        bool Get(const LookupKey& key, std::string* value, Status* s);
        nvOffset FindNode(const Slice& key);
        void Update(nvOffset node, SequenceNumber seq, ValueType type, const Slice& value);
        void GarbageCollection();
        Iterator* NewIterator();
        Iterator* NewOfficialIterator(ull seq);

        double Garbage() const;
        ull StorageUsage() const;
        virtual ull DataWritten() const {
            return written_size_;
        }
        ull UpperStorage() const { return table_.arena_.Size(); }
        ull LowerStorage() const { return table_.arena_.Size(); }

        nvAddr Location() const;

        bool HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full);
        virtual bool PreWrite(const Slice& key, const Slice& value, bool nearly_full);
        virtual bool HasRoomFor(ull size) const { return table_.HasRoomFor(size); }
        void Ref();
        void Unref();
        virtual bool Immutable() const { return immutable_; }
        virtual void SetImmutable(bool state) { immutable_ = state;  }

        void Print();
        void CheckValid();

        virtual ull BlankStorageUsage() const { return 88; }

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
            D4MemTable* mem = new D4MemTable(mng_, cp_, dbname_, log_number);
            mem->LeftBound() = lft.ToString();
            //mem->Ref();
            return mem;
        }

        virtual void Connect(nvMemTable* b, bool reverse) {
            D4MemTable* d = static_cast<D4MemTable*>(b);
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
        friend struct D4MemTableIterator;
        void operator=(const D4MemTable&) = delete;
    };

    struct D4MemTableIterator : public Iterator {
     public:
        D4MemTableIterator(D4MemTable* mem, ull seq = 0) :
            mem_(mem), buffer_(new DRAM_Buffer(8192)), seq_(seq), x_(nulloffset) {}
        virtual ~D4MemTableIterator() { delete buffer_; }

      virtual bool Valid() const { return x_ != nulloffset; }
      virtual void SeekToFirst() { x_ = mem_->table_.GetNext(mem_->table_.head_, 0); }
      virtual void SeekToLast() { x_ = mem_->table_.Tail(); }
      virtual void Seek(const Slice& target) { x_ = mem_->Seek(target); }
      virtual void Next() { x_ = mem_->table_.GetNext(x_, 0); }
      virtual void Prev() { x_ = mem_->table_.GetPrev(x_); }
      virtual Slice value() const { return mem_->table_.GetValue(x_); }
      virtual Status status() const { return Status(); }
      Slice user_key() const { return mem_->table_.GetKey_(x_); }
      virtual Slice key() const {
          assert(x_ != nulloffset);
          Slice key = user_key();
          nvOffset v = mem_->table_.GetValuePtr(x_);

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


      // Clients are allowed to register function/arg1/arg2 triples that
      // will be invoked when this iterator is destroyed.

     private:
      D4MemTable* mem_;
      DRAM_Buffer* buffer_;
      SequenceNumber seq_;
      nvOffset x_;

      D4MemTableIterator(const D4MemTableIterator&) = delete;
      void operator=(const D4MemTableIterator&) = delete;
    };

    struct D5MemTable : nvMemTable {
    private:
        NVM_Manager* mng_;
        CachePolicy cp_;

        //L4Cache cache_;
        L4SkipList table_;

        Random rnd_;
        int RandomHeight();

        std::string dbname_;
        ull seq_;
        ull pre_write_;
        ull written_size_;
        ull created_time_;

        int refs__;

        port::RWLock lock_;
        bool immutable_;

        std::string lft_bound_;
        std::string rgt_bound_;

        std::string MemName(const Slice& dbname, ull seq);
        std::string HashName(const Slice& dbname, ull seq);
        //nvOffset Hash(const Slice& key) { return leveldb::Hash(key.data(), key.size(), 0xdeadbeef) % cp_.hash_range_ + 1; }
        ull MaxSizeOf(size_t keysize, size_t valuesize) { return keysize + valuesize + (1 + 4 + 4 + 4 * kMaxHeight + 4); }
        void DeleteName();
        nvOffset Seek(const Slice& key);
        bool Get(const Slice& key, std::string* value, Status* s);
        void GetMid(std::string* key);
        void GetMin(std::string* key);
        void GetMax(std::string* key);
    public:
        enum { kMaxHeight = 12 };
        virtual ~D5MemTable();
        D5MemTable(NVM_Manager* mng, const CachePolicy& cp, const Slice& dbname, ull seq);
        void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);
        bool Get(const LookupKey& key, std::string* value, Status* s);
        void GarbageCollection();
        Iterator* NewIterator();
        Iterator* NewOfficialIterator(ull seq);

        double Garbage() const;
        ull StorageUsage() const;
        virtual ull DataWritten() const {
            return written_size_;
        }
        ull UpperStorage() const { return table_.arena_.Size(); }
        ull LowerStorage() const { return table_.arena_.Size(); }

        nvAddr Location() const;

        bool HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full);
        virtual bool PreWrite(const Slice& key, const Slice& value, bool nearly_full);
        virtual bool HasRoomFor(ull size) const { return table_.HasRoomFor(size); }
        void Ref();
        void Unref();
        virtual bool Immutable() const { return immutable_; }
        virtual void SetImmutable(bool state) { immutable_ = state;  }

        void Print();
        void CheckValid();

        virtual ull BlankStorageUsage() const { return 88; }

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
            D5MemTable* mem = new D5MemTable(mng_, cp_, dbname_, log_number);
            mem->LeftBound() = lft.ToString();
            //mem->Ref();
            return mem;
        }

        virtual void Connect(nvMemTable* b, bool reverse) {
            D5MemTable* d = static_cast<D5MemTable*>(b);
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
        friend struct D5MemTableIterator;
        void operator=(const D5MemTable&) = delete;
    };

    struct D5MemTableIterator : public Iterator {
     public:
        D5MemTableIterator(D5MemTable* mem, ull seq = 0) :
            mem_(mem), buffer_(new DRAM_Buffer(8192)), seq_(seq), x_(nulloffset) {}
        virtual ~D5MemTableIterator() { delete buffer_; }

      virtual bool Valid() const { return x_ != nulloffset; }
      virtual void SeekToFirst() { x_ = mem_->table_.GetNext(mem_->table_.head_, 0); }
      virtual void SeekToLast() { x_ = mem_->table_.Tail(); }
      virtual void Seek(const Slice& target) { x_ = mem_->Seek(target); }
      virtual void Next() { x_ = mem_->table_.GetNext(x_, 0); }
      virtual void Prev() { x_ = mem_->table_.GetPrev(x_); }
      virtual Slice value() const { return mem_->table_.GetValue(x_); }
      virtual Status status() const { return Status(); }
      Slice user_key() const { return mem_->table_.GetKey_(x_); }
      virtual Slice key() const {
          assert(x_ != nulloffset);
          Slice key = user_key();
          nvOffset v = mem_->table_.GetValuePtr(x_);

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


      // Clients are allowed to register function/arg1/arg2 triples that
      // will be invoked when this iterator is destroyed.

     private:
      D5MemTable* mem_;
      DRAM_Buffer* buffer_;
      SequenceNumber seq_;
      nvOffset x_;

      D5MemTableIterator(const D5MemTableIterator&) = delete;
      void operator=(const D5MemTableIterator&) = delete;
    };
}

#endif // D4SKIPLIST_H
