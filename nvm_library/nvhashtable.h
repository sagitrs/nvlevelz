#ifndef NVHASHTABLE_H
#define NVHASHTABLE_H

#include "db/dbformat.h"
#include "leveldb/slice.h"
#include "global.h"
#include "db/memtable.h"
#include "nvm_manager.h"
#include "murmurhash.h"
#include "nvmemtable.h"
#include "d1skiplist.h"
#include "mem_node_cache.h"
#include "util/random.h"

namespace leveldb {
/*
struct AbstractMemTable {
    virtual ~AbstractMemTable();
    virtual void Add(SequenceNumber seq, ValueType type,
             const Slice& key,
             const Slice& value) = 0;
    virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;
    virtual ull StorageUsage() const = 0;
};
struct nvMemTable : AbstractMemTable {
    virtual ~nvMemTable();

    virtual void Add(SequenceNumber seq, ValueType type,
             const Slice& key,
             const Slice& value) = 0;
    virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;
    virtual bool Immutable() const = 0;
    virtual void SetImmutable(bool state) = 0;
    virtual ull StorageUsage() const = 0;

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
    virtual SequenceNumber Seq() const = 0;

    virtual void Print() = 0;
    virtual MemTable* Immutable(ull seq) = 0;

    virtual void CheckValid() = 0;
};*/

struct nvLinearHash : AbstractMemTable {
    struct Narena {
    public:
        NVM_Manager* mng_;
        PuzzleCache cache_;
        //ull full_size_;
        //std::vector<nvAddr> block_;
        nvAddr main_;
        nvOffset full_size_;
        nvOffset alloc_ptr_;
        nvOffset alloc_remaining_;
        //nvOffset garbage_size_;

        Narena(NVM_Manager* mng, ul size) :
            mng_(mng), cache_(256), main_(), full_size_(size),
            alloc_ptr_(blankblock), alloc_remaining_(0)
        {
            main_ = mng_->Allocate(size);
            alloc_ptr_ = 0;
            alloc_remaining_ = size - 0;
        }
        ~Narena() {
            DisposeAll();
        }
        nvOffset AllocateBlock(nvOffset size) {
            if (size > alloc_remaining_)
                return blankblock;
            nvOffset ans = alloc_ptr_;
            alloc_remaining_ -= size;
            alloc_ptr_ += size;
            return ans;
        }
        nvOffset AllocateValue(nvOffset size) {
            if (size > alloc_remaining_)
                return blankblock;
            nvOffset ans = cache_.Allocate(size);
            if (ans != nulloffset) return ans;
            ans = alloc_ptr_;
            alloc_remaining_ -= size;
            alloc_ptr_ += size;
            return ans;
        }
        void Dispose(nvOffset addr, nvOffset size) {
            cache_.Reserve(addr, size);
            //garbage_size_ += size;
        }
        void DisposeAll() {
            mng_->Dispose(main_, full_size_);
            cache_.Clear();
            alloc_ptr_ = blankblock;
            alloc_remaining_ = 0;
            //garbage_size_ = 0;
            // Remember : main_block_ is not released. it shall be released by nvHashTable.
        }
        Narena(const Narena&) = delete;
        void operator=(const Narena&) = delete;
    };
    Narena arena_;
    NVM_Manager* mng_;
    ul hash_range_, full_range_, total_used_, total_key_;
    nvOffset hash_block_offset_;
    nvOffset main_block_offset_;
    bool cleaned_;
    static const ul blankblock = 0;

    struct Block {
        ul key_size_;
        nvOffset value_ptr_;
        nvOffset next_;
        const char* key_data_;
        Block() : key_size_(0), value_ptr_(blankblock), next_(blankblock), key_data_(nullptr) {}
    };

    void SetBlock(nvOffset x, nvOffset block) {
        mng_->write_ul(arena_.main_ + x, block);
    }
    // Block : [key_size][value_ptr][key_data]
    // Value : [value_size][next][version][value_data]
    enum MainBlockOffset {TableSize = 0, HashHeadOffset = 4, HashSize = 8, TableUsed = 12, MainBlockSize = 16};
    enum BlockOffset {KeySizeOffset = 0, ValuePtrOffset = 4, NextBlockOffset = 8, KeyDataOffset = 12};
    enum ValueOffset {ValueSizeOffset = 0, ValueNextOffset = 4, VersionOffset = 8, ValueDataOffset = 16};

    void LoadBlock(nvOffset x, Block* block) {
        mng_->read(reinterpret_cast<byte*>(block), arena_.main_ + x + KeySizeOffset, 12);
        block->key_data_ = mng_->GetSlice(arena_.main_ + x + KeyDataOffset, block->key_size_).data();
    }
    ul GetValueSize(nvOffset vp) {
        return mng_->read_ul(arena_.main_ + vp + ValueSizeOffset);
    }
    void UpdateBlock(nvOffset x, nvOffset new_v) {
        mng_->write_ul(arena_.main_ + x + BlockOffset::ValuePtrOffset, new_v);
    }
    Slice GetValue(const Block& block) {
        return mng_->GetSlice(arena_.main_ + block.value_ptr_ + ValueDataOffset, GetValueSize(block.value_ptr_));
    }
    Slice GetKey(const Block& block) {
        return Slice(block.key_data_, block.key_size_);
    }
    nvOffset NewValue(const Slice& value) {
        if (value.size() == 0) return blankblock;
        nvOffset x = arena_.AllocateValue(ValueDataOffset + value.size());
        mng_->write_ul(arena_.main_ + x + ValueSizeOffset, value.size());
        mng_->write(arena_.main_ + x + ValueDataOffset, reinterpret_cast<const byte*>(value.data()), value.size());
        return x;
    }
    nvOffset NewBlock(const Slice& key, const Slice& value, nvOffset next) {
        size_t l = KeyDataOffset + key.size();
        nvOffset x = arena_.AllocateBlock(l);
        nvOffset v = NewValue(value);
        assert( x != blankblock );
        char *buf = new char[l];
        char* p = buf;
//        EncodeFixed32(p, check_hash); p += 4;
        EncodeFixed64(p, key.size()); p += 4;
        EncodeFixed32(p, v); p += 4;
        EncodeFixed32(p, next); p += 4;
        memcpy(p, key.data(), key.size()); p += key.size();
        assert(buf + l == p);
        mng_->write(arena_.main_ + x, reinterpret_cast<byte*>(buf), l);
        delete[] buf;
        return x;
    }

    nvOffset hash(const leveldb::Slice& key) {
        return MurmurHash3_x86_32(key.data(), key.size()) % hash_range_;
    }
    void CleanHashBlock() {
        mng_->write_zero(arena_.main_ + hash_block_offset_, hash_range_ * sizeof (nvOffset));
        cleaned_ = true;
    }
public:
    nvLinearHash(NVM_Manager* mng, ul table_size, ul hash_range, double full_limit, bool clean = true) :
        arena_(mng, table_size),
        mng_(mng),
        hash_range_(hash_range),
        full_range_(static_cast<ul>(hash_range_ * full_limit)),
        total_used_(0), total_key_(0),
        hash_block_offset_(blankblock),
        main_block_offset_(blankblock) {
        main_block_offset_ = arena_.AllocateBlock(MainBlockSize);
        hash_block_offset_ = arena_.AllocateBlock(hash_range_ * sizeof(nvOffset));
        if (clean) {
            CleanHashBlock();
        }
        cleaned_ = false;
    }
    ~nvLinearHash() {
        //arena_.DisposeAll();
        //    arena_.DisposeAll();
    //    mng_->write_ull(hash_main_ + ArenaBlockHead, nvnullptr);
    //    mng_->Dispose(arena_.main_block_, Narena::BlockSize);
    //    mng_->Dispose(hash_main_, MainBlockLength + hash_range_ * sizeof(nvAddr));
    }
    nvAddr Location() { return arena_.main_; }
    void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) {
        if (type == kTypeValue)
            Add_(key, value, seq);
        else
            Add_(key, Slice(), seq);
    }
    void Add_(const leveldb::Slice& key, const leveldb::Slice& value, SequenceNumber seq);
    bool Get(const LookupKey& lkey, std::string* value, Status *s);
    bool Get_(const leveldb::Slice& key, std::string* value, leveldb::Status *s);
    bool Seek(const leveldb::Slice& key, Block& block, nvOffset& block_ptr, nvOffset& block_ptr_location);
    ull StorageUsage() const { return arena_.full_size_ - arena_.alloc_remaining_; }
    Iterator* NewIterator() {
        return new HashIterator(this);
    }
    struct HashIterator : Iterator {
        nvLinearHash* table_;
        Block block_;
        nvOffset ptr_, ptr_location_;
        nvOffset x_, current_;
        const nvAddr hash_block_;
        Random rand_;
        HashIterator(nvLinearHash* table) :
              table_(table),
              block_(), x_(0), current_(blankblock),
              hash_block_(table->Location() + table->hash_block_offset_),
              rand_(0xdeadbeef) {}
        nvOffset GetPtr(nvOffset pos) {
            return table_->mng_->read_ul(hash_block_ + pos * sizeof (nvOffset));
        }
        // An iterator is either positioned at a key/value pair, or
        // not valid.  This method returns true iff the iterator is valid.
        virtual bool Valid() const {
            return current_ != blankblock;
        }

        // Position at the first key in the source.  The iterator is Valid()
        // after this call iff the source is not empty.
        virtual void SeekToFirst() {
            for (x_ = 0; x_ < table_->hash_range_; ++x_) {
                current_ = GetPtr(x_);
                if (current_ != nvLinearHash::blankblock) {
                    table_->LoadBlock(current_, &block_);
                    return;
                }
            }
            current_ = blankblock;
        }

        // Position at the last key in the source.  The iterator is
        // Valid() after this call iff the source is not empty.
        virtual void SeekToLast() {
            for (x_ = table_->hash_range_-1; x_ > 0; --x_) {
                current_ = GetPtr(x_);
                if (current_ != nvLinearHash::blankblock) {
                    table_->LoadBlock(current_, &block_);
                    return;
                }
            }
            current_ = GetPtr(0);
            if (current_ != nvLinearHash::blankblock) {
                x_ = 0;
                table_->LoadBlock(current_, &block_);
            }
        }

        // Position at the first key in the source that is at or past target.
        // The iterator is Valid() after this call iff the source contains
        // an entry that comes at or past target.
        virtual void Seek(const Slice& target) {
            current_ = table_->Seek(target, block_, ptr_, ptr_location_);
        }

        // Moves to the next entry in the source.  After this call, Valid() is
        // true iff the iterator was not positioned at the last entry in the source.
        // REQUIRES: Valid()
        virtual void Next() {
            assert(Valid());
            if (block_.next_ != blankblock) {
                current_ = block_.next_;
                table_->LoadBlock(current_, &block_);
                return;
            }
            for (x_++; x_ < table_->hash_range_; ++x_) {
                current_ = GetPtr(x_);
                if (current_ != nvLinearHash::blankblock) {
                    table_->LoadBlock(current_, &block_);
                    return;
                }
            }
            current_ = blankblock;
        }

        void RandNext() {
            for (size_t i = 0; i < 100; ++i) {
                x_ = rand_.Next() % table_->hash_range_;
                if (block_.next_ != blankblock) {
                    current_ = block_.next_;
                    table_->LoadBlock(current_, &block_);
                    return;
                }
                for (x_++; x_ < table_->hash_range_; ++x_) {
                    current_ = GetPtr(x_);
                    if (current_ != nvLinearHash::blankblock) {
                        table_->LoadBlock(current_, &block_);
                        return;
                    }
                }
            }
            current_ = blankblock;
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
            assert(Valid());
            return Slice(block_.key_data_, block_.key_size_);
        }

        // Return the value for the current entry.  The underlying storage for
        // the returned slice is valid only until the next modification of
        // the iterator.
        // REQUIRES: Valid()
        virtual Slice value() const {
            assert(Valid());
            return table_->GetValue(block_);
        }

        // If an error has occurred, return it.  Else return an ok status.
        virtual Status status() const {
            return Status::OK();
        }
    };
    HashIterator* NewHashIterator() {
        return new HashIterator(this);
    }
    void CheckValid() {}
    nvLinearHash(const nvLinearHash&) = delete;
    nvLinearHash& operator=(const nvLinearHash&) = delete;
};

struct nvFixedHashTable : nvMemTable {
    NVM_Manager* mng_;
    CachePolicy cp_;
    ull full_size_, nearly_full_size_, written_size_;
    nvLinearHash table_;
    D1SkipList* imm_;

    std::string dbname_;
    ull seq_;
    ull created_time_;
    ull pre_write_;

    std::string lft_bound_;
    std::string rgt_bound_;

    port::RWLock lock_;
    bool is_immutable_;
    int refs__;
    std::string MemName(const Slice& dbname, ull seq) {
        return dbname.ToString() + std::to_string(seq) + ".nvskiplist";
    }
public:
    nvFixedHashTable(NVM_Manager* mng, const CachePolicy& cp, const Slice& dbname, ull seq, bool clean = true) :
        mng_(mng), cp_(cp), full_size_(cp.nvskiplist_size_), nearly_full_size_(cp.nearly_full_size_), written_size_(0),
        table_(mng, full_size_, cp.hash_range_, cp.hash_full_limit_, clean),
        imm_(nullptr), dbname_(dbname.ToString()), seq_(seq), created_time_(0), pre_write_(0),
        lft_bound_(), rgt_bound_(), lock_(), is_immutable_(false), refs__(0) {
        mng_->bind_name(MemName(dbname_, seq_), table_.Location());
    }
    virtual ~nvFixedHashTable() {
        if (imm_ != nullptr)
            delete imm_;
    }

    virtual void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) {
        table_.Add(seq, type, key, value);
        written_size_ += SizeOf(key, value);
        //pre_write_ -= SizeOf(key, (type == kTypeDeletion ? Slice() : value));
    }
    virtual bool Get(const LookupKey& key, std::string* value, Status* s) {
        table_.Get(key, value, s);
    }
    virtual bool Immutable() const {
        return is_immutable_;
    }
    virtual void SetImmutable(bool state) {
        is_immutable_ = state;
        //imm_ = BuildImmutableMemTable();
    }
    virtual ull StorageUsage() const {
        return table_.StorageUsage();//std::max(pre_write_, table_.StorageUsage());
    }
    virtual ull DataWritten() const {
        return written_size_;
    }
    virtual ull BlankStorageUsage() const {
        return nvLinearHash::MainBlockSize + table_.hash_range_ * sizeof(nvOffset);
    }
    virtual ull UpperStorage() const {
        return 0;
    }
    virtual ull LowerStorage() const {
        return StorageUsage();
    }

    virtual bool HasRoomFor(ull size) const {
        return StorageUsage() + size < table_.arena_.full_size_;
    }
    virtual bool HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full) {
        ul size = key.size() + value.size() + 1024;
        ul bound = (nearly_full ? nearly_full_size_ : full_size_);
        return StorageUsage() + size < bound;
    }
    ul SizeOf(const Slice& key, const Slice& value) {
        return key.size() + value.size() + 32;
    }
    virtual bool PreWrite(const Slice& key, const Slice& value, bool nearly_full) {
        assert(false);
        //ul size = ;
        //pre_write_ += SizeOf(key, value);
        //ul bound = (nearly_full ? nearly_full_size_ : full_size_);
        //return pre_write_ < bound;
    }

    virtual void Ref() { refs__++; }
    virtual void Unref() {
        assert(refs__ > 0);
        if (--refs__ == 0) {
            delete this;
        }
    }

    virtual void Lock() {
        lock_.WriteLock();
    }
    virtual void Unlock() {
        lock_.Unlock();
    }
    virtual void SharedLock() {
        lock_.ReadLock();
    }
    virtual bool TryLock() {
        return lock_.TryWriteLock();
    }
    virtual bool TrySharedLock() {
        return lock_.TryReadLock();
    }

    virtual std::string Max() {
        WaitImm();
        return imm_->GetKey(imm_->SeekToLast()).ToString();
        //assert(false);
    }
    virtual std::string Min() {
        WaitImm();
        return imm_->GetKey(imm_->SeekToFirst()).ToString();
    }

    virtual Iterator* NewIterator() {
        assert(false);
    }
    virtual Iterator* NewOfficialIterator(ull seq) {
        WaitImm();
        return imm_->NewOfficialIterator(seq);
    }

    void WaitImm() {
        while (imm_ == nullptr) {
            nanodelay(100);
        }
        assert(imm_ != nullptr);
    }

    virtual void FillKey(std::vector<std::string> &divider, size_t size) {
        //static Random rand_(0xdeadbeef);
        divider.clear();
        if (size == 0) return;
        divider.push_back(LeftBound());
        if (size == 1) return;

        size_t check_size = size * 10, checked = 0;
        nvLinearHash::HashIterator* iter = table_.NewHashIterator();
        std::vector<std::string> sample;
        for (iter->SeekToFirst(); iter->Valid(); iter->RandNext()) {
            sample.push_back(iter->key().ToString());
            if (++checked >= check_size)
                break;
        }
        std::sort(sample.begin(), sample.end());
        //divider.push_back(lft_bound_);
        for (size_t i = 10; i < sample.size(); i += 10) {
            divider.push_back(sample[i]);
        }
        delete iter;
    }

    virtual nvMemTable* Rebuild(const Slice& lft, ull log_number) {
        nvFixedHashTable* table = new nvFixedHashTable(mng_, cp_, dbname_, log_number);
        table->LeftBound() = lft.ToString();
        return table;
    }

    virtual void Connect(nvMemTable* b, bool reverse) {
        assert(false);
    }
    virtual void GarbageCollection() {
        assert(false);
    }
    virtual double Garbage() const {
        return table_.arena_.cache_.lost_ / full_size_;
    }

    virtual std::string& LeftBound() {
        return lft_bound_;
    }
    virtual std::string& RightBound() {
        return rgt_bound_;
    }
    virtual SequenceNumber Seq() const {
        return seq_;
    }

    virtual void Print() {
        assert(false);
    }
    virtual MemTable* Immutable(ull seq) {
        assert(false);
    }

    virtual void CheckValid() {
        table_.CheckValid();
    }
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
    D1SkipList* BuildImmutableMemTable() {
        D1SkipList* list = new D1SkipList(full_size_ * 2);
        Iterator* iter = table_.NewIterator();
        SequenceNumber maxseq = (1ULL<<56) - 1;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            Slice key = iter->key();
            Slice value = iter->value();
            list->Add(maxseq, (value.size() == 0 ? kTypeDeletion : kTypeValue), key, value);
        }
        delete iter;
        return list;
    }
};
}

#endif // NVHASHTABLE_H
