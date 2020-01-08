#ifndef NVHASH_H
#define NVHASH_H

#include <vector>
#include "global.h"
#include "leveldb/slice.h"
#include "murmurhash.h"
#include "leveldb/status.h"
#include "nvm_manager.h"
#include "db/dbformat.h"

namespace leveldb {

struct nvHashTable {
    struct Narena {
    public:
        NVM_Manager* mng_;
        ull full_size_;
        std::vector<nvAddr> block_;
        nvAddr main_block_;
        static const size_t BlockSize = 16 * MB;
        //size_t current_block_;
        nvAddr alloc_ptr_;
        ull alloc_remaining_;

        //enum {TotalSize = 0, NodeBound = 8, ValueBound = 16, HeadAddress = 24, MemTableInfoSize = 32 };
        void SetNext(nvAddr block, nvAddr next) {
            mng_->write_addr(block, next);
        }
        nvAddr NewBlock() {
            nvAddr x = mng_->Allocate(BlockSize);
            nvAddr prev = main_block_;
            if (block_.size() > 0)
                prev = block_[block_.size() - 1];
            SetNext(x, nvnullptr);
            SetNext(prev, x);
            block_.push_back(x);
            return x;
        }

        Narena(NVM_Manager* mng, ull size) :
            mng_(mng), full_size_(size),
            block_(), //current_block_(0),
            alloc_ptr_(nvnullptr), alloc_remaining_(0)
        {
            main_block_ = mng_->Allocate(BlockSize);
            SetNext(main_block_, nvnullptr);
            alloc_ptr_ = main_block_ + sizeof(nvAddr);
            alloc_remaining_ = BlockSize - sizeof(nvAddr);
        }
        ~Narena() {
            DisposeAll();
        }
        nvAddr Allocate(nvOffset size) {
            if (size > alloc_remaining_)
                return AllocateNewBlock(size);
            nvAddr ans = alloc_ptr_;
            alloc_remaining_ -= size;
            alloc_ptr_ += size;
            return ans;
        }
        nvAddr AllocateNewBlock(nvOffset size) {
            nvAddr newblock = NewBlock();
            nvAddr ans = newblock + sizeof(nvAddr);
            alloc_ptr_ = ans + size;
            alloc_remaining_ = BlockSize - sizeof(nvAddr) - size;
            return ans;
        }

        void Dispose(nvAddr addr, nvOffset size) {

        }
        void DisposeAll() {
            if (block_.size() == 0) return;
            for (size_t i = block_.size() - 1; i > 0; --i) {
                size_t prev = block_[i - 1];
                SetNext(prev, nvnullptr);
                mng_->Dispose(block_[i], BlockSize);
            }
            mng_->write_addr(main_block_, nvnullptr);
            mng_->Dispose(block_[0], BlockSize);
            block_.clear();
            alloc_ptr_ = main_block_ + sizeof(nvAddr);
            alloc_remaining_ = BlockSize - sizeof(nvAddr);
            // Remember : main_block_ is not released. it shall be released by nvHashTable.
        }
        Narena(const Narena&) = delete;
        void operator=(const Narena&) = delete;
    };
    Narena arena_;
    //static const size_t MaxHashSize = 10 * MB;
    //static const ull MaxSpaceSize = 50 * MB;
    enum MainBlockOffset {
        HashRangeOffset = 0,
        HashHeadOffset = HashRangeOffset + 8,
        ArenaBlockHead = HashHeadOffset + 8,
        MainBlockLength = ArenaBlockHead + 8
    };
    enum BlockOffset {
        NextAddr = 0,
        //HashCheck = NextAddr + 8,
        KeySize = NextAddr + 8, ValueSize = KeySize + 4,
        HashBlockSize = ValueSize + 4
        //KeyData = ValueSize + 4, ValueData = KeyData + 8,
        //KeyStart = ValueData + 4, ValueStart = ValueData + 8 + KeySize
        //BlockLength = ValueData + 8,
    };
    NVM_Manager* mng_;
    ull hash_range_, total_key_, full_range_;
    ull hash_size_, total_size_;
    ull garbage_size_;

    nvAddr hash_main_, hash_block_;

    nvAddr HashBlockMain() const { return hash_block_; }
    nvAddr HashDataMain() const { return arena_.main_block_; }

    nvAddr GetBlockAddr(nvOffset key) {
        return mng_->read_addr(hash_block_ + key * sizeof(nvAddr));
    }
    void SetBlockAddr(nvOffset key, nvAddr value) {
        mng_->write_addr(hash_block_ + key * sizeof(nvAddr), value);
    }
    struct HashBlock {
        nvAddr next_;
        //uint32_t check_hash_;
        uint32_t key_size_;
        uint32_t value_size_;
        HashBlock() : next_(nvnullptr), key_size_(0), value_size_(0) {}
        //nvAddr key_data_ = main_ + 12;
        //nvAddr value_data_ = main_ + 12 + key_size_;
    };
    void LoadBlock(nvAddr addr, HashBlock* block) {
        mng_->read_barrier(reinterpret_cast<byte*>(block), addr, sizeof(HashBlock));
        //block->main_ = addr;
    }
    Slice GetKey(nvAddr addr, const HashBlock& block) {
        return mng_->GetSlice(addr + HashBlockSize, block.key_size_);//block.main_ + HashBlockSize, block.key_size_);
    }
    Slice GetValue(nvAddr addr, const HashBlock& block) {
        return mng_->GetSlice(addr + HashBlockSize + block.key_size_, block.value_size_);//block.main_ + HashBlockSize + block.key_size_, block.value_size_);
    }
    nvAddr NewBlock(const Slice& key, const Slice& value, nvAddr next) {
        size_t l = HashBlockSize + key.size() + value.size();
        nvAddr x = arena_.Allocate(l);
        assert( x != nvnullptr );
        char *buf = new char[l];
        char* p = buf;
//        EncodeFixed32(p, check_hash); p += 4;
        EncodeFixed64(p, next); p += 8;
        EncodeFixed32(p, key.size()); p += 4;
        EncodeFixed32(p, value.size()); p += 4;
        memcpy(p, key.data(), key.size()); p += key.size();
        if (value.size() > 0) {
            memcpy(p, value.data(), value.size());
            p += value.size();
        }
        assert(buf + l == p);
        mng_->write(x, reinterpret_cast<byte*>(buf), l);
        delete[] buf;
        return x;
    }

    nvHashTable(NVM_Manager* mng, ull hash_range, ull hash_size, double full_limit) :
        arena_(mng, hash_size),
        mng_(mng),
        hash_range_(hash_range - MainBlockLength / sizeof (nvAddr)),
        total_key_(0),
        full_range_(hash_range_ * full_limit),
        hash_size_(hash_size - hash_range_ * sizeof(nvAddr)),
        total_size_(0),
        garbage_size_(0),
        hash_main_(nvnullptr), hash_block_(nvnullptr)
    {
        Reset();
    }
    ~nvHashTable() {
        arena_.DisposeAll();
        mng_->write_ull(hash_main_ + ArenaBlockHead, nvnullptr);
        mng_->Dispose(arena_.main_block_, Narena::BlockSize);
        mng_->Dispose(hash_main_, MainBlockLength + hash_range_ * sizeof(nvAddr));
    }
    void Reset() {
        nvAddr new_main = mng_->Allocate(MainBlockLength + hash_range_ * sizeof(nvAddr));
        CleanBlocks(new_main + MainBlockLength, hash_range_);
        mng_->write_ull(new_main + HashRangeOffset, hash_range_);
        mng_->write_ull(new_main + HashHeadOffset, hash_block_);
        mng_->write_ull(new_main + ArenaBlockHead, HashDataMain());
        nvAddr old_main = hash_main_;
        hash_main_ = new_main;
        hash_block_ = hash_main_ + MainBlockLength;
        if (old_main != nvnullptr)
            mng_->Dispose(old_main, MainBlockLength + hash_range_ * sizeof(nvAddr));
        arena_.DisposeAll();
        total_key_ = 0;
        total_size_ = 0;
    }
    nvOffset hashA(const leveldb::Slice& key) {
        return MurmurHash3_x86_32(key.data(), key.size()) % hash_range_;
    }
    nvOffset hashB(const leveldb::Slice& key) {
        const nvOffset *p = reinterpret_cast<const nvOffset*>(key.data());
        nvOffset result = 0xdeadbeef;
        size_t l = key.size() / sizeof(nvOffset);
        for (size_t i = 0; i < l; ++i)
            result = result ^ p[i];
        return result;
    }

    nvAddr Add(const leveldb::Slice& key, const leveldb::Slice& value);
    bool Get(const LookupKey& lkey, std::string* value, Status *s) {
        return Get_(lkey.user_key(), value, s);
    }
    bool Get_(const leveldb::Slice& key, std::string* value, leveldb::Status *s);
    /*
    double HashRate() const {
        return 1. * total_key_ / hash_range_;
    }*/
    bool Full() const {
        return (total_size_ > hash_size_) || (total_key_ > full_range_);
    }
    void CleanBlocks(nvAddr block, nvAddr range) {
        mng_->write_zero(block, range * sizeof(nvAddr));/*
        nvAddr x = GetBlockAddr(1048575);
        SetBlockAddr(1048575, 12345);
        x = GetBlockAddr(1048575);
        SetBlockAddr(1048575, x);
        x = GetBlockAddr(1048575);
        SetBlockAddr(1048575, 0);
        x = GetBlockAddr(1048575);*/
    }
};

}

#endif // NVHASH_H
