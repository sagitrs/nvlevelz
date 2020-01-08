#ifndef LINEARHASH_H
#define LINEARHASH_H
#include <vector>
#include "global.h"
#include "leveldb/slice.h"
#include "murmurhash.h"
#include "leveldb/status.h"

struct LinearHash {
    struct Arena {
    public:
        std::vector<byte*> block_;
        static const size_t BlockSize = 1 * MB;
        //size_t current_block_;

        byte* alloc_ptr_;
        ull alloc_remaining_;

        enum {TotalSize = 0, NodeBound = 8, ValueBound = 16, HeadAddress = 24, MemTableInfoSize = 32 };

        Arena() :
            block_(), //current_block_(0),
            alloc_ptr_(nullptr), alloc_remaining_(0)
        {
        }
        ~Arena() {
            DisposeAll();
        }
        byte* Allocate(size_t size) {
            if (size > alloc_remaining_)
                return AllocateNewBlock(size);
            byte* ans = alloc_ptr_;
            alloc_remaining_ -= size;
            alloc_ptr_ += size;
            return ans;
        }
        byte* AllocateNewBlock(size_t size) {
            alloc_ptr_ = new byte[BlockSize];
            block_.push_back(alloc_ptr_);
            alloc_remaining_ = BlockSize;
            return Allocate(size);
        }

        void Dispose(byte* addr, byte* size) {
            ;
        }
        void DisposeAll() {
            for (size_t i = 0; i < block_.size(); ++i) {
                delete[] block_[i];
            }
            block_.clear();
            alloc_ptr_ = nullptr;
            alloc_remaining_ = 0;
        }
        Arena(const Arena&) = delete;
        void operator=(const Arena&) = delete;
    };
    Arena arena_;
    static const size_t MaxHashSize = 10 * MB;
    //static const ull MaxSpaceSize = 50 * MB;
    struct KVBlock {
        leveldb::Slice *key_;
        leveldb::Slice *value_;
        KVBlock() : key_(nullptr), value_(nullptr) {}
        void Update(leveldb::Slice* v) {
            value_ = v;
        }
        void Insert(leveldb::Slice* key, leveldb::Slice* value) {
            value_ = value;
            key_ = key;
        }
        bool Blank() const { return key_ == nullptr; }
        bool Equal(const leveldb::Slice& key) const {
            return !Blank() && key.compare(*key_) == 0;
        }
        bool Insertable(const leveldb::Slice& key) const {
            return Blank() || key.compare(*key_) == 0;
        }
    };
    KVBlock *blocks_;
    size_t hash_size_;
    uint32_t total_key_;
    LinearHash(size_t hash_size = MaxHashSize);
    ~LinearHash();
    size_t hash_(const leveldb::Slice& key);
    int Seek(const leveldb::Slice& key);
    leveldb::Slice* NewSlice(const leveldb::Slice& key);
    enum {
        BlockOffset = 0,
        KeySlice = sizeof(KVBlock), ValueSlice = KeySlice + sizeof(leveldb::Slice),
        KeyData = ValueSlice + sizeof(leveldb::Slice)
    };
    size_t Next(size_t i) const;
    bool Add(const leveldb::Slice& key, const leveldb::Slice& value);

    bool Get(const leveldb::Slice& key, std::string* value, leveldb::Status *s);
    void Clear();
    double HashRate() const;
};

#endif // LINEARHASH_H
