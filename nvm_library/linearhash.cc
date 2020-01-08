
#include "linearhash.h"

    LinearHash::LinearHash(size_t hash_size) : blocks_(nullptr), hash_size_(hash_size), total_key_(0) {
        blocks_ = new KVBlock[hash_size_];
    }
    LinearHash::~LinearHash() {
        delete [] blocks_;
    }
    size_t LinearHash::hash_(const leveldb::Slice& key) {
        return MurmurHash3_x86_32(key.data(), key.size()) % hash_size_;
    }
    int LinearHash::Seek(const leveldb::Slice& key) {

    }
    leveldb::Slice* LinearHash::NewSlice(const leveldb::Slice& key) {
        byte* buf = arena_.Allocate(sizeof(leveldb::Slice) + key.size());
        leveldb::Slice* s = reinterpret_cast<leveldb::Slice*>(buf);
        char* key_start = reinterpret_cast<char*>(buf + sizeof(leveldb::Slice));
        memcpy(key_start, key.data(), key.size());
        *s = leveldb::Slice(key_start, key.size());
        return s;
    }

    size_t LinearHash::Next(size_t i) const { return (i+1)%MaxHashSize; }
    bool LinearHash::Add(const leveldb::Slice& key, const leveldb::Slice& value) {
        size_t hash_start = hash_(key);

        if (blocks_[hash_start].key_ == nullptr) {
            total_key_++;
            //if (++total_key_ * 10 >= MaxHashSize) assert(false);
            leveldb::Slice* k = NewSlice(key);
            leveldb::Slice* v = NewSlice(value);
            blocks_[hash_start].Insert(k, v);
            return true;
        } else if (blocks_[hash_start].key_->compare(key) == 0) {
            leveldb::Slice* v = NewSlice(value);
            blocks_[hash_start].Update(v);
            return true;
        }

        for (size_t i = Next(hash_start); i != hash_start; i = Next(i)) {
            if (blocks_[i].key_ == nullptr) {
                total_key_++;
                leveldb::Slice* k = NewSlice(key);
                leveldb::Slice* v = NewSlice(value);
                blocks_[i].Insert(k, v);
                return true;
            } else if (blocks_[i].key_->compare(key) == 0) {
                leveldb::Slice* v = NewSlice(value);
                blocks_[i].Update(v);
                return true;
            }
        }
        return false;
    }

    bool LinearHash::Get(const leveldb::Slice& key, std::string* value, leveldb::Status *s) {
        size_t hash_start = hash_(key);
        if (blocks_[hash_start].key_ == nullptr)
            return false;
        if (blocks_[hash_start].key_->compare(key) == 0) {
            leveldb::Slice *v = blocks_[hash_start].value_;
            if (v == nullptr) {
                *s = leveldb::Status::NotFound(leveldb::Slice());
                return true;
            }
            value->assign(v->data(), v->size());
            *s = leveldb::Status::OK();
            return true;
        }

        for (size_t i = Next(hash_start); i != hash_start; i = Next(i)) {
            if (blocks_[i].key_ == nullptr)
                return false;
            if (blocks_[i].key_->compare(key) == 0) {
                leveldb::Slice *v = blocks_[i].value_;
                if (v == nullptr) {
                    *s = leveldb::Status::NotFound(leveldb::Slice());
                    return true;
                }
                value->assign(v->data(), v->size());
                *s = leveldb::Status::OK();
                return true;
            }
        }
        return false;
    }
    void LinearHash::Clear() {
        memset(blocks_, 0, sizeof(KVBlock) * hash_size_);
        arena_.DisposeAll();
    }
    double LinearHash::HashRate() const { return 1. * total_key_ / hash_size_; }

