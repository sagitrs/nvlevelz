#ifndef BUFFER_AND_LOG_H
#define BUFFER_AND_LOG_H

#include "db/memtable.h"
#include "nvm_file.h"
#include "nvmemtable.h"
#include "skiplist_kvindram.h"
#include "d1skiplist.h"
namespace leveldb {
/*
struct L2MemTable_BufferLog : AbstractMemTable {
    NVM_Manager* mng_;
    MemTable *buffer_;
    NVM_File *log_;
    string name_;
    ull max_size_;
    L2MemTable_BufferLog(NVM_Manager* mng, ull size, const char* name)
        : mng_(mng), buffer_(nullptr), log_(nullptr), name_(name), max_size_(size) {
        log_ = new NVM_File(mng);
        buffer_ = L2MemTable::NewMemTable();
        mng_->bind_name(name, log_->location());
    }
    ~L2MemTable_BufferLog() {
        mng_->delete_name(name_);
        buffer_->Unref();
        delete log_;
    }
    void Add(SequenceNumber seq, ValueType type,
             const Slice& key,
             const Slice& value) {

        size_t key_size = key.size();
        size_t val_size = value.size();
        size_t internal_key_size = key_size + 8;
        const size_t encoded_len =
            VarintLength(internal_key_size) + internal_key_size +
            VarintLength(val_size) + val_size;
        char* buf = buffer_->arena_.Allocate(encoded_len);
        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);
        p += key_size;
        EncodeFixed64(p, (seq << 8) | type);
        p += 8;
        p = EncodeVarint32(p, val_size);
        memcpy(p, value.data(), val_size);
        assert((p + val_size) - buf == encoded_len);

        log_->append(buf, encoded_len);     // 1. Log
        buffer_->table_.Insert(buf);        // 2. Buffer
    }

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey& key, std::string* value, Status* s) {
        return buffer_->Get(key, value, s);
    }

    ull StorageUsage() const {
        return buffer_->ApproximateMemoryUsage();
    }

    void Clear() {
        buffer_->Unref();
        log_->clear();
        buffer_ = L2MemTable::NewMemTable();
    }
};

struct L2MemTable_D1SkipList : AbstractMemTable {
    NVM_Manager* mng_;
    D1SkipList *buffer_;
    NVM_File *log_;
    string name_;
    ull max_size_;
    L2MemTable_D1SkipList(NVM_Manager* mng, ull size, const char* name)
        : mng_(mng), buffer_(nullptr), log_(nullptr), name_(name), max_size_(size) {
        log_ = new NVM_File(mng);
        buffer_ = new D1SkipList(size);
        mng_->bind_name(name, log_->location());
    }
    ~L2MemTable_D1SkipList() {
        mng_->delete_name(name_);
        delete buffer_;
        delete log_;
    }
    void Add(SequenceNumber seq, ValueType type,
             const Slice& key,
             const Slice& value) {

        size_t key_size = key.size();
        size_t val_size = value.size();
        size_t internal_key_size = key_size + 8;
        const size_t encoded_len =
            VarintLength(internal_key_size) + internal_key_size +
            VarintLength(val_size) + val_size;
        char* buf = new char[encoded_len];
        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);
        p += key_size;
        EncodeFixed64(p, (seq << 8) | type);
        p += 8;
        p = EncodeVarint32(p, val_size);
        memcpy(p, value.data(), val_size);
        assert((p + val_size) - buf == encoded_len);

        log_->append(buf, encoded_len);     // 1. Log
        delete[] buf;
        buffer_->Add(seq, type, key, value);        // 2. Buffer
    }

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey& key, std::string* value, Status* s) {
        return buffer_->Get(key, value, s);
    }

    ull StorageUsage() const {
        return buffer_->StorageUsage();
    }

    void Clear() {
        delete buffer_;
        log_->clear();
        buffer_ = new D1SkipList(max_size_);
    }
};
*/
}


#endif // BUFFER_AND_LOG_H
