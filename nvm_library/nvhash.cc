#include "nvhash.h"


namespace leveldb {
nvAddr nvHashTable::Add(const leveldb::Slice& key, const leveldb::Slice& value) {
    //HashBlock debug_history[32]; size_t st = 0;
    nvOffset ha = hashA(key);//, hb = hashB(key);
    HashBlock block;
    nvAddr prev = hash_block_ + ha * sizeof(nvAddr);  //means block_addr is loaded from main list.
    nvAddr block_addr = mng_->read_addr(prev);

    while (true) {
        if (block_addr == nvnullptr) {
            total_key_ ++;
            total_size_ += key.size() + value.size() + HashBlockSize;
            nvAddr b = NewBlock(key, value, nvnullptr);
            mng_->write_ull_barrier(prev, b);
            return b;
            // Blank, Insert.
        }
        LoadBlock(block_addr, &block);
        //LoadBlock(block_addr, debug_history + st++);
        //assert(block.key_size_ == 16);
        if (GetKey(block_addr, block).compare(key) == 0) {
            total_size_ += key.size() + value.size() + HashBlockSize;
            nvAddr b = NewBlock(key, value, block.next_);
            mng_->write_ull_barrier(prev, b);
            //SetBlockAddr(ha, b);
            return b;
            //Same key, Update.
        }
        // Not this one, hash collapse, try next.
        prev = block_addr + BlockOffset::NextAddr;
        block_addr = block.next_;
        //if (block_addr != nvnullptr) {
        //    printf("Wait.\n");
        //}
    }
    //assert(total_key_ == hash_range_);
    assert(false);
    return 0;
}
bool nvHashTable::Get_(const leveldb::Slice& key, std::string* value, leveldb::Status *s) {
    //HashBlock debug_history[32]; size_t st = 0;
    nvOffset ha = hashA(key);//, hb = nulloffset;
    nvAddr prev = hash_block_ + ha * sizeof (nvAddr);
    nvAddr block_addr = mng_->read_addr(prev);
    HashBlock block;
    while (true) {
        if (block_addr == nvnullptr) {
            return false;
            // Not Found.
        }
        LoadBlock(block_addr, &block);
        //LoadBlock(block_addr, debug_history + st++);
        //assert(block.key_size_ == 16);
        if (GetKey(block_addr, block).compare(key) == 0) {
            if (block.value_size_ == 0) {
                *s = leveldb::Status::NotFound(Slice());
                return true;
                // Found : Deleted.
            } else {
                Slice v = GetValue(block_addr, block);
                value->assign(v.data(), v.size());
                return true;
                // Found : Value.
            }
        }
        // Not this one, hash collapse, try next.
        prev = block_addr + BlockOffset::NextAddr;
        block_addr = block.next_;
        //ha = (ha + 1) % hash_range_;
    }
    assert(false);
}
}
