#ifndef NVHASHTABLE_CC
#define NVHASHTABLE_CC
#include "nvhashtable.h"

namespace leveldb {
void nvLinearHash::Add_(const leveldb::Slice& key, const leveldb::Slice& value, SequenceNumber seq) {
    Block block;
    nvOffset ptr, ptr_location;
    if (!Seek(key, block, ptr, ptr_location)) {
        SetBlock(ptr_location, NewBlock(key, value, ptr));
        return;
    }
    nvOffset v = NewValue(value);
    if (block.value_ptr_ != blankblock) {
        ul oldvsize = GetValueSize(block.value_ptr_) + ValueDataOffset;
        UpdateBlock(ptr, v);
        arena_.Dispose(block.value_ptr_, oldvsize);
    } else
        UpdateBlock(ptr, v);
}

bool nvLinearHash::Get(const LookupKey& lkey, std::string* value, Status *s) {
    return Get_(lkey.user_key(), value, s);
}

bool nvLinearHash::Seek(const leveldb::Slice& key, Block& block, nvOffset& block_ptr, nvOffset& block_ptr_location) {
    nvOffset ha = hash(key);//, hb = hashB(key);
    block_ptr_location = hash_block_offset_ + ha * sizeof(nvOffset);  //means block_addr is loaded from main list.
    block_ptr = mng_->read_ul(arena_.main_ + block_ptr_location);
    int cmp = 0;
    while (true) {
        if (block_ptr == blankblock) // Tail of bucket.
            return false;
        LoadBlock(block_ptr, &block);
        cmp = key.compare(GetKey(block));
        if (cmp <= 0)
            return cmp == 0;    // Equal(found) | Larger(not found).
        block_ptr_location = block_ptr + BlockOffset::NextBlockOffset;
        block_ptr = block.next_;
    }
    assert(false);
    return false;
}

bool nvLinearHash::Get_(const leveldb::Slice& key, std::string* value, leveldb::Status *s) {
    Block block;
    nvOffset ptr, location;
    if (!Seek(key, block, ptr, location))
        return false;
    if (block.value_ptr_ == blankblock) {
        // Deleted.
        *s = Status::NotFound(Slice());
        return true;
    }
    Slice v = GetValue(block);
    value->assign(v.data(), v.size());
    *s = Status::OK();
    return true;
}

}

#endif // NVHASHTABLE_CC
