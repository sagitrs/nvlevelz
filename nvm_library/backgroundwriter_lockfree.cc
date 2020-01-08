#include "backgroundwriter_lockfree.h"

namespace leveldb {

size_t BackgroundWriter_LockFree::threads_count_ = 0;

bool BackgroundWriter_LockFree::PushWorkToQueue(nvMemTable* mem, nvAddr block_addr) {
    assert(bgthread_created_ == true);

    static const SequenceNumber maxseq = (1ULL<<56) - 1;

    //hash_->Add(maxseq, (value.size() == 0 ? kTypeDeletion : kTypeValue), key, value);
    //(*hash_)[key.ToString()] = value.ToString();
    //hash_->Add(key, value);
    WorkType work(mem, block_addr);
    AddWork(&work);/*
    {
        nvHashTable::HashBlock block;
        table_->GetBlock(block_addr, &block);
        Slice key = table_->GetKey(block);
        Slice value = table_->GetValue(block);
        if (key.size() > 10000) {
            nvHashTable::HashBlock block;
            table_->GetBlock(block_addr, &block);
            Slice key = table_->GetKey(block);
            Slice value = table_->GetValue(block);
        }
    }*/
    //list_.push_back(work);
    //assert( list_.size() == Tail() + 1 );
    //assert( list_[Tail()] != nullptr );
    //SetTail( list_.size() );

    return true;
}
/*
bool BackgroundWriter_LockFree::MakeRoomForWrite(nvMemTable* mem, const Slice& key, const Slice& value) {
    assert(false);
    return true;
}
*/
bool BackgroundWriter_LockFree::Get(const LookupKey& key, std::string* value, Status* s) {
    assert(false);
    return 0;  
}

void BackgroundWriter_LockFree::PthreadCall(const char* label, int result) {
    if (result != 0) {
        fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
        abort();
    }
}

BackgroundWriter_LockFree::BackgroundWriter_LockFree(NVM_Manager* mng, size_t id) :
    mng_(mng),
    bgthread_created_(false), shutdown_(false),
    bgthread_id_(0), id_(threads_count_++),
    queue_(1 * MB, sizeof(WorkType))
    {
}
void BackgroundWriter_LockFree::StartWorkerThread() {
        pthread_create(&bgthread_id_, NULL, &BackgroundWriter_LockFree::WorkerThreadWrapper, this);
        bgthread_created_ = true;
}

void* BackgroundWriter_LockFree::WorkerThreadWrapper(void* arg) {
    reinterpret_cast<BackgroundWriter_LockFree*>(arg)->BackgroundWork();
}

void BackgroundWriter_LockFree::DoWrite(WorkType* work) {

    nvHashTable::HashBlock block;
    nvAddr addr = work->block_addr_;
    mng_->read_barrier(reinterpret_cast<byte*>(&block), addr, sizeof(nvHashTable::HashBlock));
    //nvHashTable::GetBlock(addr, &block);
    //if (block.key_size_ > 10000 || block.value_size_ > 10000) {
    //    mng_->read_barrier(reinterpret_cast<byte*>(&block), addr, sizeof(nvHashTable::HashBlock));
//    }

    Slice key = mng_->GetSlice(addr + nvHashTable::HashBlockSize, block.key_size_);//block.main_ + HashBlockSize, block.key_size_);
    Slice value = mng_->GetSlice(addr + nvHashTable::HashBlockSize + block.key_size_, block.value_size_);//block.main_ + HashBlockSize + block.key_size_, block.value_size_);

    work->mem_->Add(0,
                    value.size() == 0 ? kTypeDeletion : kTypeValue,
                    key,
                    value);
}

void BackgroundWriter_LockFree::BackgroundWork() {     // Background work, can not be called.
    //size_t last_work = 0;
    WorkType work;
    while (true) {
        while (queue_.Empty() && !shutdown_) {
            usleep(5);
        }
        if (shutdown_) break;
        queue_.PopFront(&work);
        DoWrite(&work);
    }
    this->bgthread_created_ = false;
}
}
