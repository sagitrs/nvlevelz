#include "hashtablehelper.h"

namespace leveldb {

//size_t BackgroundHelper::threads_count_ = 0;

bool BackgroundHelper::PushWorkToQueue(nvFixedHashTable* table, WorkType::Type type) {
    assert(bgthread_created_ == true);
    bool wakeup = queue_.Empty();
    WorkType work(table, type);
    AddWork(&work);
    if (wakeup)
        WakeUp();
    return true;
}

bool BackgroundHelper::Get(const LookupKey& key, std::string* value, Status* s) {
    assert(false);
}

void BackgroundHelper::PthreadCall(const char* label, int result) {
    if (result != 0) {
        fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
        abort();
    }
}

BackgroundHelper::BackgroundHelper() :
    mu_(), cv_(&mu_),
    bgthread_created_(false), shutdown_(false),
    bgthread_id_(0), queue_(1 * MB, sizeof(WorkType))
    {
}
void BackgroundHelper::StartWorkerThread() {
    pthread_create(&bgthread_id_, NULL, &BackgroundHelper::WorkerThreadWrapper, this);
    bgthread_created_ = true;
}

void* BackgroundHelper::WorkerThreadWrapper(void* arg) {
    reinterpret_cast<BackgroundHelper*>(arg)->BackgroundWork();
    //delete this;
}

void BackgroundHelper::DoWrite(WorkType* work) {
    switch (work->type_) {
    case WorkType::Clean: {
        work->table_->table_.CleanHashBlock();
    } break;
    case WorkType::CreateImmutableMemTable:{
        if (work->table_->imm_ == nullptr)
            work->table_->imm_ = work->table_->BuildImmutableMemTable();
    } break;
    default:
        assert(false);
    }

}

void BackgroundHelper::BackgroundWork() {     // Background work, can not be called.
    //size_t last_work = 0;
    WorkType work(nullptr, WorkType::Clean);
    while (true) {
        while (queue_.Empty() && !shutdown_) {
            cv_.Wait();
        }
        if (shutdown_)
            break;
        queue_.PopFront(&work);
        DoWrite(&work);
    }
    this->bgthread_created_ = false;
    //WakeUp();
    //delete this;
}
}
