#include "backgroundworker.h"
#include "db/db_impl.h"
namespace leveldb {
/*
void BackgroundWorkers::PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

BackgroundWorkers::BackgroundWorkers(DBImpl* db) :
    db_(db),
    bgsignal_(), started_workers_thread_(false), thread_id_(0)
{
    PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
    PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void BackgroundWorkers::StartWorkerThread() {
    assert( started_workers_thread_ == false );
    thread_id_ = 0;

    for (int i = 0; i < MAX_WORKER_THREAD; i++) {
        working[i] = false;
        pthread_create(bgthread_id_ + i, NULL, &BackgroundWorkers::WorkerThreadWrapper, this);
    }
    started_workers_thread_ = true;
}

void* BackgroundWorkers::WorkerThreadWrapper(void* arg) {
    reinterpret_cast<BackgroundWorkers*>(arg)->BackGroundWorker();
}

void BackgroundWorkers::WakeupOneWorker() {
    for (int i = 0; i < MAX_WORKER_THREAD; i++) {
        if (!working[i]) {
            PthreadCall("signal", pthread_cond_signal(&bgsignal_));
            break;
        }
    }
}

void BackgroundWorkers::PushWorkToQueue(WorkType mem) {
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    if (!started_workers_thread_)
        StartWorkerThread();
    if (queue_.empty())
        WakeupOneWorker();
    queue_.push_back(mem);

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}


void BackgroundWorkers::BackGroundWorker() {
    int id = -1;
    bool searching = 0;
    while (true) {
        PthreadCall("lock", pthread_mutex_lock(&mu_));
        if (id == -1) {
            id = thread_id_++;
            fprintf(stderr, "Worker %d is ready.\n", id);fflush(stderr);
        }
        while (queue_.empty()) {
            working[id] = false;
            if (searching) {
                //fprintf(stderr, "Failed.\n");fflush(stderr);
                searching = 0;
            }
            //fprintf(stderr, "Worker %d is waiting.\n", id);fflush(stderr);
            PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
            //fprintf(stderr, "Worker %d is finding...", id);fflush(stderr);
            searching = 1;
        }
        //fprintf(stderr, "OK.\n");fflush(stderr);
        searching = 0;
        L2MemTable* mem = queue_.front();
        queue_.pop_front();
        working[id] = true;

        PthreadCall("unlock", pthread_mutex_unlock(&mu_));
        db_->JuniorCompaction(mem);
    }
}
*/
}
