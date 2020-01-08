#ifndef BACKGROUNDWORKER_H
#define BACKGROUNDWORKER_H
#include <stdio.h>
#include <pthread.h>
#include "nvm_library/nvmemtable.h"
#include <deque>
//#include "db/db_impl.h"

namespace leveldb {

class DBImpl;
/*
struct BackgroundWorkers {
    DBImpl* db_;
    pthread_mutex_t mu_;
    pthread_cond_t bgsignal_;
    typedef D2MemTable* WorkType;
    typedef std::deque<WorkType> BGQueue;
    BGQueue queue_;

    bool started_workers_thread_;
    int thread_id_;
    static const int MAX_WORKER_THREAD = 8;
    pthread_t bgthread_id_[MAX_WORKER_THREAD];
    bool working[MAX_WORKER_THREAD];

    static void PthreadCall(const char* label, int result);
    BackgroundWorkers(DBImpl* db);
    void StartWorkerThread();
    void WakeupOneWorker();
    static void* WorkerThreadWrapper(void* arg);
    void PushWorkToQueue(WorkType mem);
    void BackGroundWorker();
};
*/
}

#endif
