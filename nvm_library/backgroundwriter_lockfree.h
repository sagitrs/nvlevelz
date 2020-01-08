#ifndef BACKGROUNDWRITER_LOCKFREE_H
#define BACKGROUNDWRITER_LOCKFREE_H

#include <stdio.h>
#include <pthread.h>
#include "nvm_library/nvmemtable.h"
#include "db/dbformat.h"
#include <deque>
#include <unordered_map>
#include "util/mutexlock.h"
#include "port/port_posix.h"
#include "backgroundwriter.h"
#include "util/arena.h"
#include <atomic>
#include "nvmemtable.h"
#include "d0skiplist.h"
//#include "db/db_impl.h"
//#include "linearhash.h"
#include "nvhash.h"
#include "myqueue.h"
namespace leveldb {
struct BackgroundWriter_LockFree/* : BackgroundWriter */{
private:
    NVM_Manager* mng_;
    port::Mutex mu_;
    //port::CondVar cv_;
    bool bgthread_created_;
    bool shutdown_;
    pthread_t bgthread_id_;
    static size_t threads_count_;
    size_t id_;
    struct WorkType {
        nvMemTable* mem_;       // if mem_ == nullptr, it means "I locked for you, just finish all your jobs and clean your log&hash."
        nvAddr block_addr_;
        WorkType() : mem_(nullptr), block_addr_(nulloffset) {}
        WorkType(nvMemTable* mem, nvAddr block_addr_) : mem_(mem), block_addr_(block_addr_) {}
        //ull TotalSize() const { return key_.size() + value_.size() + 8; }
    };
    MyQueue queue_;
    //nvHashTable* table_;

    bool clear_mark_;

    void AddWork(WorkType* work) {
        queue_.PushBack(work);
    }
    static void PthreadCall(const char* label, int result);
    static void* WorkerThreadWrapper(void* arg);
    void DoWrite(WorkType* work);
    void BackgroundWork();
public:
    BackgroundWriter_LockFree(NVM_Manager* mng, size_t id);
    ~BackgroundWriter_LockFree() {
        ShutdownWorkerThread();
    }
    void StartWorkerThread();
    void ShutdownWorkerThread() {
        shutdown_ = true;
        usleep(100);
    }
    void Lock() { mu_.Lock(); }
    void UnLock() { mu_.Unlock(); }

    //bool GetClearMark() const { return clear_mark_; }
    //void SetClearMark(bool mark) { clear_mark_ = mark; }
    bool CheckClear() const {
        while (!queue_.Empty())
            usleep(1);
    }

    //void SetClearForce();
    bool PushWorkToQueue(nvMemTable* mem, nvAddr block_addr);
    //bool MakeRoomForWrite(nvMemTable* mem, const Slice& key, const Slice& value);
    bool Get(const LookupKey& key, std::string* value, Status* s);
};
}


#endif // BACKGROUNDWRITER_LOCKFREE_H
