#ifndef HASHTABLEHELPER_H
#define HASHTABLEHELPER_H
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
#include "nvhashtable.h"
#include "myqueue.h"

namespace leveldb {
struct BackgroundHelper {
private:
    port::Mutex mu_;
    port::CondVar cv_;

public:
    bool bgthread_created_;
    bool shutdown_;

    pthread_t bgthread_id_;
    struct WorkType {
        enum Type {Clean = 0, CreateImmutableMemTable = 1};
        nvFixedHashTable* table_;
        Type type_;

        //WorkType() : mem_(nullptr), block_addr_(nulloffset) {}
        WorkType(nvFixedHashTable* mem, Type type) : table_(mem), type_(type) {}
        //ull TotalSize() const { return key_.size() + value_.size() + 8; }
    };
private:
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
    BackgroundHelper();
    ~BackgroundHelper() {
        //ShutdownWorkerThread();
    }
    void StartWorkerThread();
    void ShutdownWorkerThread() {
        shutdown_ = true;
        cv_.Signal();
        //cv_.Wait();
    }
    void Lock() { mu_.Lock(); }
    void UnLock() { mu_.Unlock(); }
    void WakeUp() { cv_.Signal(); }
    void Wait() { cv_.Wait(); }

    //bool GetClearMark() const { return clear_mark_; }
    //void SetClearMark(bool mark) { clear_mark_ = mark; }
    bool CheckClear() const {
        while (!queue_.Empty())
            usleep(1);
    }

    //void SetClearForce();
    bool PushWorkToQueue(nvFixedHashTable* mem, WorkType::Type type);
    //bool MakeRoomForWrite(nvMemTable* mem, const Slice& key, const Slice& value);
    bool Get(const LookupKey& key, std::string* value, Status* s);
};

}


#endif // HASHTABLEHELPER_H
