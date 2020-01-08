#ifndef BACKGROUNDWRITER_H
#define BACKGROUNDWRITER_H

#include <stdio.h>
#include <pthread.h>
#include "nvm_library/nvmemtable.h"
#include "db/dbformat.h"
#include <deque>
#include <unordered_map>
#include "util/mutexlock.h"
#include "port/port_posix.h"

//#include "db/db_impl.h"

namespace leveldb {

class DBImpl;

struct BackgroundWriter {

public:
    virtual ~BackgroundWriter() = 0;
    virtual void StartWorkerThread() = 0;
    virtual void ShutdownWorkerThread() = 0;
    virtual bool HasRoomFor(size_t l) = 0;
    virtual void Lock() = 0;
    virtual void UnLock() = 0;

    virtual void SetClearForce() = 0;
    virtual bool GetClearMark() const = 0;
    virtual bool CheckClear() const = 0;
    virtual void SetClearMark(bool mark) = 0;
    virtual bool PushWorkToQueue(nvMemTable* mem, const Slice& key, const Slice& value) = 0;
    virtual bool MakeRoomForWrite(nvMemTable* mem, const Slice& key, const Slice& value) = 0;
    virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;
};

struct BackgroundWriter_Mutex : BackgroundWriter {
private:
    port::Mutex mu_;
    Arena master_arena_, slave_arena_;
    port::CondVar cv_;
    bool bgthread_created_;
    bool shutdown_;
    pthread_t bgthread_id_;
    static size_t threads_count_;
    size_t id_;
    struct WorkType {
        nvMemTable* mem_;       // if mem_ == nullptr, it means "I locked for you, just finish all your jobs and clean your log&hash."
        nvOffset key_data_, key_size_;
        nvOffset value_data_, value_size_;
        WorkType() : mem_(nullptr), key_data_(0), key_size_(0), value_data_(0), value_size_(0) {}
    };

    std::unordered_map<std::string, std::string> hash_;
    std::deque<WorkType*> queue_;
    NVM_File *log_;
    size_t log_size_;
    size_t log_counts_;
    bool clear_mark_;

    static void PthreadCall(const char* label, int result);
    static void* WorkerThreadWrapper(void* arg);
    WorkType* NewWorkType();
    //void ShutDown();
    void DoClear();
    size_t DoWrite(WorkType* work);
    void BackgroundWork();
    void SetClear(bool sleeping);
    bool CheckClear() const {
        return 1;
    }
public:
    BackgroundWriter_Mutex(NVM_File* log, size_t id);
    ~BackgroundWriter_Mutex();
    void StartWorkerThread();
    void ShutdownWorkerThread();
    bool HasRoomFor(size_t l) ;

    void Lock() {mu_.Lock();}
    void UnLock() {mu_.Unlock();}

    void SetClearForce();
    bool GetClearMark() const { return clear_mark_; }
    void SetClearMark(bool mark) { clear_mark_ = mark; }

    bool PushWorkToQueue(nvMemTable* mem, const Slice& key, const Slice& value);
    bool MakeRoomForWrite(nvMemTable* mem, const Slice& key, const Slice& value);
    bool Get(const LookupKey& key, std::string* value, Status* s);
};

}


#endif // BACKGROUNDWRITER_H
