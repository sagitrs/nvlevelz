#include "backgroundwriter.h"

namespace leveldb {

BackgroundWriter::~BackgroundWriter() {}

size_t BackgroundWriter_Mutex::threads_count_ = 0;

void BackgroundWriter_Mutex::SetClearForce() {
    MutexLock lock(&mu_);
    SetClear(queue_.empty());
}

BackgroundWriter_Mutex::WorkType* BackgroundWriter_Mutex::NewWorkType() {
    return new (master_arena_.Allocate(sizeof(WorkType))) WorkType();
}

void BackgroundWriter_Mutex::SetClear(bool sleeping) {
    mu_.AssertHeld();
    queue_.push_back(NewWorkType());//new WorkType);     // Clear Signal.
    if (sleeping)
        cv_.Signal();
    //mu_.Unlock();
    while (!queue_.empty())
        cv_.Wait();
}

bool BackgroundWriter_Mutex::PushWorkToQueue(nvMemTable* mem, const Slice& key, const Slice& value) {
    //MutexLock lock(&mu_);
    mu_.AssertHeld();
    assert(bgthread_created_ == true);
    bool sleeping = queue_.empty();

    nvOffset offset = log_->totalSize();
    size_t key_vsize = VarintLength(key.size()), value_vsize = VarintLength(value.size());
    size_t l = key_vsize + value_vsize + key.size() + value.size();

    if (!HasRoomFor(l)) {
        SetClear(sleeping);
        assert(HasRoomFor(l));
    }
    char* t = master_arena_.Allocate(l);//new char[l];
    char* p = t;
    p = EncodeVarint32(p, key.size());
    memcpy(p, key.data(), key.size()); p += key.size();
    p = EncodeVarint32(p, value.size());
    memcpy(p, value.data(), value.size());
    assert(p + value.size() == t + l);

    log_->append(t, l);
    log_size_ += l;
    log_counts_ ++;

//    delete[] t;

    WorkType * work = NewWorkType();//new WorkType;
    work->mem_ = mem;
    work->key_size_ = key.size();
    work->key_data_ = offset + key_vsize;
    work->value_size_ = value.size();
    work->value_data_ = work->key_data_ + key.size() + value_vsize;

    string sk, sv;
    sk.assign(key.data(), key.size());
    sv.assign(value.data(), value.size());
    hash_[sk] = sv;
    queue_.push_back(work);
    if (sleeping)
        cv_.Signal();
    return true;
}

bool BackgroundWriter_Mutex::MakeRoomForWrite(nvMemTable* mem, const Slice& key, const Slice& value) {
    mu_.AssertHeld();
    assert(bgthread_created_ == true);
    bool sleeping = queue_.empty();
    size_t max_length = log_size_ + log_counts_*(1+4+4*12) + key.size() + value.size() + 8;
    if (!mem->HasRoomFor(max_length)) {
        //SetClear(sleeping);
        return false;
    }
    if (!HasRoomFor(key.size() + value.size() + 8))
        SetClear(sleeping);
    assert(HasRoomFor(key.size() + value.size() + 8));
    return true;
}

bool BackgroundWriter_Mutex::Get(const LookupKey& key, std::string* value, Status* s) {
  //MutexLock lock(&mu_);
  std::string k = key.user_key().ToString();
  auto p = hash_.find(k);
  if (p == hash_.end()) {
      return false;
  }
  const std::string& v = p->second;
  if (v.size() == 0) {
      *s = Status::NotFound(Slice());
      return true;
  }
  value->assign(v);
  *s = Status::OK();
  return true;
}

static void PthreadCall(const char* label, int result) {
    if (result != 0) {
        fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
        abort();
    }
}

BackgroundWriter_Mutex::BackgroundWriter_Mutex(NVM_File* log, size_t id) :
    mu_(), cv_(&mu_),
    bgthread_created_(false), shutdown_(false),
    bgthread_id_(0), id_(threads_count_++),
    hash_(), log_(log), log_size_(0),log_counts_(0), clear_mark_(false) {
    DoClear();
}
BackgroundWriter_Mutex::~BackgroundWriter_Mutex() {
    ShutdownWorkerThread();
    mu_.Unlock();
}

void BackgroundWriter_Mutex::StartWorkerThread() {
        pthread_create(&bgthread_id_, NULL, &BackgroundWriter_Mutex::WorkerThreadWrapper, this);
        bgthread_created_ = true;
}

void* BackgroundWriter_Mutex::WorkerThreadWrapper(void* arg) {
    reinterpret_cast<BackgroundWriter_Mutex*>(arg)->BackgroundWork();
}

bool BackgroundWriter_Mutex::HasRoomFor(size_t l) {
    return log_->totalSize() + l < log_->totalSpace();
}


void BackgroundWriter_Mutex::ShutdownWorkerThread() {
    mu_.Lock();
    shutdown_ = true;
    if (queue_.empty())
        cv_.Signal();
    cv_.Wait();
    delete log_;
}

void BackgroundWriter_Mutex::DoClear() {
    hash_.clear();
    assert(queue_.empty());
    log_->clear();
    log_size_ = 0;
    log_counts_ = 0;
    master_arena_.DisposeAll();
    slave_arena_.DisposeAll();
}
size_t BackgroundWriter_Mutex::DoWrite(WorkType* work) {
    assert(work->mem_ != nullptr);
    char* key = slave_arena_.Allocate(work->key_size_);//new char[work->key_size_];
    char* value = work->value_size_ == 0 ? nullptr : slave_arena_.Allocate(work->value_size_);//new char[work->value_size_];
    log_->read(reinterpret_cast<byte*>(key), work->key_size_, work->key_data_);
    if (work->value_size_ > 0)
        log_->read(reinterpret_cast<byte*>(value), work->value_size_, work->value_data_);

    work->mem_->Add(0,
                    work->value_size_ == 0 ? kTypeDeletion : kTypeValue,
                    Slice(key, work->key_size_),
                    Slice(value, work->value_size_));
//    delete[] key;
//    if (value != nullptr)
//        delete[] value;
    return work->key_size_ + work->value_size_ + VarintLength(work->key_size_) + VarintLength(work->value_size_);
}

void BackgroundWriter_Mutex::BackgroundWork() {     // Background work, can not be called.
    size_t last_work = 0;
    while (true) {
        mu_.Lock();
        log_size_ -= last_work;
        if (queue_.empty()) {
            if (shutdown_) break;
            cv_.Wait();
            if (shutdown_) break;
            assert(!queue_.empty());
        }
        WorkType* work = queue_.front();
        queue_.pop_front();
        if (work->mem_ == nullptr) {
            DoClear();
            last_work = 0;
            cv_.Signal();
            mu_.Unlock();
        } else {
            mu_.Unlock();
            last_work = DoWrite(work);
        }
        //delete work;
    }
    cv_.Signal();
    mu_.Unlock();
}
}
