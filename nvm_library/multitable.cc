#include "multitable.h"
#include "nvskiplist.h"
#include "mixedskiplist_connector.h"
#include "d2skiplist.h"
namespace leveldb {

std::string nvMultiTable::commonPrefix(const std::string& s1, const std::string& s2) {
    size_t l1 = s1.size(), l2 = s2.size();
    size_t m = (l1 < l2 ? l1 : l2);
    for (size_t i = 0; i < m; ++i)
        if (s1[i] != s2[i]) return s1.substr(0,i);
    return (l1 < l2 ? s1 : s2);
}
nvMultiTable::nvMultiTable(DBImpl* db, const Options& options, const string &dbname):
    options_(options),
    memType_(options.TEST_nvm_accelerate_method == BUFFER_WITH_LOG ? WITH_BUFFER_AND_LOG : MSL_ONLY),
    comparator_(options.comparator), env_(options.env), mng_(env_->NVM_Env()->mng_),
    cache_policy_(
        options.TEST_max_nvm_memtable_size,     // Size of single memTable
        options.TEST_halfmax_nvm_memtable_size,
        options.TEST_max_dram_buffer_size,              // Size of dram
        options.TEST_max_nvm_buffer_size - options.TEST_nvm_buffer_reserved,    // Size of nvm
        options_.TEST_cover_range,               // Overlap range
        options_.TEST_hash_div),
    dbname_(dbname), index_(nullptr),
    writer_num_( options.TEST_write_thread ),
    writers_(nullptr),
    leveli_(256, sizeof(nvHashTable*)), helper_(nullptr),
    level0_(256, sizeof(nvMemTable*)), isCompactingLevel0_(false),
    //cache_(mng_, options.TEST_hash_range, options.TEST_hash_size, options.TEST_hash_full_limit),
    cache_(new nvHashTable(mng_, cache_policy_.hash_range_, options.TEST_hash_size, options.TEST_hash_full_limit)),
    imm_cache_(new nvHashTable(mng_, cache_policy_.hash_range_, options.TEST_hash_size, options.TEST_hash_full_limit)),
    //pool_(new BackgroundWorkers(db)),
    file_in_use_(),
    prev_poped_(""), node_total_(0), seq_(0), log_number_(0), bytes_(0), oprs_(0), clear_mark_(false) {
        BuildWriters(mng_, options, dbname);
        if (options_.TEST_nvskiplist_type == kTypeLinearHash)
            FillLevelI();
}

void nvMultiTable::BuildWriters(NVM_Manager* mng, const Options& options, const string &dbname) {
    if (options.TEST_nvskiplist_type == kTypeLinearHash) {
        helper_ = new BackgroundHelper();
        helper_->StartWorkerThread();
    }
    if (options.TEST_nvm_accelerate_method != BUFFER_WITH_LOG) {
        return;
    }
    writers_ = new BackgroundWriter_LockFree*[options.TEST_write_thread];
    for (int i = 0; i < options.TEST_write_thread; ++i) {
        //std::string log_name = dbname + "/" + NumToString(i, 16) + ".nvlog";
        //NVM_File* log = new NVM_File(mng);
        //log->openType_ = NVM_File::READ_WRITE;
        //log->setName(log_name);
        //mng->bind_name(log_name, log->location());
        //if (options.TEST_background_lock_free) {
        writers_[i] = new BackgroundWriter_LockFree(mng_, i);
        //} else {
        //    result[i] = new BackgroundWriter_Mutex(log, i);
        //}
        writers_[i]->StartWorkerThread();
    }
}

nvMemTable* nvMultiTable::WhereIs(const Slice& key) {
    nvMemTable* mem = (index_.FuzzyFind(key));
    //if (mem->LeftBound().size() > 0 && mem->LeftBound()[0] != '0') {
    //    CheckIndexValid();
    //}
    return mem;
}
/*
void nvMultiTable::Add(SequenceNumber seq, ValueType type,
         const Slice& key,
         const Slice& value) {
    WhereIs(key)->Add(seq, type, key, value);
}
bool nvMultiTable::Get(const LookupKey& key, std::string* value, Status* s) {
    return WhereIs(key.user_key())->Get(key, value, s);
}
*/
bool nvMultiTable::HasRoomForNewMem() {
    return node_total_ + level0_.Size() + leveli_.Size() + 2 <= cache_policy_.standard_nvmemtable_size_;
}

bool nvMultiTable::ReleaseAll() {
    if (options_.TEST_nvskiplist_type == kTypeLinearHash && helper_) {
        helper_->ShutdownWorkerThread();
        while (helper_->bgthread_created_);
        delete helper_;
    }
    if (writers_) {
        for (int i = 0; i < options_.TEST_write_thread; ++i) {
            //writers_[i]->
            writers_[i]->ShutdownWorkerThread();
            //delete writers_[i];
        }
        env_->SleepForMicroseconds(1000);
        for (int i = 0; i < options_.TEST_write_thread; ++i) {
            //writers_[i]->
            //writers_[i]->ShutdownWorkerThread();
            delete writers_[i];
        }
        delete[] writers_;
    }
    auto iter = index_.NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        nvMemTable * mem = (iter->Data());
        if (mem) mem->Unref();
    }
    delete iter;
    while (!level0_.Empty()) {
        //nvMemTable* mem = level0_.front();
        //level0_.pop_front();
        //assert(mem);
        //mem->Unref();
        nvMemTable* mem = nullptr;
        level0_.PopFront(&mem);
        assert(mem);
        mem->Unref();
    }
    delete cache_;
    delete imm_cache_;
}

ll nvMultiTable::StorageUsage() const {
    return cache_policy_.nvskiplist_size_ * (node_total_ + level0_.Size());
}

void nvMultiTable::SetFileInUse(ull file_number, bool in_use) {
    if (in_use)
        file_in_use_.insert(file_number);
    else
        file_in_use_.erase(file_number);
}

bool nvMultiTable::FileInUse(ull file_number) const {
    return file_in_use_.find(file_number) != file_in_use_.end();
}

void nvMultiTable::Print() {

}

void nvMultiTable::Init(MemTable* mem) {
    if (node_total_ > 0) return;
    switch (options_.TEST_nvskiplist_type) {
    case kTypeD2SkipList: {
        //L2SkipList * list = new L2SkipList(mng_, cache_policy_);
        nvMemTable * blank = nullptr;
        //blank = new L2MemTable(list, "", mng_, dbname_, log_number_++);
        blank = new D2MemTable(mng_, cache_policy_, dbname_, log_number_++);
        blank->Ref();
        index_.Add("", blank);
        if (mem != nullptr) {
            DramKV_Skiplist * kvmem = DramKV_Skiplist::BuildFromMemtable(mem);
            auto iter = kvmem->NewIterator();
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                Slice key = iter->key().ToSlice();
                StaticSlice* vp = iter->valuePtr();
                Slice value = (vp ? vp->ToSlice() : Slice());
                blank->Add(0, (iter->isDeletion() ? kTypeDeletion : kTypeValue),
                           key, value);
            }
            delete iter;
        }
    } break;
    case kTypeLinearHash: {
        nvMemTable * blank = nullptr;
        blank = new nvFixedHashTable(mng_, cache_policy_, dbname_, log_number_++);
        blank->Ref();
        index_.Add("", blank);
        if (mem != nullptr) {
            DramKV_Skiplist * kvmem = DramKV_Skiplist::BuildFromMemtable(mem);
            auto iter = kvmem->NewIterator();
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                Slice key = iter->key().ToSlice();
                StaticSlice* vp = iter->valuePtr();
                Slice value = (vp ? vp->ToSlice() : Slice());
                blank->Add(0, (iter->isDeletion() ? kTypeDeletion : kTypeValue),
                           key, value);
            }
            delete iter;
        }
    } break;
    case kTypeHashedSkipList: {
        //L2SkipList * list = new L2SkipList(mng_, cache_policy_);
        nvMemTable * blank = nullptr;
        //blank = new L2MemTable(list, "", mng_, dbname_, log_number_++);
        blank = static_cast<nvMemTable*>(new D4MemTable(mng_, cache_policy_, dbname_, log_number_++));
        blank->Ref();
        index_.Add("", blank);
        if (mem != nullptr) {
            DramKV_Skiplist * kvmem = DramKV_Skiplist::BuildFromMemtable(mem);
            auto iter = kvmem->NewIterator();
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                Slice key = iter->key().ToSlice();
                StaticSlice* vp = iter->valuePtr();
                Slice value = (vp ? vp->ToSlice() : Slice());
                blank->Add(0, (iter->isDeletion() ? kTypeDeletion : kTypeValue),
                           key, value);
            }
            delete iter;
        }
    } break;
    case kTypePureSkiplist: {
        //L2SkipList * list = new L2SkipList(mng_, cache_policy_);
        nvMemTable * blank = nullptr;
        //blank = new L2MemTable(list, "", mng_, dbname_, log_number_++);
        blank = new D5MemTable(mng_, cache_policy_, dbname_, log_number_++);
        blank->Ref();
        index_.Add("", blank);
        if (mem != nullptr) {
            DramKV_Skiplist * kvmem = DramKV_Skiplist::BuildFromMemtable(mem);
            auto iter = kvmem->NewIterator();
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                Slice key = iter->key().ToSlice();
                StaticSlice* vp = iter->valuePtr();
                Slice value = (vp ? vp->ToSlice() : Slice());
                blank->Add(0, (iter->isDeletion() ? kTypeDeletion : kTypeValue),
                           key, value);
            }
            delete iter;
        }
    } break;
    default:
        assert(false);
    }

    node_total_ ++;
}

Iterator* nvMultiTable::NewIterator() {
  return new nvMultiTableIterator(this);
}

void nvMultiTable::ForcePop(Version* current, const Slice* except) {
    ul try_sample = PrepareListSize * 10;
    IndexIterator * iter = index_.NewIterator();
    iter->Seek(prev_poped_);
    iter->Next(); if (!iter->Valid()) iter->SeekToFirst();
    nvMemTable* mem = iter->Data();
    if (except->compare(mem->LeftBound()) == 0) {
        iter->Next(); if (!iter->Valid()) iter->SeekToFirst();
        mem = iter->Data();
    }
    ull lifetime = bytes_ - mem->Paramenter(nvMemTable::ParameterType::CreatedTime);
    double lowest_write_speed = 1. * mem->Paramenter(nvMemTable::ParameterType::WrittenSize) / lifetime;
    for (ul i = 1; i < try_sample; ++i) {
        iter->Next(); if (!iter->Valid()) iter->SeekToFirst();
        if (except->compare(mem->LeftBound()) == 0) {
            iter->Next(); if (!iter->Valid()) iter->SeekToFirst();
        }
        nvMemTable* cur = iter->Data();
        lifetime = bytes_ - cur->Paramenter(nvMemTable::ParameterType::CreatedTime);
        double write_speed = 1. * cur->Paramenter(nvMemTable::ParameterType::WrittenSize) / lifetime;
        if (write_speed < lowest_write_speed) {
            lowest_write_speed = write_speed;
            mem = cur;
        }
    }
    //nvMemTable* mem = iter->Data();
    assert(mem != nullptr);
    iter->Seek(mem->LeftBound());
    iter->Next();
    if (!iter->Valid()) {
        mem->RightBound() = "";
        iter->SeekToFirst();
        prev_poped_ = iter->Data()->LeftBound();
    } else {
        prev_poped_ = iter->Data()->LeftBound();
        mem->RightBound() = prev_poped_;
    }

    delete iter;

    if (mem->StorageUsage() == mem->BlankStorageUsage()) {
        DeleteKey(mem->LeftBound());
        mem->Unref();
    } else {
        //mem->CheckValid();
        //level0_.push_back(mem);
        //mem->SetImmutable(true);
        level0_.PushBack(&mem);
        if (options_.TEST_nvskiplist_type == kTypeLinearHash)
            helper_->PushWorkToQueue(reinterpret_cast<nvFixedHashTable*>(mem), BackgroundHelper::WorkType::CreateImmutableMemTable);
        global_ic_.AddCompaction(-1);
        DeleteKey(mem->LeftBound());
    }
    node_total_--;
}

void nvMultiTable::Delete(const Slice& key) {
    assert(false);
}
void nvMultiTable::InitPop(nvMemTable* mem) {
    vector<std::string> divider;
    vector<nvMemTable*> pack;
    assert(log_number_ > mem->Seq());
    mem->FillKey(divider, cache_policy_.standard_multimemtable_size_);
    assert(divider.size() > 0);
    assert(divider[0] == mem->LeftBound());
    for (size_t i = 0; i < divider.size(); ++i) {
        nvMemTable* m = nullptr;
        m = mem->Rebuild(divider[i], log_number_++);
        m->Ref();
        m->Paramenter(nvMemTable::ParameterType::CreatedTime) = this->bytes_;
        index_.Add(m->LeftBound(), m);
    }
    node_total_ += divider.size();
    node_total_ --;
    Inserts(mem);
    mem->Unref();
}
void nvMultiTable::Inserts(nvMemTable* oldmem) {
    Iterator* iter = oldmem->NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        Slice lkey = iter->key();
        Slice key(lkey.data(), lkey.size() - 8);
        Slice value(iter->value());
        nvMemTable* mem = WhereIs(key);
        mem->Add(0, value.size() == 0 ? kTypeDeletion : kTypeValue, key, value);
    }
}
void nvMultiTable::Pop(Version* current, const Slice& key) {
    //assert(HasRoomForNewMem());
    //while (!HasRoomForNewMem()) {
    //    Slice except = key;
    //    ForcePop(current, &except);
    //}
    assert(HasRoomForNewMem());
    CheckIndexValid();
    //nvMemTable* mem__;
    /*
    bool ok = index_.Get(key, &mem__);
    nvMemTable* mem = (mem__);
    assert(ok == true);
    */ //MemTable* imm = mem->Immutable(seq_++);
    IndexIterator *iter = index_.NewIterator();
    iter->Seek(key);
    nvMemTable* mem = iter->Data();
    assert(mem != nullptr);
    iter->Next();
    mem->RightBound() = (iter->Valid() ? iter->Data()->LeftBound() : "");
    delete iter;

    if (mem->Garbage() >= options_.TEST_min_nvm_memtable_garbage_rate) {
        //ic_.Pop(mem->StorageUsage(), mem->Garbage(), this->seq_ - mem->Seq());                                      // Info Collection !!!
        mem->GarbageCollection();
        return;
    }
    if (node_total_ == 1) { InitPop(mem); return; }
    vector<std::string> divider;
    vector<nvMemTable*> pack;
    assert(log_number_ > mem->Seq());
    ull lifetime = log_number_ - mem->Seq();
    size_t try_divid = 2;

    mem->FillKey(divider, try_divid);

    if (divider.size() == 0)
        DeleteKey(mem->LeftBound());
    else {
        assert(divider[0] == mem->LeftBound());
        for (size_t i = 0; i < divider.size(); ++i) {
            nvMemTable* m = nullptr;
            m = mem->Rebuild(divider[i], log_number_++);
            m->Ref();
            m->Paramenter(nvMemTable::ParameterType::CreatedTime) = this->bytes_;
            index_.Add(m->LeftBound(), m);
        }
    }
    //mem->CheckValid();
    //mem->SetImmutable(true);                                                   // Info Collection !!!
    level0_.PushBack(&mem);
    if (options_.TEST_nvskiplist_type == kTypeLinearHash)
        helper_->PushWorkToQueue(reinterpret_cast<nvFixedHashTable*>(mem), BackgroundHelper::WorkType::CreateImmutableMemTable);
    global_ic_.AddCompaction(-1);
    //level0_.push_back(mem);
    node_total_ += divider.size();
    node_total_ --;

    while (node_total_ >= cache_policy_.standard_multimemtable_size_) {
        ForcePop(current, &key);
    }
/*
    ic_.Pop(mem->StorageUsage(), mem->Garbage(), lifetime);                                         // Info Collection !!!

    std::vector<std::string> *snapshot_ = new std::vector<std::string>;
    auto i = index_.NewIterator();
    for (i->SeekToFirst(); i->Valid(); i->Next())
        snapshot_->push_back((i->Data())->LeftBound());
    delete i;
    ic_.SaveSnapshot(snapshot_);                                                                                   // Info Collection !!!
    level0_.push_back(mem);
    node_total_ += divider.size();
    node_total_ --;
*/
}

bool nvMultiTable::DeleteKey(const string& key) {
    if (key != "") {
        return index_.Delete(key);
    }

    IndexIterator* iter = index_.NewIterator();
    iter->SeekToFirst();
    assert( iter->key() == "" );
    iter->Next();
    assert(iter->Valid());
    nvMemTable* n = (iter->Data());
    string s = n->LeftBound();
    delete iter;

    n->LeftBound() = "";
    index_.Add("", n);
    return index_.Delete(s);

}

void nvMultiTable::CheckIndexValid() {
    return;
    IndexIterator * iter = index_.NewIterator();
    nvMemTable* prev = nullptr, *mem = nullptr;
    //printf("Checking Index:\n");
    //fflush(stdout);

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        mem = (iter->Data());
        nvMemTable* mem_test;
        bool ok = index_.Get(mem->LeftBound(), &mem_test);
        assert(ok && mem_test == mem);
        //printf("[%s] = [%s] ~ [%s]\n",
        //       mem->LeftBound().c_str(),
        //       mem->list_->Min().c_str(),
        //       mem->list_->Max().c_str());
        //fflush(stdout);
        if (prev) {
            string a = prev->LeftBound(), b = mem->LeftBound();
            assert(Slice(a).compare(b) < 0);
            a = prev->Max();
            b = mem->Min();
            assert(a == "" || b == "" || Slice(a).compare(b) < 0);
            //nvMemTable* t = index_.FuzzyFind(b);
            //printf("\t\t\t\t%s -> %s\n", b.c_str(), t->LeftBound().c_str());
        }
        prev = mem;
    }
    delete iter;
}

void nvMultiTable::ClearWriteBuffer() {
    //if (imm_cache_ != nullptr) {
    //    imm_cache_->Reset();
   // }
    for (int i = 0; i < writer_num_; ++i) {
        BackgroundWriter_LockFree* writer = writers_[i];
        writer->CheckClear();
    }
    if (cache_->Full()) {
    //    nvHashTable* tmp = imm_cache_;
    //    imm_cache_ = cache_;
    //    cache_ = tmp;
        cache_->Reset();
    }
}

IndexIterator* nvMultiTable::NewIndexIterator() {
    return index_.NewIterator();
}
//-------------------------------------------------

nvMultiTableIterator::~nvMultiTableIterator() {
    delete index_iter_;
    delete iter_;
}
  nvMultiTableIterator::nvMultiTableIterator(nvMultiTable* mt) :
        mt_(mt),
        index_iter_(mt_->NewIndexIterator()),
        iter_(nullptr) {
  }

  bool nvMultiTableIterator::Valid() const {
      return iter_ && iter_->Valid();
  }
  void nvMultiTableIterator::Seek(const Slice& k) {
      index_iter_->Seek(k);
      if (iter_) delete iter_;
      iter_ = (index_iter_->Data())->NewIterator();
      iter_->Seek(k);
  }
  void nvMultiTableIterator::SeekToFirst() {
      index_iter_->SeekToFirst();
      if (iter_) delete iter_;
      iter_ = (index_iter_->Data())->NewIterator();
      iter_->SeekToFirst();
  }
  void nvMultiTableIterator::SeekToLast() {
        index_iter_->SeekToLast();
        if (iter_) delete iter_;
        iter_ = (index_iter_->Data())->NewIterator();
        iter_->SeekToLast();
  }
  void nvMultiTableIterator::Next() {
      assert(Valid());
      iter_->Next();
      while (iter_ && !iter_->Valid()) {
        index_iter_->Next();
        delete iter_;
        if (index_iter_->Valid()) {
            iter_ = (index_iter_->Data())->NewIterator();
            iter_->SeekToFirst();
        } else {
            iter_ = nullptr;
        }
      }
      //if (!iter_->Valid())
  }
  void nvMultiTableIterator::Prev() {
        assert(Valid());
        iter_->Prev();
        while (iter_ && !iter_->Valid()) {
          index_iter_->Prev();
          delete iter_;
          if (index_iter_->Valid()) {
              iter_ = (index_iter_->Data())->NewIterator();
              iter_->SeekToLast();
          } else {
              iter_ = nullptr;
          }
        }
  }
  Slice nvMultiTableIterator::key() const {
      assert(Valid());
      return iter_->key();
  }
  Slice nvMultiTableIterator::value() const {
      assert(Valid());
      return iter_->value();
  }

  Status nvMultiTableIterator::status() const {
      return Status::OK();
  }

//static InfoCollector __thread ic_;
};
