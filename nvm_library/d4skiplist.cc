#ifndef D4SKIPLIST_CC
#define D4SKIPLIST_CC
#include "d4skiplist.h"

namespace leveldb {

L4MemTableAllocator::~L4MemTableAllocator() {
    //printf("Deleting L4MemTableAllocator...\n");fflush(stdout);
    mng_->Dispose(main_, total_size_);
    //printf("Deleting L4MemTableAllocator : Finished\n"); fflush(stdout);
}
L4SkipList::~L4SkipList() {
    //printf("Deleting L4Skiplist...\n");fflush(stdout);
    //printf("Deleting L4Skiplist : Finished\n"); fflush(stdout);
}
L4Cache::~L4Cache() {
    //printf("Deleting L4Cache...\n");fflush(stdout);
    arena_->Dispose(main_, size_);
    //printf("Deleting L4Cache : Finished\n"); fflush(stdout);
}


L4SkipList::L4SkipList(NVM_Manager* mng, uint32_t size, uint32_t garbage_cache_size)
    : mng_(mng), arena_(mng, size, garbage_cache_size),
      head_(nulloffset),
      max_height_(1)
{
    head_ = NewNode("", "", kTypeDeletion, kMaxHeight, nullptr);
    for (byte i = 0; i < kMaxHeight; ++i)
        SetNext(head_, i, nulloffset);
    SetHead(head_);
}
L4SkipList::L4SkipList(const L4SkipList& t)  // Garbage Collection.
     : mng_(t.mng_), arena_(mng_, t.arena_.Size(), t.arena_.cache_.Bytes()),
       head_(t.head_), max_height_(t.max_height_)
 {
     byte* buf = new byte[t.arena_.Size()];
     mng_->read(buf, t.arena_.Main(), t.arena_.Size());
     D1SkipList* d = new D1SkipList(buf, t.arena_.Size(), t.arena_.node_bound_, t.arena_.value_bound_, t.head_, t.max_height_);
     d->CheckValid();
     d->GarbageCollection();
     d->CheckValid();
     mng_->write(arena_.Main(), d->arena_.Main(), d->arena_.node_bound_);
     mng_->write(arena_.Main() + d->arena_.value_bound_,
                 d->arena_.Main() + d->arena_.value_bound_,
                 d->arena_.Size() - d->arena_.value_bound_);
     arena_.SetBound(d->arena_.node_bound_, d->arena_.value_bound_);
     CheckValid();
     delete d;
 }

int D4MemTable::RandomHeight() {
    // Increase height with probability 1 in kBranching
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
      height++;
    }
    //assert(height > 0);
    //assert(height <= kMaxHeight);
    return height;
}
std::string D4MemTable::MemName(const Slice& dbname, ull seq) {
    return dbname.ToString() + std::to_string(seq) + ".nvskiplist";
}
std::string D4MemTable::HashName(const Slice& dbname, ull seq) {
    return dbname.ToString() + std::to_string(seq) + ".nvhash";
}
D4MemTable::D4MemTable(NVM_Manager* mng, const CachePolicy& cp, const Slice& dbname, ull seq) :
    mng_(mng), cp_(cp),
    cache_(mng, cp_.hash_range_ + 1),
    //cache_enabled_(cp.node_cache_size_ > 32768),
    table_(mng_,
           static_cast<ul>(cp.nvskiplist_size_),
           static_cast<ul>(cp.garbage_cache_size_)),
    rnd_(0xdeadbeef),
    dbname_(dbname.ToString()), seq_(seq), pre_write_(0),
    written_size_(0), created_time_(0),
    refs__(0), lock_(), immutable_(false) {
    mng_->bind_name(MemName(dbname_, seq_), table_.mem());
}

D4MemTable::~D4MemTable() {
    //printf("Deleting D4MemTable...\n"); fflush(stdout);
    //delete cache_;
    //delete table_;
    //printf("Deleting D4MemTable : Finished\n"); fflush(stdout);

}
void D4MemTable::DeleteName() { mng_->delete_name(MemName(dbname_, seq_)); }
nvOffset D4MemTable::Seek(const Slice& key) {
    nvOffset y = table_.Head();
    byte height = table_.max_height_-1;
    if (table_.Seek(key, y, height, nullptr, nullptr)) {
        return y;
    }
    return nulloffset;
}
bool D4MemTable::HashLocate(const Slice& key, nvOffset hash, nvOffset& node, nvOffset& next) {
    node = cache_.Read(hash);
    next = nulloffset;
    if (node == 0) {
        node = nulloffset;
        return false;
    }
    int cmp = table_.GetKey_(node).compare(key);
    if (cmp > 0) {
        next = node;
        node = nulloffset;
        return false;
    }
    next = table_.GetReserved_(node);
    if (cmp == 0) return true;
    while (next != nulloffset && (cmp = table_.GetKey_(next).compare(key)) < 0) {
        node = next;
        next = table_.GetReserved_(node);
    }
    if (cmp == 0) {
        node = next;
        next = table_.GetReserved_(node);
        return true;
    }
    return false;
}
void D4MemTable::Insert(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value,
                        nvOffset hash, nvOffset prev, nvOffset next) {
    if (pre_write_ > 0)
        pre_write_ -= MaxSizeOf(key.size(), value.size());
    written_size_ += MaxSizeOf(key.size(), value.size());
    nvOffset y = table_.Add(key, value, type, RandomHeight());
    assert (y != nulloffset);
    table_.SetReserved_(y, next);
    if (prev == nulloffset)
        cache_.Write(hash, y);
    else
        table_.SetReserved_(prev, y);
}
void D4MemTable::Update(nvOffset node, SequenceNumber seq, ValueType type, const Slice& value) {
    table_.Update(node, value, type);
}
nvOffset D4MemTable::FindNode(const Slice& key) {
    nvOffset node, next;
    nvOffset hash = Hash(key);
    if (HashLocate(key, hash, node, next))
        return node;
    else return nulloffset;
}
void D4MemTable::Add(SequenceNumber seq, ValueType type,
                 const Slice& key,
                 const Slice& value) {
    //byte level = 0;
    if (pre_write_ > 0)
        pre_write_ -= MaxSizeOf(key.size(), value.size());
    written_size_ += MaxSizeOf(key.size(), value.size());

    nvOffset node, next;
    nvOffset hash = Hash(key);
    bool exist = HashLocate(key, hash, node, next);
    if (exist) {
        Update(node, seq, type, value);
        return;
    }
    Insert(seq, type, key, value, hash, node, next);
}

bool D4MemTable::Get(const Slice& key, std::string* value, Status* s) {
    nvOffset x = cache_.Read(Hash(key));
    if (x == 0) return false;
    return table_.Get_(x, key, value, s);
}
bool D4MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
    return Get(key.user_key(), value, s);
}

void D4MemTable::Ref() {refs__++;}
void D4MemTable::Unref() {
    --refs__;
    assert(refs__ >= 0);
    if (refs__ <= 0) {
      delete this;
    }
}
void D4MemTable::GarbageCollection() {
    assert(false);
    /*
    CheckValid();
    L4SkipList *t = new L4SkipList(*table_);
    t->CheckValid();
    mng_->bind_name(MemName(dbname_, seq_), t->mem());
    L4SkipList *old = table_;
    table_ = t;
    delete old;
    */
}

Iterator* D4MemTable::NewIterator() { return new D4MemTableIterator(this, (1ULL<<56)-1); }
Iterator* D4MemTable::NewOfficialIterator(ull seq) { return new D4MemTableIterator(this, seq); }

void D4MemTable::GetMid(std::string* key) {
    nvOffset y = table_.Middle();
    assert(y != nulloffset);
    table_.GetKey(y, key);
}
void D4MemTable::GetMin(std::string* key) {
    nvOffset y = table_.GetNext(table_.Head(), 0);
    if (y == nulloffset) {
        *key = "";
        return;
    }
    //assert(y != nulloffset);
    table_.GetKey(y, key);
}
void D4MemTable::GetMax(std::string* key) {
    nvOffset y = table_.Tail();
    if (y == nulloffset || y == table_.Head()) {
        *key = "";
        return;
    }
    table_.GetKey(y, key);
}

double D4MemTable::Garbage() const {
    return 1. * table_.arena_.Garbage() / table_.arena_.Size() ;
}
ull D4MemTable::StorageUsage() const {
    return pre_write_ + table_.StorageUsage();
}
nvAddr D4MemTable::Location() const {
    return table_.mem();
}
bool D4MemTable::HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full = false) {
    ull size = table_.StorageUsage() + key.size() + value.size() + 1024;
    return size < cp_.nvskiplist_size_ && !(nearly_full && size >= cp_.nearly_full_size_);
}
bool D4MemTable::PreWrite(const Slice& key, const Slice& value, bool nearly_full = false) {
    ull total = MaxSizeOf(key.size(), value.size());
    ull bottom = (nearly_full ? cp_.nearly_full_size_ : cp_.nvskiplist_size_);
    if (total + StorageUsage() + 4096 < bottom) {
        pre_write_ += total;
        return true;
    }
    return false;
}

size_t D4MemTable::CountOfLevel(byte level) {
    size_t ans = 0;
    for (nvOffset y = table_.GetNext(table_.Head(), level); y != nulloffset; y = table_.GetNext(y, level))
        ans++;
    return ans;
}
void D4MemTable::FillKey(std::vector<std::string> &divider, size_t size) {
    divider.clear();
    if (size == 0) return;
    divider.push_back(LeftBound());
    if (size == 1) return;
    size_t total;
    byte level;
    for (level = table_.max_height_-1; level > 0; level--) {
        total = CountOfLevel(level);
        if (total < size * 10)
            continue;
        break;
    }
    if (level == 0) {
        size = 2;
        total = CountOfLevel(0);
    }
    //size * (M - 1) < total; M = total / size
    size_t M = total / size;
    size_t counter = 0;
    for (nvOffset x = table_.GetNext(table_.head_,level); x != nulloffset; x = table_.GetNext(x, level))
        if (++counter % M == 0)
            divider.push_back(table_.GetKey(x).ToString());

    //assert(divider.size() <= size + 1);
    while (divider.size() > size) {
        divider.pop_back();
    }
    return;
}

void D4MemTable::Print() {
    assert(false);
}

void D4MemTable::CheckValid() {
    std::string min_key, max_key;
    GetMin(&min_key);
    GetMax(&max_key);

    return;
    table_.CheckValid();
}

///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////
///
///
///
///
///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////

int D5MemTable::RandomHeight() {
    // Increase height with probability 1 in kBranching
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
      height++;
    }
    //assert(height > 0);
    //assert(height <= kMaxHeight);
    return height;
}
std::string D5MemTable::MemName(const Slice& dbname, ull seq) {
    return dbname.ToString() + std::to_string(seq) + ".nvskiplist";
}
std::string D5MemTable::HashName(const Slice& dbname, ull seq) {
    return dbname.ToString() + std::to_string(seq) + ".nvhash";
}
D5MemTable::D5MemTable(NVM_Manager* mng, const CachePolicy& cp, const Slice& dbname, ull seq) :
    mng_(mng), cp_(cp),
    //cache_(mng, cp_.hash_range_),
    //cache_enabled_(cp.node_cache_size_ > 32768),
    table_(mng_,
           static_cast<ul>(cp.nvskiplist_size_),
           static_cast<ul>(cp.garbage_cache_size_)),
    rnd_(0xdeadbeef),
    dbname_(dbname.ToString()), seq_(seq), pre_write_(0),
    written_size_(0), created_time_(0),
    refs__(0), lock_(), immutable_(false) {
    mng_->bind_name(MemName(dbname_, seq_), table_.mem());
}

D5MemTable::~D5MemTable() {
    //printf("Deleting D4MemTable...\n"); fflush(stdout);
    //delete cache_;
    //delete table_;
    //printf("Deleting D4MemTable : Finished\n"); fflush(stdout);

}
void D5MemTable::DeleteName() { mng_->delete_name(MemName(dbname_, seq_)); }
nvOffset D5MemTable::Seek(const Slice& key) {
    nvOffset y = table_.Head();
    byte height = table_.max_height_-1;
    if (table_.Seek(key, y, height, nullptr, nullptr)) {
        return y;
    }
    return nulloffset;
}
void D5MemTable::Add(SequenceNumber seq, ValueType type,
                 const Slice& key,
                 const Slice& value) {
    //byte level = 0;
    if (pre_write_ > 0)
        pre_write_ -= MaxSizeOf(key.size(), value.size());
    written_size_ += MaxSizeOf(key.size(), value.size());
    nvOffset y = table_.Add(key, value, type, RandomHeight());
}

bool D5MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
    return table_.Get(key, value, s);
}

void D5MemTable::Ref() {refs__++;}
void D5MemTable::Unref() {
    --refs__;
    assert(refs__ >= 0);
    if (refs__ <= 0) {
      delete this;
    }
}
void D5MemTable::GarbageCollection() { assert(false); }

Iterator* D5MemTable::NewIterator() { return new D5MemTableIterator(this, (1ULL<<24)-1); }
Iterator* D5MemTable::NewOfficialIterator(ull seq) { return new D5MemTableIterator(this, seq); }

void D5MemTable::GetMid(std::string* key) {
    nvOffset y = table_.Middle();
    assert(y != nulloffset);
    table_.GetKey(y, key);
}
void D5MemTable::GetMin(std::string* key) {
    nvOffset y = table_.GetNext(table_.Head(), 0);
    if (y == nulloffset) {
        *key = "";
        return;
    }
    //assert(y != nulloffset);
    table_.GetKey(y, key);
}
void D5MemTable::GetMax(std::string* key) {
    nvOffset y = table_.Tail();
    if (y == nulloffset || y == table_.Head()) {
        *key = "";
        return;
    }
    table_.GetKey(y, key);
}

double D5MemTable::Garbage() const {
    return 1. * table_.arena_.Garbage() / table_.arena_.Size() ;
}
ull D5MemTable::StorageUsage() const {
    return pre_write_ + table_.StorageUsage();
}
nvAddr D5MemTable::Location() const {
    return table_.mem();
}
bool D5MemTable::HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full = false) {
    ull size = table_.StorageUsage() + key.size() + value.size() + 1024;
    return size < cp_.nvskiplist_size_ && !(nearly_full && size >= cp_.nearly_full_size_);
}
bool D5MemTable::PreWrite(const Slice& key, const Slice& value, bool nearly_full = false) {
    ull total = MaxSizeOf(key.size(), value.size());
    ull bottom = (nearly_full ? cp_.nearly_full_size_ : cp_.nvskiplist_size_);
    if (total + StorageUsage() + 4096 < bottom) {
        pre_write_ += total;
        return true;
    }
    return false;
}

size_t D5MemTable::CountOfLevel(byte level) {
    size_t ans = 0;
    for (nvOffset y = table_.GetNext(table_.Head(), level); y != nulloffset; y = table_.GetNext(y, level))
        ans++;
    return ans;
}
void D5MemTable::FillKey(std::vector<std::string> &divider, size_t size) {
    divider.clear();
    if (size == 0) return;
    divider.push_back(LeftBound());
    if (size == 1) return;
    size_t total;
    byte level;
    for (level = table_.max_height_-1; level > 0; level--) {
        total = CountOfLevel(level);
        if (total < size * 10)
            continue;
        break;
    }
    if (level == 0) {
        size = 2;
        total = CountOfLevel(0);
    }
    //size * (M - 1) < total; M = total / size
    size_t M = total / size;
    size_t counter = 0;
    if (M == 0) {   // MemTable is too small to be divided.
        divider.clear();
        return;
    }
    for (nvOffset x = table_.GetNext(table_.head_,level); x != nulloffset; x = table_.GetNext(x, level))
        if (++counter % M == 0)
            divider.push_back(table_.GetKey(x).ToString());

    //assert(divider.size() <= size + 1);
    while (divider.size() > size) {
        divider.pop_back();
    }
    return;
}

void D5MemTable::Print() {
    assert(false);
}

void D5MemTable::CheckValid() {
    std::string min_key, max_key;
    GetMin(&min_key);
    GetMax(&max_key);

    return;
    table_.CheckValid();
}
}

#endif // D2SKIPLIST_CC
