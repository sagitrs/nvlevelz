#ifndef D2SKIPLIST_CC
#define D2SKIPLIST_CC
#include "d2skiplist.h"

namespace leveldb {

int D2MemTable::RandomHeight() {
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
bool D2MemTable::CacheSave(byte height) {
    if (height > st_height_)
        return 1;
    if (height < st_height_)
        return 0;
    if (st_rate_ == 0)
        return 0;
    return rnd_.Next() % st_height_ == 0;
}

std::string D2MemTable::MemName(const Slice& dbname, ull seq) {
    return dbname.ToString() + std::to_string(seq) + ".nvskiplist";
}
D2MemTable::D2MemTable(NVM_Manager* mng, const CachePolicy& cp, const Slice& dbname, ull seq) :
    mng_(mng), cp_(cp),
    cache_(cp.node_cache_size_), cache_enabled_(cp.node_cache_size_ > 32768),
    table_(new LowerD1Skiplist(mng_, cp.nvskiplist_size_, cp.garbage_cache_size_)),
    rnd_(0xdeadbeef), st_height_(cp.height_), st_rate_(cp.p_),
    dbname_(dbname.ToString()), seq_(seq), pre_write_(0), written_size_(0), created_time_(0), refs__(0), lock_(), immutable_(false) {
    mng_->bind_name(MemName(dbname_, seq_), table_->mem());
}

D2MemTable::~D2MemTable() { delete table_; }
void D2MemTable::DeleteName() { mng_->delete_name(MemName(dbname_, seq_)); }
nvOffset D2MemTable::Seek(const Slice& key) {
    nvOffset x = cache_.Head(), y = table_->Head();
    cache_.Seek(key, x, cache_.max_height_-1, st_height_-1, nullptr);
    if (x != cache_.head_)
        y = cache_.GetValue(x);
    byte height = (st_height_ > table_->max_height_ ? table_->max_height_-1 : st_height_-1);
    if (table_->Seek(key, y, height, nullptr, nullptr)) {
        return y;
    }
    return nulloffset;
}
void D2MemTable::Add(SequenceNumber seq, ValueType type,
                 const Slice& key,
                 const Slice& value) {
    //byte level = 0;
    if (pre_write_ > 0)
        pre_write_ -= MaxSizeOf(key.size(), value.size());
    written_size_ += MaxSizeOf(key.size(), value.size());

    nvOffset x = cache_.Head(), y = table_->Head();
    byte l = table_->max_height_ - 1;
    byte height = RandomHeight();
    nvOffset dprev[kMaxHeight], prev[kMaxHeight], next[kMaxHeight];
    if (cache_enabled_) {
        byte min_level = 0;
        if (st_height_ > 1) min_level = st_height_ - 1;
        cache_.Seek(key, x, cache_.max_height_-1, min_level, dprev);
        if (x != cache_.Head())
            y = cache_.GetValue(x);
        if (st_height_ < table_->max_height_)
            l = (st_height_ == 0 ? 0 : st_height_ - 1);
        //byte l = (st_height_ > table_->max_height_ ? table_->max_height_-1 : (st_height_ == 0 ? 0 : st_height_-1));
    }
    if (table_->Seek(key, y, l, prev, next)) {
        table_->Update(y, value, type);
        return;
    }
    for (byte h = st_height_; h < height; ++h) {
        nvOffset x = (h >= cache_.max_height_ ? cache_.Head() : dprev[h]);
        prev[h] = (x == cache_.Head() ? table_->Head() : cache_.GetValue(x));
        x = cache_.GetNext(x, h);
        next[h] = (x == nulloffset ? nulloffset : cache_.GetValue(x));
    }
    y = table_->Insert(key, value, type, height, prev, next);
    if (CacheSave(height) && cache_.HasRoomForWrite(key, height))
        cache_.Add(key, y, height, dprev);
}

bool D2MemTable::Get(const Slice& key, std::string* value, Status* s) {
    nvOffset x = cache_.Head(), y = table_->Head();
    byte height = table_->max_height_ - 1;
    if (cache_enabled_) {
        byte min_level = 0;
        if (st_height_ > 1) min_level = st_height_ - 1;
        cache_.Seek(key, x, cache_.max_height_-1, min_level, nullptr);
        if (x != cache_.head_)
            y = cache_.GetValue(x);
        if (st_height_ < table_->max_height_) {
            height = (st_height_ == 0 ? 0 : st_height_ - 1);
        }
        //byte height = (st_height_ > table_->max_height_ ? table_->max_height_-1 : (st_height_ == 0 ? 0 : st_height_-1));

    }
    if (table_->Seek(key, y, height, nullptr, nullptr)) {
        if (!table_->GetValue(y, value))
            *s = Status::NotFound(Slice());
        return true;
    }
    return false;
}
bool D2MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
    return Get(key.user_key(), value, s);
}

void D2MemTable::Ref() {refs__++;}
void D2MemTable::Unref() {
    assert(refs__ > 0);
    if (--refs__ == 0)
        delete this;
}
void D2MemTable::GarbageCollection() {
    CheckValid();
    LowerD1Skiplist *t = new LowerD1Skiplist(*table_);
    t->CheckValid();
    mng_->bind_name(MemName(dbname_, seq_), t->mem());
    LowerD1Skiplist *old = table_;
    table_ = t;
    delete old;
}

Iterator* D2MemTable::NewIterator() { return new D2MemTableIterator(this, (1ULL<<56)-1); }
Iterator* D2MemTable::NewOfficialIterator(ull seq) { return new D2MemTableIterator(this, seq); }

void D2MemTable::GetMid(std::string* key) {
    nvOffset y = table_->Middle();
    assert(y != nulloffset);
    table_->GetKey(y, key);
}
void D2MemTable::GetMin(std::string* key) {
    nvOffset y = table_->GetNext(table_->Head(), 0);
    if (y == nulloffset) {
        *key = "";
        return;
    }
    //assert(y != nulloffset);
    table_->GetKey(y, key);
}
void D2MemTable::GetMax(std::string* key) {
    nvOffset y = table_->Tail();
    if (y == nulloffset || y == table_->Head()) {
        *key = "";
        return;
    }
    table_->GetKey(y, key);
}

double D2MemTable::Garbage() const {
    return 1. * table_->arena_.Garbage() / table_->arena_.Size() ;
}
ull D2MemTable::StorageUsage() const {
    return pre_write_ + table_->StorageUsage();
}
nvAddr D2MemTable::Location() const {
    return table_->mem();
}
bool D2MemTable::HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full = false) {
    ull size = table_->StorageUsage() + key.size() + value.size() + 1024;
    return size < cp_.nvskiplist_size_ && cache_.HasRoomForWrite(key, kMaxHeight)
        && !(nearly_full && size >= cp_.nearly_full_size_);
}
bool D2MemTable::PreWrite(const Slice& key, const Slice& value, bool nearly_full = false) {
    ull total = MaxSizeOf(key.size(), value.size());
    ull bottom = (nearly_full ? cp_.nearly_full_size_ : cp_.nvskiplist_size_);
    if (total + StorageUsage() + 4096 < bottom && cache_.HasRoomForWrite(key, kMaxHeight)) {
        pre_write_ += total;
        return true;
    }
    return false;
}

size_t D2MemTable::CountOfLevel(byte level) {
    size_t ans = 0;
    if (level > st_height_) {
        for (nvOffset x = cache_.GetNext(cache_.head_,level); x != nulloffset; x = cache_.GetNext(x, level))
            ans++;
    } else {
        for (nvOffset y = table_->GetNext(table_->Head(), level); y != nulloffset; y = table_->GetNext(y, level))
            ans++;
    }
    return ans;
}
void D2MemTable::FillKey(std::vector<std::string> &divider, size_t size) {
    divider.clear();
    if (size == 0) return;
    divider.push_back(LeftBound());
    if (size == 1) return;
    size_t total;
    byte level;
    for (level = table_->max_height_-1; level > 0; level--) {
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
    if (level > st_height_) {
        size_t counter = 0;
        for (nvOffset x = cache_.GetNext(cache_.head_,level); x != nulloffset; x = cache_.GetNext(x, level))
            if (++counter % M == 0)
                divider.push_back(cache_.GetKey(x).ToString());
    } else {
        size_t counter = 0;
        for (nvOffset x = table_->GetNext(table_->head_,level); x != nulloffset; x = table_->GetNext(x, level))
            if (++counter % M == 0)
                divider.push_back(table_->GetKey(x).ToString());
    }
    //assert(divider.size() <= size + 1);
    while (divider.size() > size) {
        divider.pop_back();
    }
    return;
}

void D2MemTable::Print() {
    assert(false);
}

void D2MemTable::CheckValid() {
    std::string min_key, max_key;
    GetMin(&min_key);
    GetMax(&max_key);

    return;
    cache_.CheckValid();
    table_->CheckValid();
}

}

#endif // D2SKIPLIST_CC
