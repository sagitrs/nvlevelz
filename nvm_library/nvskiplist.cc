#ifndef NVSKIPLIST_CC
#define NVSKIPLIST_CC

#include "nvskiplist.h"
namespace leveldb {

bool nvSkiplist::CacheSave(byte height) {
    if (cache_storage_usage_ >= cache_policy_.node_cache_size_) return 0;
    if (height > cache_policy_.height_) return 1;
    if (height < cache_policy_.height_) return 0;
    if (cache_policy_.p_ == 0) return 0;
    return rnd_.Next() % cache_policy_.p_ == 0;
}

string nvSkiplist::GetKey(NodePtr node) {
    const nvAddr keyAddr = GetKeyAddr(node);
    const nvAddr keySize = GetKeySize(node);
    assert(keySize > 0);
    byte buf[keySize+1];
    mng_->read(buf, keyAddr, keySize);
    buf[keySize] = 0;
    return string(reinterpret_cast<char*>(buf),keySize);
}
const char* nvSkiplist::GetKeyBoost(NodePtr node) {
    const nvAddr keyAddr = GetKeyAddr(node);
    const nvAddr keySize = GetKeySize(node);
    assert(keySize > 0);
    char* buf = arena_->Allocate(keySize+1);
    mng_->read(reinterpret_cast<byte*>(buf), keyAddr, keySize);
    buf[keySize] = 0;
    return buf;
}

const char* nvSkiplist::GetPartialKeyBoost(NodePtr node) {
    const nvAddr keyAddr = GetKeyAddr(node) + common_size_;
    const nvAddr keySize = GetKeySize(node) - common_size_;
    //assert(keySize > 0);
    char* buf = arena_->Allocate(keySize+1);
    mng_->read(reinterpret_cast<byte*>(buf), keyAddr, keySize);
    buf[keySize] = 0;
    return buf;
}

string nvSkiplist::GetValue(NodePtr node) {
    const nvAddr valueAddr = GetValueAddr(node);
    if (valueAddr == nvnullptr) return string();
    const nvAddr valueSize = mng_->read_ull(valueAddr);
    assert(valueSize > 0);
    byte buf[valueSize+1];
    mng_->read(buf, valueAddr+8, valueSize);
    buf[valueSize] = 0;
    return string(reinterpret_cast<char*>(buf), valueSize);
}

const char* nvSkiplist::GetValueBoost(NodePtr node) {
    const nvAddr valueAddr = GetValueAddr(node);
    if (valueAddr == nvnullptr) return "";
    const nvAddr valueSize = mng_->read_ull(valueAddr);
    assert(valueSize > 0);

    char* buf = arena_->Allocate(valueSize+1);
    mng_->read(reinterpret_cast<byte*>(buf), valueAddr+8, valueSize);
    buf[valueSize] = 0;
    return buf;

}


nvSkiplist::NodePtr nvSkiplist::NewNode(const Slice& key, const Slice& value, byte height){
    const ull keysize = key.size();
    const ull valueSize = value.size();
    const ull nodeSize = NextOffset + 8 * height + 1 * keysize;
    //storage_usage_ += nodeSize + valueSize;

    NodePtr nodeAddr = mng_->Allocate(nodeSize);
    storage_usage_ += nodeSize;
    NodePtr valueAddr = nvnullptr;

    byte buf[NextOffset];
    if (key.size()) {
        mng_->write(nodeAddr + NextOffset + height * 8,
                    reinterpret_cast<const byte*>(key.data()), key.size());
    }
    if (value.size()) {
        valueAddr = mng_->Allocate(valueSize + 8);
        storage_usage_ += valueSize + 8;
        mng_->write_ull(valueAddr, valueSize);
        mng_->write(valueAddr + 8, reinterpret_cast<const byte*>(value.data()), valueSize);
    }
    memcpy(buf + NodeHeightOffset, &height, 1);
    memcpy(buf + KeyLengthOffset, reinterpret_cast<const byte*>(&keysize),8);
    memcpy(buf + ValueAddressOffset, reinterpret_cast<byte*>(&valueAddr), 8);
    mng_->write(nodeAddr, buf, NextOffset);


    //delete[] buf;
    return nodeAddr;
}

void nvSkiplist::DisposeNode(NodePtr node) {
    if (node == nvnullptr) return;
    byte buf0[NextOffset];
    mng_->read(buf0, node, NextOffset);

    byte height = *(reinterpret_cast<byte*>(buf0 + NodeHeightOffset));
    ull keySize = *(reinterpret_cast<ull*>(buf0 + KeyLengthOffset));
    nvAddr valueAddr = *(reinterpret_cast<ull*>(buf0 + ValueAddressOffset));

    if (valueAddr != nvnullptr) {
        ull valueSize = mng_->read_ull(valueAddr);
        mng_->Dispose(valueAddr, valueSize + 8);
    }
    mng_->Dispose(node, NextOffset + 8 * height + 1 * keySize);
}

void nvSkiplist::DirectUpdate(NodePtr nodeAddr, DataPtr valueAddr, ull valueSize,
                              const Slice& value, uint64_t seq, bool isDeletion){
    DataPtr newValueAddr = nvnullptr;
    if (!isDeletion) {
        assert(value.size() > 0);
        newValueAddr = mng_->Allocate(value.size() + 8);
        storage_usage_ += value.size();
        mng_->write_ull(newValueAddr, value.size());
        mng_->write(newValueAddr + 8, reinterpret_cast<const byte*>(value.data()), value.size());
    }
    mng_->write_addr(nodeAddr + ValueAddressOffset, newValueAddr);
    if (valueAddr != nvnullptr) {
        assert(valueSize > 0);
        mng_->Dispose(valueAddr, valueSize + 8);
    }
    storage_usage_ -= valueSize;
}

nvSkiplist::nvSkiplist(NVM_Manager* mng, const CachePolicy& cache_policy)
    : storage_usage_(0),
      mng_(mng),
      //filter_(new MemBloomFilter(10, (1 << 17))),
      //cache_mng_(cache_mng),
      cache_(),
      cache_policy_(cache_policy),
      cache_storage_usage_(0),
      rnd_(0xdeadbeef),
      max_height_(1),
      head_(NewNode(Slice(), Slice(), kMaxHeight)),
      common_size_(0),
      empty_(true),arena_(new Arena) {
  for (int i = 0; i < kMaxHeight; i++) {
    SetNext(head_, i, nvnullptr);
  }
}
/*
nvSkiplist::nvSkiplist(NVM_Manager* mng, NodePtr head, size_t commonSize)
    : storage_usage_(0),
      mng_(mng),
      cache_(),
      cache_policy_(cache_policy),
      cache_storage_usage_(0),
      rnd_(0xdeadbeef),
      max_height_(1),
      head_(head),
      common_size_(commonSize),
      empty_(true),arena_(new Arena) {
  for (int i = 0; i < kMaxHeight; i++) {
    if (GetNext(head_,i) != BLANK_NODE)
        empty_ = false;
    else {
        max_height_ = i;
        break;
    }
  }
}*/

nvSkiplist::~nvSkiplist() {
    NodePtr x = head_;
    if (x == BLANK_NODE) return;
    NodePtr x_next;
    while (x != BLANK_NODE){
        x_next = GetNext(x,0);
        DisposeNode(x);
        x = x_next;
    }
    //delete filter_;
    ClearCache();
    //cache_.DisposeAll();
    delete arena_;
}

nvSkiplist::NodePtr nvSkiplist::Insert_(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, NodePtr* prev){
    int height = RandomHeight();

    bool in_cache_ = CacheSave(height);
    Node* node = in_cache_ ? new Node() : nullptr;

    if (in_cache_) {
        node->SetHeight(height);
        node->SetKey(key);
        cache_storage_usage_ += 26 + 8 * height + key.size();
    }
    if (height > GetMaxHeight()) {
        for (int i = GetMaxHeight(); i < height; ++i)
            prev[i] = head_;
        max_height_ = height;
    }

    NodePtr x = NewNode(key, value, height);

    std::unordered_map<NodePtr, Node*>::iterator p;
    NodePtr p_next;
    for (int i = 0; i < height; i++) {
        if (i >= cache_policy_.height_ && ((p = cache_.find(prev[i])) != cache_.end())) {
            p_next = p->second->GetNext(i);
            p->second->SetNext(i, x);
        } else
            p_next = GetNext(prev[i], i);
        if (in_cache_) node->SetNext(i, p_next);
        SetNext(x, i, p_next);
        SetNext(prev[i], i, x);
    }

    empty_ = false;
    //if (filter_)
    //filter_->InsertKey(key);
    if (in_cache_)
        cache_[x] = node;
    return x;
}

void nvSkiplist::Insert(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
    if (arena_->MemoryUsage() >= 1 * MB) {
        delete arena_;
        arena_ = new Arena;
    }
    NodePtr prev[kMaxHeight];
    NodePtr x = FindGreaterOrEqual(key,prev);
    //assert(x == nvBLANK_NODE || !Equal(key, GetKey(x)));
    // you just can't insert the same key.
    if (x != BLANK_NODE && Equal(key, GetKey(x)))
        Update(x, value, seq, isDeletion);
    else
        Insert_(key, value, seq, isDeletion, prev);
}

void nvSkiplist::InsertPartial(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
    if (arena_->MemoryUsage() >= 1 * MB) {
        delete arena_;
        arena_ = new Arena;
    }
    /*
    if (cache_) {
        L2MemTableInfo info;
        if (cache_->Get(key,info)) {
            DirectUpdate(info.nodeAddr_,info.valueAddr_,info.valueSize_,
                         value,seq,isDeletion);
            return;
        }
    }*/
    NodePtr prev[kMaxHeight];
    NodePtr x = FindGreaterOrEqualPartial(key,prev);
    //assert(x == nvBLANK_NODE || !Equal(key, GetKey(x)));
    // you just can't insert the same key.
    if (x != BLANK_NODE && EqualPartial(key, GetKey(x)))
        Update(x, value, seq, isDeletion);
    else
        Insert_(key, value, seq, isDeletion, prev);
}

bool nvSkiplist::Contains(const Slice& key) {
  //if (!filter_->KeyMayMatch(key)) return false;
  NodePtr x = FindGreaterOrEqual(key, nullptr);
  return x != BLANK_NODE && Equal(key, GetKey(x));
}

bool nvSkiplist::TryGetPartial(const Slice& key, std::string* &value) {
    //if (!filter_->KeyMayMatch(key)) {
    //    value = nullptr;
    //    return false;
    //}
    NodePtr x = FindGreaterOrEqualPartial(key, nullptr);
    if (x == BLANK_NODE || !EqualPartial(key, GetKey(x))) {
        value = nullptr;
        return false;
    }
    DataPtr v = GetValueAddr(x);
    if (v == nvnullptr)
        value = nullptr;
    else
        value = new std::string(GetValue(x));
    return true;
}


bool nvSkiplist::Get(const Slice& key, std::string* value, Status* s) {
    if (arena_->MemoryUsage() >= 1 * MB) {
        delete arena_;
        arena_ = new Arena;
    }
    NodePtr x = FindGreaterOrEqual(key, nullptr);
    if (x == BLANK_NODE || !Equal(key, GetKey(x))) {
        //value = nullptr;
        return false;
    }
    DataPtr v = GetValueAddr(x);
    if (v == nvnullptr) {
        *s = Status::NotFound(Slice());
        return true;
    } else {
        const char* v = GetValueBoost(x);
        value->assign(v, strlen(v));
        return true;
        //value = new std::string(GetValue(x));
    }
    assert(false);
}

bool nvSkiplist::GetPartial(const Slice& key, std::string* value, Status* s) {
    if (arena_->MemoryUsage() >= 1 * MB) {
        delete arena_;
        arena_ = new Arena;
    }
    NodePtr x = FindGreaterOrEqualPartial(key, nullptr);
    if (x == BLANK_NODE || !EqualPartial(key, GetKey(x))) {
        //value = nullptr;
        return false;
    }
    DataPtr v = GetValueAddr(x);
    if (v == nvnullptr) {
        *s = Status::NotFound(Slice());
        return true;
    } else {
        const char* v = GetValueBoost(x);
        value->assign(v, strlen(v));
        return true;
        //value = new std::string(GetValue(x));
    }
    assert(false);
}

bool nvSkiplist::TryGet(const Slice& key, std::string* &value) {
    //if (!filter_->KeyMayMatch(key)) {
    //    value = nullptr;
    //    return false;
    //}
    NodePtr x = FindGreaterOrEqual(key, nullptr);
    if (x == BLANK_NODE || !Equal(key, GetKey(x))) {
        value = nullptr;
        return false;
    }
    DataPtr v = GetValueAddr(x);
    if (v == nvnullptr)
        value = nullptr;
    else {
        const char* v = GetValueBoost(x);
        value->assign(v, strlen(v));
        //value = new std::string(GetValue(x));
    }
    return true;
}


ll nvSkiplist::Inserts(DramKV_Skiplist* a){
    ll old_storage = storage_usage_;
    NodePtr prev[kMaxHeight];
    for (byte i = 0; i < kMaxHeight; ++i)
        prev[i] = BLANK_NODE;
    Inserts_(a, head_, max_height_+1, prev);
    return storage_usage_ - old_storage;
}

void nvSkiplist::Inserts_(DramKV_Skiplist* a, NodePtr x, byte height, NodePtr* prev){
    if (height == 0) {
        DramKV_Skiplist::Iterator* iter = a->NewIterator();
        assert(prev[0] == nvnullptr || GetNext(prev[0],0) == x);
        for (iter->SeekToFirst();iter->Valid(); iter->Next()){
            StaticSlice key(iter->key());
            StaticSlice* vp = iter->valuePtr();
            StaticSlice value(vp ? *vp : StaticSlice());
            uint64_t seq(iter->seq());
            bool isDeletion(iter->isDeletion());
            if (Equal(key.ToSlice(),GetKey(x))){
                Update(x,value.ToSlice(),seq,isDeletion);
            } else {
                const byte H = GetHeight(x);
                for (byte h = 0; h < H; ++h)
                    prev[h] = x;
                NodePtr p = Insert_(key.ToSlice(), value.ToSlice(), seq, isDeletion, prev);
                assert(GetNext(x,0) == p);
                x = p;
            }
        }
        //prev[0] = p;
        return;
    }
    NodePtr end = GetNext(x,height);
    height --;
    NodePtr l, r;
    for (l = x; l != end; l = r){
        r = GetNext(l,height);
        DramKV_Skiplist *rest;
        if (r == BLANK_NODE)
            rest = NULL;
        else
            rest = a->Cut(GetKey(r));
        if (!a->isEmpty()){
            if (l != x)
                FindTails(prev,x,l);
            Inserts_(a,l,height,prev);
        }
        a = rest;
        if (a == NULL) break;
    }
    assert( a == NULL );
}
/*
nvSkiplist* nvSkiplist::Cut(const Slice& key){
    NodePtr prev[kMaxHeight];
    NodePtr x = FindGreaterOrEqual(key,prev);
    if (x == BLANK_NODE)
        return NULL;
    NodePtr head2 = NewNode(Slice(), Slice(), kMaxHeight);
    for (byte i = 0; i < max_height_; ++i) {
        if (prev[i])
            SetNext(head2,i,GetNext(prev[i],i));
        else
            SetNext(head2, i, nvnullptr);
        if (prev[i])
            SetNext(prev[i],i,BLANK_NODE);
    }
    for (byte i = max_height_; i < kMaxHeight; ++i)
        SetNext(head2,i,nvnullptr);
    nvSkiplist *rest = new nvSkiplist(mng_, head2);
    rest->storage_usage_ = storage_usage_ / 2;
    storage_usage_ = rest->storage_usage_;
    return rest;
}
*/

nvSkiplist::NodePtr nvSkiplist::GetMiddle(std::string& key) {
    int top = GetMaxHeight();
    std::vector<NodePtr> a;

    for (int level = top-1; level >= 0; --level){
        for (NodePtr x = GetNext(head_,level); x; x = GetNext(x,level))
            a.push_back(x);
        if (level == 0 || a.size() >= 16) {
            assert(a.size() > 0);
            NodePtr result = a[a.size()/2];
            key = GetKey(result);
            return result;
        } else
            a.clear();
    }
    assert(false);
}

void nvSkiplist::Print() {
    static const bool fully_print = 0;
    //static const bool fully_print = 1;

    printf("Print Skiplist [Height = %d]:\n",max_height_);
    for (int level = 0; level < GetMaxHeight(); ++level){
        NodePtr l = head_;
        int count = 0;
        printf("  level %2d:\n    ",level);
        for (NodePtr r = GetNext(l,level); r != BLANK_NODE; r = GetNext(l,level)) {
            l = r;
            std::string value;
            if (GetValueAddr(l) != BLANK_NODE)
                value = GetValue(l);
            if (++count < 12 || fully_print)
                printf("[%s:%s], ",
                       GetKey(l).c_str(),
                       value.c_str()
                );
            else if (count == 12 && !fully_print) printf("...");
        }
        printf("\n  level %2d print finished, %d ele(s) in total.\n",level,count);
    }
    printf("All Print Finished.\n");
}

};

#endif // NVSKIPLIST_CC
