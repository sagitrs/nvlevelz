#ifndef NVSKIPLIST_H
#define NVSKIPLIST_H

#include <cstdio>
#include <string>
#include <cstdlib>
#include <cmath>
#include <ctime>
#include <vector>
#include "leveldb/slice.h"
#include "slice_static.h"
#include "util/random.h"
#include "global.h"
#include "nvm_manager.h"
#include <map>
#include <queue>
#include "skiplist_kvindram.h"
#include "mem_bloom_filter.h"
#include "mem_node_cache.h"

using std::string;
#define BLANK_NODE nvnullptr

namespace leveldb {
struct nvSkiplist {
    typedef nvAddr NodePtr;
    typedef nvAddr DataPtr;
private:
    // 0. Const.
    //static const nvAddr BLANK_NODE = nvnullptr;
    enum { kMaxHeight = 12, kBranching = 4 };
    enum {
        NodeHeightOffset = 0,
        KeyLengthOffset = 1,
        ValueAddressOffset = 9,
        NextOffset = 17,
    };
    //  Node = [Height = 1][KeyLength = 8][ValueAddress = 8][Next_0:Height][Key_0:KeyLength]
    // Value = [ValueLength = 8][Value_0:ValueLength]
    ll storage_usage_;

    // 1. Memory Management.
    NVM_Manager* mng_;
    //MemBloomFilter* filter_;

    struct Node {
        byte height_;
        size_t key_size_;
        NodePtr* next_;
        char *key_;
        Node() : height_(0), key_size_(0), next_(nullptr), key_(nullptr) {}
        ~Node() {
            delete[] next_;
            delete[] key_;
        }

        void SetHeight(byte height) {
            height_ = height;
            next_ = new NodePtr[height];
        }
        void SetKey(const Slice& key) {
            key_ = new char[key.size()+1];
            memcpy(key_, key.data(), key.size());
            key_[key.size()] = 0;
            key_size_ = key.size();
        }
        void SetNext(byte level, NodePtr next) {
            next_[level] = next;
        }
        byte GetHeight() const { return height_; }
        ull GetKeySize() const { return key_size_; }
        const char* GetKey() const { return key_; }
        NodePtr GetNext(byte k) const { return next_[k]; }
        const char* GetKeyPartial(int offset) const { return key_ + offset; }
    };
    std::unordered_map<NodePtr, Node*> cache_;
    CachePolicy cache_policy_;
    ull cache_storage_usage_;
    bool CacheSave(byte height);

    inline nvAddr GetValueAddr(NodePtr node) {
        return mng_->read_addr(node + ValueAddressOffset);
    }
    inline byte GetHeight(NodePtr node) {
        return mng_->read_ull(node + NodeHeightOffset) & 0xFF;
    }
    inline ull GetKeySize(NodePtr node) {
        return mng_->read_ull(node +KeyLengthOffset);
    }
    inline nvAddr GetKeyAddr(NodePtr node) {
        return node + NextOffset + 8 * GetHeight(node);
    }
    inline nvAddr GetNext(NodePtr node, byte n) {
        if (node == nvnullptr) return nvnullptr;
        return mng_->read_addr(node + NextOffset + 8 * n);
    }
    inline void SetValueAddr(NodePtr node, nvAddr v) {
        mng_->write_addr(node + ValueAddressOffset, v);
    }
    inline void SetNext(NodePtr node, byte n, nvAddr next) {
        assert(node != nvnullptr);
        mng_->write_addr(node + NextOffset + 8 * n, next);
    }
    inline bool isDeletion(NodePtr node) {
        return GetValueAddr(node) == BLANK_NODE;
    }
    string GetKey(NodePtr node);
    const char* GetKeyBoost(NodePtr node);
    const char* GetPartialKeyBoost(NodePtr node);

    string GetValue(NodePtr node);
    const char* GetValueBoost(NodePtr node);

    NodePtr NewNode(const Slice& key, const Slice& value, byte height);

    void DisposeNode(NodePtr node);
    //2.Compare Function.
    struct Comparator {
        int Compare(const Slice& a, const Slice &b) const {
            return a.compare(b);
        }
    } cmp_;
    bool EqualPartial(const Slice& a, const Slice& b) const {
        return cmp_.Compare(
                    Slice(a.data()+common_size_, a.size()-common_size_),
                    Slice(b.data()+common_size_, b.size()-common_size_)
               )==0;
    }
    bool Equal(const Slice& a, const Slice& b) const { return cmp_.Compare(a,b)==0; }

    //3. Height Calculation.
    Random rnd_;
    byte max_height_;
    inline byte GetMaxHeight() const { return max_height_; }
    int RandomHeight(){
        int height = 1;
        while (height < kMaxHeight && (rnd_.Next() % kBranching == 0))
            height ++;
        assert( height > 0 );
        assert( height <= kMaxHeight );
        return height;
    }

    //4. Basic Search Logic.
    bool KeyIsAfterNode(const Slice& key, NodePtr n) {
        return n != BLANK_NODE && (cmp_.Compare(GetKey(n), key) < 0);
    }
    // iff key > *n.
    NodePtr FindGreaterOrEqualPartial(const Slice& key, NodePtr* prev) {
        NodePtr l = head_;
        NodePtr r;
        for (int level = GetMaxHeight() - 1; level >= 0; level--){
            for (r = GetNext(l,level); r != BLANK_NODE &&
                 cmp_.Compare(GetPartialKeyBoost(r), Slice(key.data()+common_size_, key.size()-common_size_)) < 0;
                 r = GetNext(l,level))
                l = r;
            if (prev != NULL)
                prev[level] = l;
        }
        return r;
    }
    NodePtr FindGreaterOrEqualWithCache(const Slice& key, NodePtr* prev) {
        NodePtr l = head_, r;
        std::unordered_map<NodePtr, Node*>::iterator p;
        const char* tmp_key_;
        NodePtr next_level_;
        for (byte level = GetMaxHeight() - 1; level >= 0; level--) {
            //r = GetNext(l, level);
            if (level >= cache_policy_.height_ && ((p = cache_.find(r)) != cache_.end()))
                r = p->second->GetNext(level);
            else
                r = GetNext(l, level);

            while (r != BLANK_NODE) {
                if (level >= cache_policy_.height_ && ((p = cache_.find(r)) != cache_.end())) {
                    tmp_key_ = p->second->GetKey();
                    if (cmp_.Compare(tmp_key_, key) >= 0)
                        break;
                    next_level_ = p->second->GetNext(level);
                } else {
                    tmp_key_ = GetKeyBoost(r);
                    if (cmp_.Compare(tmp_key_, key) >= 0)
                        break;
                    next_level_ = GetNext(r, level);
                }
                l = r;
                r = next_level_;
            }
            if (prev != NULL)
                prev[level] = l;
        }
        return r;
    }
    NodePtr FindGreaterOrEqual(const Slice& key, NodePtr* prev) {
        NodePtr l = head_;
        NodePtr r;
        for (int level = GetMaxHeight() - 1; level >= 0; level--){
            for (r = GetNext(l,level); r != BLANK_NODE && cmp_.Compare(GetKeyBoost(r), key) < 0; r = GetNext(l,level))
                l = r;
            if (prev != NULL)
                prev[level] = l;
        }
        return r;
    }
    //return the node >= key, if exist, return prevs in every level.
    NodePtr FindLessThan(const Slice& key) {
        NodePtr l = head_;
        for (int level = GetMaxHeight() - 1; level >= 0; --level)
            for (NodePtr r = GetNext(l, level); r != BLANK_NODE && cmp_.Compare(GetKey(r), key) < 0; r = GetNext(l, level))
                l = r;
        return l;
    }
    //return the node < key. all next is recorded in node.
    NodePtr FindLast() {
        NodePtr l = head_;
        for (int level = GetMaxHeight() - 1; level >= 0; --level)
            for (NodePtr r = GetNext(l,level); r != BLANK_NODE; r = GetNext(l,level))
                l = r;
        return l;
    }
    void FindTails(NodePtr* tails, NodePtr begin = BLANK_NODE, NodePtr end = BLANK_NODE) {
        NodePtr l = (begin == BLANK_NODE ? head_ : begin);
        NodePtr r;
        assert(tails != NULL);
        for (int level = (begin == BLANK_NODE ? GetMaxHeight() : GetHeight(l)) - 1; level >= 0; level--){
            for (r = GetNext(l,level); r != end; r = GetNext(l,level))
                l = r;
            tails[level] = l;
        }
    }

    //5. Private operation.
    void Update(NodePtr x, const Slice& value, uint64_t seq, bool isDeletion) {
        //if (GetSeq(x) >= seq) return;
        const nvAddr oldValueAddr = GetValueAddr(x);
        //mng_->read_addr(x + ValueAddressOffset);
        const nvAddr oldValueSize = oldValueAddr == nvnullptr ? 0 : mng_->read_ull(oldValueAddr);
        if (isDeletion)
            DirectUpdate(x, oldValueAddr, oldValueSize, Slice(), seq, isDeletion);
        else
            DirectUpdate(x, oldValueAddr, oldValueSize, value, seq, isDeletion);
    }

    void Inserts_(DramKV_Skiplist* a, NodePtr begin, byte height, NodePtr* prev);
    NodePtr Insert_(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, NodePtr* prev);

public:
    class Iterator {
    public:
        explicit Iterator(nvSkiplist* list) : list_(list), node_(BLANK_NODE) {}
        bool Valid() const { return node_ != BLANK_NODE; }
        std::string key() {
            assert(Valid());
            //string s = list_->GetKey(node_);
            //return std::string(s.data(),s.size());
            return string(list_->GetKeyBoost(node_));
        }
        std::string value() {
            return list_->GetValue(node_);
        }
        uint64_t seq() const {
            assert(false);
        }
        bool isDeletion() const {
            return list_->GetValueAddr(node_) == BLANK_NODE;
        }
        size_t size() const {
            size_t valueSize = 0;
            nvAddr valueAddr = list_->GetValueAddr(node_);
            if (valueAddr != nvnullptr)
                valueSize = list_->mng_->read_ull(valueAddr) + 8;
            return nvSkiplist::NextOffset
                 + list_->GetKeySize(node_)
                 + list_->GetHeight(node_) * 8
                 + valueSize;
        }
        void Next() {
            assert(Valid());
            node_ = list_->GetNext(node_,0);
        }
        void Prev() {
            assert(Valid());
            node_ = list_->FindLessThan(list_->GetKey(node_));
            if (node_ == list_->head_)
                node_ = NULL;
        }
        void Seek(const Slice& target){
            node_ = list_->FindGreaterOrEqual(target, NULL);
        }
        void SeekToFirst(){
            node_ = list_->GetNext(list_->head_, 0);
        }
        void SeekToLast() {
            node_ = list_->FindLast();
            if (node_ == list_->head_)
                node_ = NULL;
        }
    private:
        nvSkiplist* list_;
        NodePtr node_;
    };

    explicit nvSkiplist(NVM_Manager* mng, const CachePolicy& cache_policy);
    //explicit nvSkiplist(NVM_Manager* mng, NodePtr head, size_t commonSize = 0);
    ~nvSkiplist();
    void Insert(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion);
    void InsertPartial(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion);
    void DirectUpdate(NodePtr nodeAddr, DataPtr valueAddr, ull valueSize, const Slice& value, uint64_t seq, bool isDeletion);
    ll Inserts(DramKV_Skiplist *a);
    bool Contains(const Slice& key);
    bool Get(const Slice& key, std::string* value, Status* s);
    bool GetPartial(const Slice& key, std::string* value, Status* s);
    bool TryGet(const Slice& key, std::string* &value);
    bool TryGetPartial(const Slice& key, std::string* &value);
    //nvSkiplist* Cut(const Slice& key);
    NodePtr GetMiddle(std::string& key);
    void ClearCache() {
        for (auto p = cache_.begin(); p != cache_.end(); ++p)
            delete p->second;
        cache_.clear();
    }
    Iterator* NewIterator() { return new Iterator(this); }
    bool isEmpty() const { return empty_; }
    void SetCommonSize(size_t common) { common_size_ = common; }
    void Print();
    ll StorageUsage() const { return storage_usage_; }
    //void ClearCache() { }
    void Dispose() {
        NodePtr prev_x = GetNext(head_,0);
        NodePtr x;
        if (prev_x == nvnullptr) return;
        for (x = GetNext(prev_x, 0); x != nvnullptr; x = GetNext(x,0)) {
            DisposeNode(prev_x);
            prev_x = x;
        }
        DisposeNode(x);
    }
    size_t DEBUG_UsageCheck() {
        size_t total = 0;
        for (NodePtr x = GetNext(head_,0); x != nvnullptr; x = GetNext(x,0)) {
            DataPtr v = GetValueAddr(x);
            byte height = GetHeight(x);
            size_t keySize = GetKeySize(x);
            size_t valueSize = (v ? mng_->read_ull(v) + 8 : 0);
            total += NextOffset + height * 8 + keySize * 1 + valueSize;
        }
        return total;
    }

private:
    NodePtr head_;
    size_t common_size_;
    bool empty_;
    //return the last node in list.
    Arena* arena_;

    nvSkiplist(const nvSkiplist&) = delete;
    void operator=(const nvSkiplist&) = delete;
    // No copying allowed.
};

};

#endif // NVSKIPLIST_H
