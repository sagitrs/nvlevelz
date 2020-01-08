#ifndef KVINDRAM_SKIPLIST_H
#define KVINDRAM_SKIPLIST_H

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
#include "db/memtable.h"


using std::string;
#define BLANK_PTR nullptr

namespace leveldb {

struct DramKV_Skiplist {
private:
    // 0. Const.
    enum { kMaxHeight = 12, kBranching = 4 };

    // 1. Memory Management.
    typedef StaticSlice* DataPtr;
    struct Node {
        typedef Node* NodePtr;
        DataPtr key_;
        DataPtr value_;
        uint64_t seq_;
        NodePtr* next_;
        byte height_;
        Node(DataPtr key, DataPtr value, uint64_t seq, byte height) :
            key_(key), value_(value), seq_(seq), next_(BLANK_PTR), height_(height) {
            next_ = new NodePtr[height];
        }
        ~Node(){ delete key_; delete value_; delete[] next_; }

        ll size() {
            return 0LL + (key_ ? key_->size() : 0)
                 + (value_ ? value_->size() : 0)
                 + (8)  // seq_
                 + (height_ * 8) // next
                 + (1); // height
        }
    };
    typedef Node* NodePtr;
    ll memory_usage_;

    const StaticSlice& GetKey(NodePtr node) const {
        return *(node->key_);
    }
    StaticSlice* GetValuePtr(NodePtr node) const {
        return node->value_;
    }
    uint64_t GetSeq(NodePtr node) const {
        return node->seq_;
    }
    void SetValue(NodePtr node, StaticSlice *value, uint64_t seq) {
        StaticSlice *tmp = node->value_;
        node->value_ = value;
        node->seq_ = seq;
        memory_usage_ += (value ? value->size() : 0LL) - (tmp ? tmp->size() : 0LL);
        delete tmp;
    }
    NodePtr GetNext(NodePtr node, byte n) const {
        assert(n <= node->height_);
        return node->next_[n];
    }
    void SetNext(NodePtr node, byte n, NodePtr next) {
        assert(n <= node->height_);
        node->next_[n] = next;
    }
    byte GetHeight(NodePtr node) const {
        return node->height_;
    }
    NodePtr NewNode(StaticSlice* key, StaticSlice* value, uint64_t seq, byte height) {
        memory_usage_ +=  (key ? key->size() : 0)
                        + (value ? value->size() : 0)
                        + (8)  // seq_
                        + (height * 8) // next
                        + (1);
        return new Node(key,value,seq,height);
    }
    void DisposeNode(NodePtr node){
        memory_usage_ -= node->size();
        delete node;
    }

    //2.Compare Function.
    struct Comparator {
        int Compare(const Slice& a, const StaticSlice &b) const {
            return a.compare(b.ToSlice());
        }
        int Compare(const StaticSlice& a, const Slice &b) const {
            return a.ToSlice().compare(b);
        }
        int Compare(const StaticSlice &a, const StaticSlice &b) const {
            return a.ToSlice().compare(b.ToSlice());
        }
        int Compare(const Slice& a, const Slice &b) const {
            return a.compare(b);
        }
    } cmp_;
    bool Equal(const Slice& a, const Slice& b) const { return cmp_.Compare(a,b)==0; }
    bool Equal(const StaticSlice& a, const Slice& b) const { return cmp_.Compare(a,b)==0; }
    bool Equal(const Slice& a, const StaticSlice& b) const { return cmp_.Compare(a,b)==0; }
    bool Equal(const StaticSlice& a, const StaticSlice& b) const { return cmp_.Compare(a,b)==0; }

    //3. Height Calculation.
    Random rnd_;
    byte max_height_;

    inline int GetMaxHeight() const { return max_height_; }

    int RandomHeight(){
        int height = 1;
        while (height < kMaxHeight && (rnd_.Next() % kBranching == 0))
            height ++;
        assert( height > 0 );
        assert( height <= kMaxHeight );
        return height;
    }

    //4. Basic Search Logic.
    bool KeyIsAfterNode(const Slice& key, NodePtr n) const {
        return n != BLANK_PTR && (cmp_.Compare(GetKey(n), key) < 0);
    }
    // iff key > *n.
    NodePtr FindGreaterOrEqual(const Slice& key, NodePtr* prev) const {
        NodePtr l = head_;
        NodePtr r;
        for (int level = GetMaxHeight() - 1; level >= 0; level--){
            for (r = GetNext(l,level); r != BLANK_PTR && cmp_.Compare(GetKey(r), key) < 0; r = GetNext(l,level))
                l = r;
            if (prev != NULL)
                prev[level] = l;
        }
        return r;
    }
    //return the node >= key, if exist, return prevs in every level.
    NodePtr FindLessThan(const Slice& key) const {
        NodePtr l = head_;
        for (int level = GetMaxHeight() - 1; level >= 0; --level)
            for (NodePtr r = GetNext(l, level); r != BLANK_PTR && cmp_.Compare(GetKey(r), key) < 0; r = GetNext(l, level))
                l = r;
        return l;
    }
    //return the node < key. all next is recorded in node.
    NodePtr FindLast() const {
        NodePtr l = head_;
        for (int level = GetMaxHeight() - 1; level >= 0; --level)
            for (NodePtr r = GetNext(l,level); r != BLANK_PTR; r = GetNext(l,level))
                l = r;
        return l;
    }
    void FindTails(NodePtr* tails, NodePtr begin = BLANK_PTR, NodePtr end = BLANK_PTR) const {
        NodePtr l = (begin == BLANK_PTR ? head_ : begin);
        NodePtr r;
        assert(tails != NULL);
        for (int level = (begin == BLANK_PTR ? GetMaxHeight() : l->height_) - 1; level >= 0; level--){
            for (r = GetNext(l,level); r != end; r = GetNext(l,level))
                l = r;
            tails[level] = l;
        }
    }
    //5. Private operation.
    void Update(NodePtr x, const Slice& value, uint64_t seq, bool isDeletion){
        if (GetSeq(x) >= seq) return;
        if (isDeletion){
            SetValue(x,BLANK_PTR,seq);
        } else {
            StaticSlice* v = new StaticSlice(value);
            SetValue(x,v,seq);
        }
    }

    void Insert_(DramKV_Skiplist* a, NodePtr begin, byte height, NodePtr* prev);
    void Insert_(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, NodePtr* prev);

public:
    class Iterator {
    public:
        explicit Iterator(const DramKV_Skiplist* list) : list_(list), node_(BLANK_PTR) {}
        bool Valid() const { return node_ != BLANK_PTR; }
        const StaticSlice& key() const {
            assert(Valid());
            return list_->GetKey(node_);
        }
        StaticSlice* valuePtr() const {
            return list_->GetValuePtr(node_);
        }
        uint64_t seq() const {
            return list_->GetSeq(node_);
        }
        bool isDeletion() const {
            return list_->GetValuePtr(node_) == BLANK_PTR;
        }
        void Next() {
            assert(Valid());
            node_ = list_->GetNext(node_,0);
        }
        void Prev() {
            assert(Valid());
            node_ = list_->FindLessThan(list_->GetKey(node_).ToSlice());
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
        const DramKV_Skiplist* list_;
        NodePtr node_;
    };

    explicit DramKV_Skiplist();
    explicit DramKV_Skiplist(NodePtr head);
    ~DramKV_Skiplist();
    void Insert(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion);
    void Insert(DramKV_Skiplist *a);
    bool Contains(const Slice& key) const;
    bool TryGet(const Slice& key, std::string* &value) const;
    std::string GetMiddle();
    DramKV_Skiplist* Cut(const Slice& key);
    Iterator* NewIterator() { return new Iterator(this); }
    bool isEmpty() const { return empty_; }
    void Print();
    ll ApproximateMemoryUsage() const { return memory_usage_; }
    static DramKV_Skiplist* BuildFromMemtable(MemTable* mem);
    std::string Head() const {
        Iterator iter(this);
        iter.SeekToFirst();
        std::string result = "";
        if (!iter.Valid()) return result;
        StaticSlice* s = iter.valuePtr();
        if (s) {
            result = s->ToString();
        }
        return result;
    }
    void Append_Start();
    void Append_Stop();
    void Append(const std::string &key, const std::string &value, ull seq);


private:
    NodePtr head_;
    struct TailInfo {
        NodePtr tail_;
        NodePtr *tails_;
        TailInfo() : tail_(BLANK_PTR), tails_(nullptr) {}
        ~TailInfo() { delete[] tails_; }
    } tailinfo_;
    bool empty_;
    //return the last node in list.

    DramKV_Skiplist(const DramKV_Skiplist&) = delete;
    void operator=(const DramKV_Skiplist&) = delete;
    // No copying allowed.

};

};
#endif // KVINDRAM_SKIPLIST_H
