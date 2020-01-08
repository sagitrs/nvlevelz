#ifndef LINEAR_SKIPLIST_H
#define LINEAR_SKIPLIST_H

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
#include "nvm_library/arena.h"
#include "port/atomic_pointer.h"

using std::string;

namespace leveldb {

struct L2MemTableInfo {
    nvAddr nodeAddr_;
    nvAddr valueAddr_;
    ull valueSize_;
    L2MemTableInfo(nvAddr node, nvAddr addr, ull size)
        : nodeAddr_(node), valueAddr_(addr), valueSize_(size) {}
    L2MemTableInfo() : nodeAddr_(nvnullptr), valueAddr_(nvnullptr), valueSize_(0) {}
};

struct LinearSkiplist {
private:
    // 0. Const.
    enum { kMaxHeight = 12, kBranching = 4 };

    // 1. Memory Management.
    typedef L2MemTableInfo DataType;
    struct Node {
        typedef Node* NodePtr;
        const Slice key_;
        DataType info_;
        const byte height_;
        port::AtomicPointer next_[1];
        Node(const Slice& key, const DataType& info, byte height) :
            key_(key), info_(info), height_(height) {
        }
        ~Node() {}
    };
    typedef Node* NodePtr;

    inline static const Slice& GetKey(NodePtr node) {
        return node->key_;
    }
    inline static const DataType& GetInfo(NodePtr node) {
        return node->info_;
    }
    inline static void SetInfo(NodePtr node, const DataType& info) {
        node->info_ = info;
    }
    inline static NodePtr GetNext(NodePtr node, byte n) {
        assert(n <= node->height_);
        //return node->next_[n];
        return reinterpret_cast<Node*>(node->next_[n].Acquire_Load());
    }
    inline static void SetNext(NodePtr node, byte n, NodePtr next) {
        assert(n <= node->height_);
        node->next_[n].NoBarrier_Store(next);
    }
    inline static byte GetHeight(NodePtr node) {
        return node->height_;
    }
    NodePtr NewNode(const Slice& key, const DataType& info, byte height) {
        const size_t usage = sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1);
        return new (arena_->AllocateAligned(usage)) Node(key,info,height);
//        return new Node(key,info,height);
    }

    //2.Compare Function.
    struct Comparator {
        int Compare(const Slice& a, const Slice &b) const {
            return a.compare(b);
        }
    } cmp_;
    bool Equal(const Slice& a, const Slice& b) const { return cmp_.Compare(a,b)==0; }

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
        return n != nullptr && (cmp_.Compare(GetKey(n), key) < 0);
    }
    // iff key > *n.
    NodePtr FindGreaterOrEqual(const Slice& key, NodePtr* prev) const {
        NodePtr l = head_;
        NodePtr r;
        for (int level = GetMaxHeight() - 1; level >= 0; level--){
            for (r = GetNext(l,level);
                 r != nullptr && cmp_.Compare(GetKey(r), key) < 0;
                 r = GetNext(l,level))
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
            for (NodePtr r = GetNext(l, level);
                 r != nullptr && cmp_.Compare(GetKey(r), key) < 0;
                 r = GetNext(l, level))
                l = r;
        return l;
    }
    //return the node < key. all next is recorded in node.
    NodePtr FindLast() const {
        NodePtr l = head_;
        for (int level = GetMaxHeight() - 1; level >= 0; --level)
            for (NodePtr r = GetNext(l,level);
                 r != nullptr;
                 r = GetNext(l,level))
                l = r;
        return l;
    }
    void FindTails(NodePtr* tails, NodePtr begin = nullptr, NodePtr end = nullptr) const {
        NodePtr l = (begin == nullptr ? head_ : begin);
        NodePtr r;
        assert(tails != NULL);
        for (int level = (begin == nullptr ? GetMaxHeight() : l->height_) - 1; level >= 0; level--){
            for (r = GetNext(l,level); r != end; r = GetNext(l,level))
                l = r;
            tails[level] = l;
        }
    }
    //5. Private operation.
    NodePtr Insert_(const Slice& key, const DataType& info, NodePtr* prev) {
        int height = RandomHeight();
        if (height > GetMaxHeight()) {
            for (int i = GetMaxHeight(); i < height; ++i)
                prev[i] = head_;
            max_height_ = height;
        }

        NodePtr x = NewNode(key, info, height);

        for (int i = 0; i < height; i++) {
            SetNext(x, i, GetNext(prev[i],i));
            SetNext(prev[i], i, x);
        }
        return x;
    }
    std::string GetMiddle() {
        int top = GetMaxHeight();
        std::vector<NodePtr> a;

        for (int level = top-1; level >= 0; --level){
            for (NodePtr x = GetNext(head_,level); x; x = GetNext(x,level))
                a.push_back(x);
            if (level == 0 || a.size() >= 16) {
                assert(a.size() > 0);
                NodePtr result = a[a.size()/2];
                return GetKey(result).ToString();
            } else
                a.clear();
        }
        assert(false);
    }

    LinearSkiplist* Cut(const Slice& key) {
        NodePtr prev[kMaxHeight];
        NodePtr x = FindGreaterOrEqual(key,prev);
        if (x == nullptr)
            return NULL;
        NodePtr head2 = NewNode(Slice(), DataType(), kMaxHeight);
        for (byte i = 0; i < kMaxHeight; ++i) if (prev[i] != nullptr){
            SetNext(head2,i,GetNext(prev[i],i));
            SetNext(prev[i],i,nullptr);
        }
        return new LinearSkiplist(arena_, head2);
    }
    static byte MaxHeightOf(Node* node) {
        for (char h = static_cast<char>(node->height_-1); h >= 0; --h) {
            if (GetNext(node,h))
                return static_cast<byte>(h+1);
        }
        return 1;
    }
public:

    explicit LinearSkiplist(DRAM_Linear_Allocator* arena)
        : rnd_(0xdeadbeef),
          max_height_(1),
          head_(NewNode(Slice(), DataType(), kMaxHeight)),
          arena_(arena) {
      for (int i = 0; i < kMaxHeight; i++) {
        SetNext(head_, i, NULL);
      }
    }
    explicit LinearSkiplist(DRAM_Linear_Allocator* arena, NodePtr head)
        : rnd_(0xdeadbeef),
          max_height_(MaxHeightOf(head)),
          head_(head),
          arena_(arena) {
    }
    ~LinearSkiplist() {}
    void Insert(const Slice& key, const DataType& info) {
        NodePtr prev[kMaxHeight];
        NodePtr x = FindGreaterOrEqual(key,prev);
        //assert(x == nvBLANK_PTR || !Equal(key, GetKey(x)));
        // you just can't insert the same key.
        if (x != nullptr && Equal(key, GetKey(x))) {
            SetInfo(x,info);
        } else {
            Insert_(key, info, prev);
        }
    }
    bool Contains(const Slice& key) const {
        NodePtr x = FindGreaterOrEqual(key, nullptr);
        return x != nullptr && Equal(key, GetKey(x));
    }
    bool TryGet(const Slice& key, DataType& info) const {
        NodePtr x = FindGreaterOrEqual(key, nullptr);
        if (x == nullptr || !Equal(key, GetKey(x)))
            return false;

        info = GetInfo(x);
        return true;
    }
    LinearSkiplist* Separate() {
        std::string mid = GetMiddle();
        return Cut(mid);
    }
    void Print() {
        static const bool fully_print = 0;
        //static const bool fully_print = 1;

        printf("Print Skiplist [Height = %d]:\n",max_height_);
        for (int level = 0; level < GetMaxHeight(); ++level){
            NodePtr l = head_;
            int count = 0;
            printf("  level %2d:\n    ",level);
            for (NodePtr r = GetNext(l,level); r != nullptr; r = GetNext(l,level)) {
                l = r;
                DataType info = GetInfo(l);
                if (++count < 12 || fully_print)
                    printf("[%s:(%llu,%llu,%llu)], ",
                           GetKey(l).data(),
                           info.nodeAddr_,info.valueAddr_,info.valueSize_
                    );
                else if (count == 12 && !fully_print) printf("...");
            }
            printf("\n  level %2d print finished, %d ele(s) in total.\n",level,count);
        }
        printf("All Print Finished.\n");
    }

private:
    NodePtr head_;
    DRAM_Linear_Allocator* arena_;
    //return the last node in list.

    LinearSkiplist(const LinearSkiplist&) = delete;
    void operator=(const LinearSkiplist&) = delete;
    // No copying allowed.

};

};
#endif // LINEAR_SKIPLIST_H
