#ifndef SKIPLIST_DYNAMIC_H
#define SKIPLIST_DYNAMIC_H

#include <cstdio>
#include <string>
#include <cstdlib>
#include <cmath>
#include <ctime>
#include <vector>
#include "arena.h"
#include "dram_allocator.h"
#include "util/random.h"

using std::string;

template<typename Key, class Comparator>
struct DynamicSkipList {
private:
    enum { kMaxHeight = 15, kBranching = 4 };
private:
    struct Node {
        Key const key;
        int height__;
        explicit Node(const Key& k, const int height) : key(k), height__(height) {/*
            next_ = new Node*[height];
            for (int i=0; i<height; ++i)
                next_[i] = nullptr;*/
        }
        ~Node(){ delete[] next_; }
        Node* Next(int n) {
            assert(0 <= n);
            return next_[n];
        }
        void SetNext(int n, Node* x){
            assert(0 <= n);
            next_[n] = x;
        }
        Node* next_[1];
    };

public:
    explicit DynamicSkipList(Comparator cmp, DRAM_Allocator *allocator);
    ~DynamicSkipList();
    void Insert(const Key& key);
    bool Delete(const Key& key);
    bool Contains(const Key& key) const;
    void print();

    class Iterator {
    public:
        explicit Iterator(const DynamicSkipList* list) : list_(list), node_(NULL) {}
        bool Valid() const { return node_ != NULL; }
        const Key& key() const { assert(Valid()); return node_->key; }
        void Next() { assert(Valid()); node_ = node_->Next(0); }
        void Prev() {
            assert(Valid());
            node_ = list_->FindLessThan(node_->key);
            if (node_ == list_->head_)
                node_ = NULL;
        }
        void Seek(const Key& target){
            node_ = list_->FindGreaterOrEqual(target, NULL);
        }
        void SeekToFirst(){
            node_ = list_->head_->Next(0);
        }
        void SeekToLast() {
            node_ = list_->FindLast();
            if (node_ == list_->head_)
                node_ = NULL;
        }
    private:
        const DynamicSkipList* list_;
        Node* node_;
    };
private:
    Comparator const cmp_;
    DRAM_Allocator *allocator_;
    Node* const head_;

    leveldb::Random rnd_;
    int max_height_;

    inline int GetMaxHeight() const { return max_height_; }

    Node* NewNode(const Key& key, int height){
        char* mem = reinterpret_cast<char*>(allocator_->Allocate(
                    sizeof(Node)+
                    sizeof(int)+
                    sizeof(Node*)*(height-1)));
        return new (mem) Node(key,height);
    }

    int RandomHeight(){
        int height = 1;
        while (height < kMaxHeight && (rnd_.Next() % kBranching == 0))
            height ++;
        assert( height > 0 );
        assert( height <= kMaxHeight );
        return height;
    }

    bool Equal(const Key& a, const Key& b) const {return cmp_(a,b)==0;}
    bool KeyIsAfterNode(const Key& key, Node* n) const {
        return n != NULL && (cmp_(n->key, key) < 0);
    }
    // iff key > *n.
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const {
        Node* l = head_;
        Node* r;
        for (int level = GetMaxHeight() - 1; level >= 0; level--){
            for (r = l->Next(level); r && cmp_(r->key, key) < 0; r = l->Next(level))
                l = r;
            if (prev != NULL)
                prev[level] = l;
        }
        return r;
    }
    //return the node >= key, if exist, return prevs in every level.
    Node* FindLessThan(const Key& key) const {
        Node* l = head_;
        for (int level = GetMaxHeight() - 1; level >= 0; --level)
            for (Node* r = l->Next(level); r && cmp_(r->key, key) < 0; r = l->Next(level))
                l = r;
        return l;
    }
    //return the node < key. all next is recorded in node.
    Node* FindLast() const {
        Node* l = head_;
        for (int level = GetMaxHeight() - 1; level >= 0; --level)
            for (Node* r = l->Next(level); r; r = l->Next(level))
                l = r;
        return l;
    }
    void FindTails(Node** tails) const {
        Node* l = head_;
        Node* r;
        assert(tails != NULL);
        for (int level = GetMaxHeight() - 1; level >= 0; level--){
            for (r = l->Next(level); r; r = l->Next(level))
                l = r;
            tails[level] = l;
        }
    }
    //return the last node in list.

    DynamicSkipList(const DynamicSkipList&) = delete;
    void operator=(const DynamicSkipList&) = delete;
    // No copying allowed.

};

template<typename Key, class Comparator>
DynamicSkipList<Key,Comparator>::DynamicSkipList(Comparator cmp, DRAM_Allocator* allocator)
    : cmp_(cmp),
      allocator_(allocator),
      head_(NewNode(0, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, NULL);
  }
}

template<typename Key, class Comparator>
DynamicSkipList<Key,Comparator>::~DynamicSkipList() {/*
    Node *x = head_;
    if (x == NULL) return;
    Node *x_next = x->Next(0);
    while (x_next){
        delete x;
        x = x_next;
        x_next = x->Next(0);
    }
    delete x;*/
}

template<typename Key, class Comparator>
void DynamicSkipList<Key,Comparator>::Insert(const Key& key) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key,prev);
    assert(x == NULL || !Equal(key,x->key));
    // you just can't insert the same key.

    int height = RandomHeight();
    if (height > GetMaxHeight()) {
        for (int i = GetMaxHeight(); i < height; ++i)
            prev[i] = head_;
        max_height_ = height;
    }
    x = NewNode(key, height);
    for (int i = 0; i < height; i++){
        x ->SetNext(i,prev[i]->Next(i));
        prev[i]->SetNext(i, x);
    }
}

template<typename Key, class Comparator>
bool DynamicSkipList<Key,Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  return x != NULL && Equal(key, x->key);
}

template<typename Key, class Comparator>
bool DynamicSkipList<Key,Comparator>::Delete(const Key& key) {
  static Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);
  if (x == nullptr) return false;
  if (!Equal(x->key,key)) return false;
  for (int i = 0; i < x->height__; ++i) {
      if (prev[i] == nullptr) break;
      prev[i]->SetNext(i,x->Next(i));
  }
  allocator_->Dispose(x,
                      sizeof(Node)+
                      sizeof(int)+
                      sizeof(Node*)*(x->height__-1)
                      );
  return true;
//  return x != NULL && Equal(key, x->key);
}



template<typename Key, class Comparator>
void DynamicSkipList<Key,Comparator>::print(){
    static const bool fully_print = 0;
    //static const bool fully_print = 1;

    printf("Print SkipList [Height = %d]:\n",max_height_);
    for (int level = 0; level < GetMaxHeight(); ++level){
        Node* l = head_;
        int count = 0;
        printf("  level %2d:\n    ",level);
        for (Node* r = l->Next(level); r; r = l->Next(level)){
            l = r;
            if (++count < 12 || fully_print)
                printf("%s, ",l->key.ToString().c_str());
            else if (count == 12 && !fully_print) printf("...");
        }
        printf("\n  level %2d print finished, %d ele(s) in total.\n",level,count);
    }
    printf("All Print Finished.\n");
}

#endif // SKIPLIST_DYNAMIC_H
