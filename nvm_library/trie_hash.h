#ifndef TRIE_HASH_H
#define TRIE_HASH_H

#include <string>
#include <map>
#include <algorithm>
#include <assert.h>
using std::string;
using std::map;
//int total = 0;

template <typename DType>
struct HashTrie {
    Trie* parent_;
    char ch_;
    map<char, Trie*> child_;
    bool boost_;
    Trie** boost_hash_;
    static const size_t HASH_LIMIT = 8;

    Trie() : parent_(nullptr), ch_(0), child_(), node_(nullptr),
        boost_(false), boost_hash_(nullptr) {}
    Trie(Trie* parent, char c) : parent_(parent), ch_(c), child_(), node_(nullptr),
        boost_(false), boost_hash_(nullptr) {}
    ~Trie() { delete [] boost_hash_; }
    string Key() const {
        string s;
        for (const Trie* t = this; t; t = t->parent_)
        s += t->ch_;
        s.erase(s.size()-1); // except the main char '\0'.
        std::reverse(s.begin(), s.end());
        return s;
    }
    DType* Data() const { return node_ == nullptr ? nullptr : node_->data_; }
    Trie* Next() const {
        if (node_) return node_->next_;
        if (HasChild()){
            Trie* t;
            for (t = MinChild(); t; t = t->MinChild()){
                if (t->Data()) return t;
                assert(t->HasChild());
            }
        }

        for (const Trie* t = this; t; t = t->parent_)
            if (t->parent_ != nullptr) {
                Trie* brother = t->parent_->NextChild(t->ch_);
                if (brother != nullptr) {
                    if (brother->Data())
                        return brother;
                    return brother->Next();
                }
            }

        //printf("Error/Warning: This should only happen when Trie is null.\n");
        return nullptr;
    }
    Trie* Head() const {
        if (Data()) return parent_->Child(ch_);
        return Next();
    }
    Trie* Tail() const {
        for (Trie* t = MaxChild(); t; t = t->MaxChild())
            if (!t->HasChild()) {
                assert(t->Data());
                return t;
            }
        if (Data())
            return parent_->Child(ch_);
        return nullptr;
    }
    Trie* Prev() const {
        if (node_) return node_->prev_;
        if (parent_ == nullptr) return nullptr;
        Trie* brother = parent_->PrevChild(ch_);
        if (brother == nullptr) {
            if (parent_->Data())
                return parent_;
            return parent_->Prev();
        }
        Trie* tail = brother->Tail();
        if (tail == nullptr) {
            printf("Wait.\n");
        }
        assert(tail != nullptr);
        return tail;
    }
    void SetNext(Trie* next) {
        assert(node_ != nullptr);
        node_->next_ = next;
    }
    void SetPrev(Trie* prev) {
        assert(node_ != nullptr);
        node_->prev_ = prev;
    }
    void SetNode(DType* data, Trie* next, Trie* prev) {
//        total += 1;
        assert(node_ == nullptr);
        node_ = new Node(data, next, prev);
        printf("[%s]-[%s]-[%s]\n",
               (prev ? prev->Key().c_str() : ""),
               Key().c_str(),
               (next ? next->Key().c_str() : ""));
        if (next) next->SetPrev(this);
        if (prev) prev->SetNext(this);
    }
    void SetNode(DType* data) {
        if (node_ == nullptr) {
            SetNode(data, Next(), Prev());
        } else
            node_->data_ = data;
    }
    void DelNode() {
        assert( node_ != nullptr );
        if (node_->next_)
            node_->next_->SetPrev(node_->prev_);
        if (node_->prev_)
            node_->prev_->SetNext(node_->next_);
        delete node_;
        node_ = nullptr;
    }
    Trie* Child(char c) const {
        if (boost_) {
            Trie* result = boost_hash_[c];
            if (result == nullptr || result->ch_ != c)
                return nullptr;
            return result;
        }
        auto p = child_.find(c);
        if (p == child_.end()) return nullptr;
        return p->second;
    }
    Trie* MinChild() const {
        if (HasChild()) return child_.begin()->second;
        return nullptr;
    }
    Trie* MaxChild() const {
        if (HasChild()) return child_.rbegin()->second;
        return nullptr;
    }
    Trie* PrevChild(char c, bool force = false) const {
        switch (child_.size()) {
        case 0:
            return nullptr;
        case 1:
            if (!force)
                return nullptr;
            if (child_.begin()->first < c)
                return child_.begin()->second;
            return nullptr;
        default:
            break;
        }
        if (boost_)
            return boost_hash_[c-1];
        auto p = child_.find(c);
        if (!force)
            assert(p != child_.end());
        else {
            for (p = child_.begin(); p != child_.end(); ++p)
                if (p->first >= c) {
                    assert(p->first > c);
                    break;
                }
        }
        if (p == child_.begin())
            return nullptr;
        p--;
        return p->second;
    }
    Trie* NextChild(char c, bool force = false) const {
        auto p = child_.find(c);
        if (!force) {
            if (p == child_.end()){
                printf("Wait.\n");
            }
            //assert(p != child_.end());
        } else {
            for (p = child_.begin(); p != child_.end(); ++p)
                if (p->first >= c){
                    assert(p->first > c);
                    return p->second;
                }
            return nullptr;
        }
        p++;
        if (p == child_.end())
            return nullptr;
        return p->second;
    }

    void ResetHash() {
        Trie* l = nullptr;
        auto p = child_.begin();
        for (size_t i = 0; i < 256; ++i) {
            if (i == p->first) {
                l = p->second;
                if (p != child_.end())
                    p++;
            }
            boost_hash_[i] = l;
        }
    }

    void SetChild(char c, Trie* t) {
        child_[c] = t;
        if (child_.size() >= HASH_LIMIT) {
            boost_ = 1;
            if (boost_hash_ == nullptr)
                boost_hash_ = new Trie*[256];
            ResetHash();
        }
    }
    void DelChild(char c) {
        child_.erase(c);
        if (child_.size() < HASH_LIMIT) {
            boost_ = 0;
        } else
            ResetHash();
    }
    bool HasChild() const { return !child_.empty(); }

    Trie* Find_(const string& key, size_t* pos) {
        const size_t l = key.size();
        const char* s = key.data();
        Trie* t = this;
        size_t i;
        for (i = 0; i < l; ++i) {
            Trie* son = t->Child(s[i]);
            if (son == nullptr) {
                *pos = i;
                return t;
            }
            t = son;
        }
        *pos = i;
        return t;
    }
    Trie* FuzzyFind_(const string& key) {
        const size_t l = key.size();
        size_t i;
        Trie* t = Find_(key,&i);
        if (i == l)
            return t->Data() ? t : t->Prev();
        assert(t->Child(key[i]) == nullptr);
        Trie* child = t->PrevChild(key[i], true);
        if (child == nullptr)
            return t->Data() ? t : t->Prev();
        else
            return child;
    }
    void Insert_(const char* key, size_t size, DType* data) {
        //total += 1;
        assert (size >= 1);
        char ch = key[0];
        Trie* first = new Trie(this, ch);
        Trie* t = first;
        Trie* lb = PrevChild(ch, true), *rb = NextChild(ch, true);
        Trie* prev = (lb ? lb->Tail() : (Data() ? this : Prev()));
        Trie* next = (rb ? rb->Head() : nullptr);
        if (!next && prev)
            next = prev->Next();

        for (size_t i = 1; i < size; ++i) {
            Trie* son = new Trie(t,key[i]);
            t->SetChild(key[i], son);
            t = son;
        }
        t->SetNode(data,next,prev);
        this->SetChild(ch, first);
    }
    void Delete_(Trie* target) {
//        total -= 1;
        target->DelNode();
        if (target->HasChild())
            return;
        assert(!target->Data() && !target->HasChild());
        char k = target->ch_;
        for (Trie* t = target->parent_; t; t = t->parent_) {
            t->DelChild(k);
            if (!t->Data() && !t->HasChild()) {
                k = t->ch_;
            } else
                return;
        }
    }

    void Insert(const string& key, DType* data) {
        if (key.size() > 4 && key[0] == 'P' && key[1] == 'G' && key[2] == 'J' && key[3] == 'P'){
            printf("Wait");
        }
        const size_t l = key.size();
        size_t i = 0;
        Trie* t = Find_(key, &i);

        if (i == l)
            t->SetNode(data);
        else
            t->Insert_(key.data()+i, l-i, data);
    }
    bool Delete(const string& key) {
        if (key[0] == 'H' && key[1] == 'H'){
//            printf("Wait");
        }
        const size_t l = key.size();
        size_t i = 0;
        Trie* t = Find_(key, &i);
        if (i == l){
            if (t->Data()){
                Delete_(t);
                return 1;
            } else
                return 0;
        }
        return 0;
    }
    DType* FuzzyFind(const string& key) {
        Trie* t = FuzzyFind_(key);
        if (t == nullptr)
            return Head()->Data();
        return t->Tail()->Data();
    }
    void Print() {
        Iterator* iter = new Iterator(this);
        int count = 0;
        printf("Print -> :\n");
        for (iter->SeekToFirst();
             iter->Valid();
             iter->Next()) {
            count ++;
            printf("[%s]->",iter->Key().c_str());
            if (iter->Data())
                printf("%3d,\t",*iter->Data());
            else
                printf("X,\t");
        }
        printf("\n");
        printf("%d elements in total.\n",count);
//        if (count != total)
//            printf("Error! Data Count doesn't match! (count = %d, total=%d)\n",count,total);
    }

    class Iterator {
     public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit Iterator(Trie* tree) : main_tree_(tree), current_(nullptr) {
      }

      // Returns true iff the iterator is positioned at a valid node.
      bool Valid() const {
          return current_ != nullptr && current_->Data();
      }

      // Returns the key at the current position.
      // REQUIRES: Valid()
      string Key() const {
          assert(Valid());
          return current_->Key();
      }
      DType* Data() const {
          assert(Valid());
          return current_->Data();
      }

      // Advances to the next position.
      // REQUIRES: Valid()
      void Next() {
          current_ = current_->Next();
      }

      // Advances to the previous position.
      // REQUIRES: Valid()
      void Prev() {
          current_ = current_->Prev();
      }

      // Advance to the first entry with a key >= target
      void Seek(const string& key) {
          current_ = main_tree_->FuzzyFind_(key);
      }

      // Position at the first entry in list.
      // Final state of iterator is Valid() iff list is not empty.
      void SeekToFirst() {
          current_ = main_tree_->Next();
      }

      // Position at the last entry in list.
      // Final state of iterator is Valid() iff list is not empty.
      void SeekToLast() {
          current_ = main_tree_->Tail();
      }

     private:
      Trie* main_tree_;
      Trie* current_;
      // Intentionally copyable
    };
    Iterator* NewIterator() {
        return new Iterator(this);
    }
};

#endif // TRIE_HASH_H
