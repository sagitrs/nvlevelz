#ifndef TRIE_COMPRESSED_H
#define TRIE_COMPRESSED_H

#include <string>
#include <map>
#include <algorithm>
#include <assert.h>
#include "leveldb/slice.h"
#include "global.h"
#include "leveldb/iterator.h"
#include <stdlib.h>
#include "index.h"
#include "nvmemtable.h"

namespace leveldb {
using std::string;
using std::map;
struct nvMemTable;
struct CTrie : public AbstractIndex {
private:
    static const size_t HASH_LIMIT = 256;
    CTrie* parent_;

    char ch_;
    string str_;
    const char* String() const { return str_.data(); }
    inline size_t Size() const { str_.size(); }

    typedef std::map<char, CTrie*> Dict;
    Dict child_;
    CTrie** hash_;
    CTrie** boost_hash_;

    struct DataInfo {
        CTrie* prev_;
        CTrie* next_;
        nvMemTable* data_;
        DataInfo(nvMemTable* data) : prev_(nullptr), next_(nullptr), data_((data)) {}
    } *info_;

    inline size_t strcommon(const char* a, const char* b, const size_t len) {
        for (size_t i = 0; i < len; ++i)
            if (a[i] != b[i]) return i;
        return len;
    }
    inline CTrie* FindCommon(const Slice& key, size_t* p_key, size_t *p_str) {
        const size_t l = key.size();
        const char* s = key.data();
        CTrie* t = this, *son = nullptr;
        size_t str_len = 0;
        for (*p_key = 0; *p_key < l; (*p_key)++) {
            str_len = t->Size();
            if (str_len > 0) {
                *p_str = strcommon(t->String(), s+*p_key, MIN(l-*p_key, t->Size()));
                *p_key += *p_str;
                if (*p_str < str_len || *p_key == l)
                    return t;
            } else
                *p_str = 0;
            son = t->GetChild(s[*p_key]);
            if (son == nullptr)
                return t;
            t = son;
        }

        if (key.size() == 0) {
            *p_key = 0;
            *p_str = 0;
            return this;
        }
        assert(*p_key == l);
        *p_str = 0;
        return t;

        //assert(key.size() == 0);
        //assert(false);
    }
    CTrie* FindCommonOld(const Slice& key, size_t* p_key, size_t *p_str) {
        const size_t l = key.size();
        const char* s = key.data();
        CTrie* t = this;
        size_t i, j;
        for (i = 0; i < l; ++i) {
            if (t->Size() > 0) {
                j = strcommon(t->String(), s+i, MIN(l-i, t->Size()));
                i += j;
                if (j < t->Size()) {
                    *p_key = i;
                    *p_str = j;
                    return t;
                }
            }

            CTrie* son = t->GetChild(s[i]);
            if (son == nullptr) {
                *p_key = i;
                *p_str = t->Size();
                return t;
            }
            t = son;
        }
        *p_key = l;
        *p_str = 0;
        return t;
    }
    void CleanHash() {
        if (hash_) {
            delete[] hash_;
            hash_ = nullptr;
        }
    }
    void AddChild(char ch, CTrie* t) {
        assert(child_.find(ch) == child_.end());
        child_[ch] = t;
        //t->parent_ = this;
        if (hash_ == nullptr) {
            assert(boost_hash_ == nullptr);
            HashBuild();
            return;
        }
        hash_[ch] = t;
        typename Dict::iterator p = child_.find(ch);
        p++;
        int x = 256;
        if  (p != child_.end())
            x = p->first;

        for (int i = ch; i < x; ++i)
            boost_hash_[i] = t;
    }
    void DelChild(char ch, CTrie* *child = nullptr) {
        auto p = child_.find(ch);
        assert( p != child_.end());
        CTrie* t = p->second;
        p++;
        int x = (p == child_.end()) ? 256 : p->first;
        if (child) *child = t;
        //delete t;
        child_.erase(ch);
        if (child_.empty()){
            HashBuild();
            return;
        }
        hash_[ch] = nullptr;
        CTrie* prev = (ch == 0) ? nullptr : boost_hash_[ch-1];
        for (int i = ch; i < x; ++i)
            boost_hash_[i] = prev;
    }
    inline CTrie* GetChild(char ch) const {
        if (hash_)
            return hash_[ch];
        else
            return nullptr;
    }
    CTrie* GetChildLessOrEqual(char ch) const { if (boost_hash_) return boost_hash_[ch]; return nullptr; }
    CTrie* GetChildLessThan(char ch) const {
        if (ch == 0) return nullptr;
        CTrie* le = GetChildLessOrEqual(ch);
        if (le != nullptr && le == GetChild(ch))
            return GetChildLessOrEqual(ch - 1);
        return le;
    }
    void SetString(const Slice& key) {
        assert(key.size() > 0);
        ch_ = key[0];
        str_.assign(key.data()+1, key.size()-1);
    }
    void SetInfo(DataInfo *info) {
        if (info_ != nullptr) {
            DataInfo *old = info_;
            info_ = info == nullptr ? nullptr : new DataInfo(*info);
            delete old;
        }
        info_ = info == nullptr ? nullptr : new DataInfo(*info);
    }
    void SetChild(Dict* child) {
        child_.clear();
        if (child != nullptr)
            child_.insert(child->begin(), child->end());
        HashBuild();
    }

    void SetAll(const Slice& key, DataInfo *info, Dict* child) {
        SetString(key);
        SetInfo(info);
        SetChild(child);
        HashBuild();
    }
    CTrie* Smallest() {
        CTrie* t;
        for (t = this; t->info_ == nullptr; t = t->child_.begin()->second){
            assert(!t->child_.empty());
        }
        assert(t->info_ != nullptr);
        return t;
    }
    CTrie* Largest() {
        CTrie* t;
        for (t = this; !t->child_.empty(); t = t->child_.rbegin()->second);
        assert(t->child_.empty() && t->info_ != nullptr);
        return t;
    }
    CTrie* Prev() {
        if (info_ == nullptr)
            return Smallest()->Prev();
        assert(info_ && info_->prev_); // Main Node always has ""->DType.
        return info_->prev_;
    }
    CTrie* Next() {
        if (info_ != nullptr && child_.empty())
            return info_->next_;
        return Smallest();
    }
    void HashBuild() {
        delete[] hash_;
        delete[] boost_hash_;
        hash_ = nullptr;
        boost_hash_ = nullptr;
        if (child_.size() == 0)
            return;
        hash_ = new CTrie*[HASH_LIMIT];
        boost_hash_ = new CTrie*[HASH_LIMIT];
        typename Dict::iterator p = child_.begin();
        CTrie* prev = nullptr;
        for (int i = 0; i < 256; ++i) {
            if (p != child_.end() && i == p->first) {
                hash_[i] = p->second;
                prev = hash_[i];
                p++;
            } else
                hash_[i] = nullptr;
            boost_hash_[i] = prev;
        }
    }
    std::string Key() const {
        string s;
        if (parent_ == nullptr)
            return "";
        s += ch_;
        s += str_;
        return parent_->Key() + s;
    }
    static void Check(CTrie* t) {
        if (t->info_ != nullptr)
            return;
        if (t->child_.size() > 1)
            return;

        if (t->child_.empty()) {
            CTrie* p = t->parent_;
            if (p == nullptr)
                return;
            p->DelChild(t->ch_, nullptr);
            Check(p);
            delete t;
            return;
        }

        CTrie* c = t->child_.begin()->second;
        string s = t->ch_ + t->str_ + c->ch_ + c->str_;
        t->SetAll(Slice(s), c->info_, c->child_.empty() ? nullptr : &c->child_);
        for (auto p = c->child_.begin(); p != c->child_.end(); ++p)
            p->second->parent_ = t;

        if (t->info_) {
            if (t->info_->prev_)
                t->info_->prev_->info_->next_ = t;
            if (t->info_->next_)
                t->info_->next_->info_->prev_ = t;
        }
        delete c;
        if (t->parent_)
            Check(t->parent_);
    }
public:
    CTrie(nvMemTable* data) :
        parent_(nullptr),
        ch_(0),
        str_(""),
        child_(),
        hash_(nullptr),
        boost_hash_(nullptr),
        info_(new DataInfo(data)) {
    }
    CTrie(CTrie* parent, const Slice& key, DataInfo *info, Dict* child) :
        parent_(parent),
        ch_(key[0]),
        str_(key.data() + 1, key.size() - 1),
        child_(child == nullptr ? Dict() : *child),
        hash_(nullptr),
        boost_hash_(nullptr),
        info_(info == nullptr ? nullptr : new DataInfo(*info)) {
        if (child_.size() > 0)
            HashBuild();
    }
    virtual ~CTrie() {
        delete[] hash_;
        delete[] boost_hash_;
        delete info_;
        //assert(info_ == nullptr);
    }

    virtual void Add(const Slice& key, nvMemTable* data) {
        size_t p_key, p_str;
        CTrie* t = FindCommon(key, &p_key, &p_str);
        Slice key_rest(key), str_rest(t->str_);
        key_rest.remove_prefix(p_key);
        str_rest.remove_prefix(p_str);
        assert( p_str < t->Size() ||
                ( p_str == t->Size() && key_rest.size() == 0 ) ||
                ( p_str == t->Size() && t->GetChild(key_rest[0]) == nullptr) );
        CTrie *t1 = nullptr, *t2 = nullptr;
        if (p_str < t->Size()) {
            t1 = new CTrie(
                        t,
                        str_rest,
                        t->info_,
                        t->child_.empty() ? nullptr : &t->child_);
            for (auto p = t->child_.begin(); p != t->child_.end(); ++p)
                p->second->parent_ = t1;
            if (t1->info_) {
                if (t1->info_->prev_)
                    t1->info_->prev_->info_->next_ = t1;
                if (t1->info_->next_)
                    t1->info_->next_->info_->prev_ = t1;
            }
            string new_str = "";
            new_str += t->ch_;
            new_str += t->str_.substr(0,p_str);
            t->SetAll(new_str, nullptr, nullptr);
            t->AddChild(t1->ch_, t1);
        }
        CTrie* p;
        if (key_rest.size() == 0) {
            if (t->info_ != nullptr) {
                t->info_->data_ = (data);
                return;
            }
            t2 = t;
            p = t->Prev();
            assert( p != nullptr );
        }
        else {
            t2 = new CTrie(t, key_rest, nullptr, nullptr);
            p = t->GetChildLessThan(key_rest[0]);
            if (p != nullptr)
                p = p->Largest();
            else if (t->info_)
                p = t;
            else
                p = t->Prev();
            assert( p != nullptr );
        }
        DataInfo * newinfo_ = new DataInfo(data);

        newinfo_->next_ = p->info_->next_;
        newinfo_->prev_ = p;
        if (p->info_->next_ != nullptr)
            p->info_->next_->info_->prev_ = t2;
        p->info_->next_ = t2;

        t2->SetInfo(newinfo_);

        if (t != t2) {
            assert(t->GetChild(key_rest[0]) == nullptr);
            t->AddChild(key_rest[0], t2);
        }
    }

    virtual bool Delete(const Slice& key, nvMemTable* *data = nullptr) {
        bool equal = false;
        CTrie* t = LocateLessOrEqual(key, &equal);
        if (!equal)
            return false;
        assert( t->info_ != nullptr );

        if (t->parent_ == nullptr) {
            // a method of generating root data:
            // if ""->data is Deleted, it keeps the data of its next.
            CTrie* n = t->info_->next_;
            if (n == nullptr) {
                printf("Error : Trie is becoming empty.\n");
                return 0;
            }
            nvMemTable* new_data;
            if (data != nullptr)
                *data = t->info_->data_;
            Delete(n->Key(), &new_data);
            info_->data_ = (new_data);
            return 1;
        }

        CTrie* prev = t->info_->prev_;
        CTrie* next = t->info_->next_;
        if (prev)
            prev->info_->next_ = next;
        if (next)
            next->info_->prev_ = prev;
        if (data)
            *data = t->info_->data_;
        delete t->info_;
        t->info_ = nullptr;

        Check(t);
        return 1;
    }

    virtual bool Get(const Slice& key, nvMemTable* *data) {
        size_t p_key, p_str;
        CTrie* t = FindCommon(key, &p_key, &p_str);
        if (key.size() > p_key) return false;
        if (t->info_ == nullptr) return false;
        *data = t->info_->data_;
        return true;
    }

    CTrie* LocateLessOrEqualOld(const Slice& key, bool* equal) {
        size_t p_key, p_str;
        CTrie* t = FindCommon(key, &p_key, &p_str);
        Slice key_rest(key), str_rest(t->str_);
        key_rest.remove_prefix(p_key);
        str_rest.remove_prefix(p_str);
        if (str_rest.size() > 0) {
            if (key_rest.size() == 0 || key_rest[0] < str_rest[0])
                return t->Smallest()->info_->prev_;
            else
                return t->Largest()->info_->next_;
        }
        assert( p_str == t->Size() );
        if (key_rest.size() == 0) {
            if (t->info_ != nullptr) {
                *equal = true;
                return t;
            }
            *equal = false;
            return Smallest()->info_->prev_;
        }
        assert( key_rest.size() > 0 );
        assert( t->GetChild(key_rest[0]) == nullptr );
        CTrie* c = t->GetChildLessThan(key_rest[0]);
        if (c == nullptr)
            return t->Smallest()->info_->prev_;
        else
            return c->Largest();
    }
    inline CTrie* LocateLessOrEqual(const Slice& key, bool* equal) {
        size_t p_key, p_str;
        CTrie* t = FindCommon(key, &p_key, &p_str);

        if (t->Size() > p_str) {
            if (key.size() == p_key || key[p_key] < t->str_[p_str])
                return t->Smallest()->info_->prev_;
            else
                return t->Largest();
        }
        //assert( p_str == t->Size() );
        if (key.size() == p_key) {      // Equal?
            if (t->info_ != nullptr) {
                *equal = true;
                return t;
            }
            *equal = false;
            return t->Smallest()->info_->prev_;
        }
        //assert( key.size() > p_key );
        //assert( t->GetChild(key_rest[0]) == nullptr );
        CTrie* c = t->GetChildLessThan(key[p_key]);
        if (c == nullptr)
            return t->info_ ? t : t->Smallest()->info_->prev_;
        else
            return c->Largest();
    }

    bool GetLessOrEqual(const Slice& key, nvMemTable* *data, bool *equal) {
        CTrie* ans = LocateLessOrEqual(key, equal);
        if (ans == nullptr)
            return false;
        assert( ans->info_ != nullptr );
        *data = ans->info_->data_;
        return true;
    }

    virtual nvMemTable* FuzzyFind(const Slice& key) {
        nvMemTable* data;
        bool equal = false;
        if (!GetLessOrEqual(key, &data, &equal)) {
            printf("Error : Fuzzy Not Found.\n");
        }
        return data;
    }
    virtual void FuzzyLeftBoundary(const Slice& key, std::string *value) {
        bool equal = false;
        CTrie* t = LocateLessOrEqual(key, &equal);
        if (t == nullptr)
            *value = "";
        else
            value->assign(t->Key());
        //return t->Key();
    }
    virtual void FuzzyRightBoundary(const Slice& key, std::string *value) {
        //DType data;
        bool equal = false;
        CTrie* t = LocateLessOrEqual(key, &equal);
        if (t == nullptr) {
            value->assign("");
            return;
        }
        t = t->info_->next_;
        if (t == nullptr) {
            value->assign("");
            return;
        }
        value->assign(t->Key());
    }


    class CTrieIterator : public IndexIterator {
     public:
        CTrieIterator(CTrie* main) : main_(main), current_(nullptr) {}
        virtual ~CTrieIterator(){

        }

      // An iterator is either positioned at a key/value pair, or
      // not valid.  This method returns true iff the iterator is valid.
      virtual bool Valid() const {
          return current_ != nullptr && current_->info_ != nullptr;
      }

      // Position at the first key in the source.  The iterator is Valid()
      // after this call iff the source is not empty.
      virtual void SeekToFirst() {
          current_ = main_->Smallest();
      }

      // Position at the last key in the source.  The iterator is
      // Valid() after this call iff the source is not empty.
      virtual void SeekToLast() {
          current_ = main_->Largest();
      }

      // Position at the first key in the source that is at or past target.
      // The iterator is Valid() after this call iff the source contains
      // an entry that comes at or past target.
      virtual void Seek(const Slice& target) {
          current_ = main_->LocateLessOrEqual(target, &equal);
      }

      // Moves to the next entry in the source.  After this call, Valid() is
      // true iff the iterator was not positioned at the last entry in the source.
      // REQUIRES: Valid()
      virtual void Next() {
          current_ = current_->info_->next_;
      }

      // Moves to the previous entry in the source.  After this call, Valid() is
      // true iff the iterator was not positioned at the first entry in source.
      // REQUIRES: Valid()
      virtual void Prev() {
          current_ = current_->info_->prev_;
      }

      // Return the key for the current entry.  The underlying storage for
      // the returned slice is valid only until the next modification of
      // the iterator.
      // REQUIRES: Valid()
      virtual Slice key() const {
            return current_->Key();
      }

      // Return the value for the current entry.  The underlying storage for
      // the returned slice is valid only until the next modification of
      // the iterator.
      // REQUIRES: Valid()
      virtual Slice value() const {
            return Slice();
      }
      virtual nvMemTable* Data() const {
          return current_->info_->data_;
      }

      // If an error has occurred, return it.  Else return an ok status.
      virtual Status status() const {
          return Status::OK();
      }

      // No copying allowed
      CTrieIterator(const CTrieIterator&);
      void operator=(const CTrieIterator&);
    private:
      CTrie* main_;
      CTrie* current_;
      bool equal;
    };
    virtual IndexIterator* NewIterator() {
        return new CTrieIterator(this);
    }
    void Print() {/*
        CTrieIterator *iter = NewIterator(), *next = NewIterator();

        next->SeekToFirst(); next->Next();
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            string key = iter->Key(), key2 = next->Valid() ? next->Key() : "";
            int value = iter->Data();
            assert(!next->Valid()|| key < next->Key());
            printf("%s : %d\n", key.c_str(), value);
            if (next->Valid()) next->Next();
        }
        delete iter;
        delete next;*/
    }
};

};

#endif // TRIE_COMPRESSED_H
