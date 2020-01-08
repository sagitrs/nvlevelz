#ifndef D0SKIPLIST_H
#define D0SKIPLIST_H

#include "nvmemtable.h"
#include "db/dbformat.h"

namespace leveldb {
struct D0MemTableArena {
public:
    std::vector<byte*> block_;
    static const size_t BlockSize = 1 * MB;
    //size_t current_block_;

    byte* alloc_ptr_;
    ull alloc_remaining_;

    enum {TotalSize = 0, NodeBound = 8, ValueBound = 16, HeadAddress = 24, MemTableInfoSize = 32 };

    D0MemTableArena() :
        block_(), //current_block_(0),
        alloc_ptr_(0), alloc_remaining_(0)
    {
    }
    ~D0MemTableArena() {
        for (size_t i = 0; i < block_.size(); ++i) {
            delete[] block_[i];
        }
    }
    byte* Allocate(size_t size) {
        if (size > alloc_remaining_)
            return AllocateNewBlock(size);
        byte* ans = alloc_ptr_;
        alloc_remaining_ -= size;
        alloc_ptr_ += size;
        return ans;
    }
    byte* AllocateNewBlock(size_t size) {
        alloc_ptr_ = new byte[BlockSize];
        block_.push_back(alloc_ptr_);
        alloc_remaining_ = BlockSize;
        return Allocate(size);
    }

    void Dispose(byte* addr, byte* size) {
        ;
    }

    D0MemTableArena(const D0MemTableArena&) = delete;
    void operator=(const D0MemTableArena&) = delete;
};

struct D0MemTable {
private:
    D0MemTableArena arena_;
    byte* head_;
    byte max_height_;
    enum { HeightOffset = 0, KeySize = 0 + 1, ValueOffset = 1 + sizeof(ull), NextOffset = ValueOffset + sizeof(byte*) };
    enum { kMaxHeight = 12 };
    static const ull BlankMark = 1ULL << 63;
    //L2SkipList* msl_;

    int refs__;

    Random rnd_;
    int RandomHeight() {
        // Increase height with probability 1 in kBranching
        static const unsigned int kBranching = 4;
        int height = 1;
        while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
          height++;
        }
        assert(height > 0);
        assert(height <= kMaxHeight);
        return height;
    }
    byte* Head() const { return head_; }
    void SetHead(byte* head) { head_ = head; }

    byte* GetNext(byte* x, byte level) const {
        return *reinterpret_cast<byte**>(x + NextOffset + level * sizeof(byte*));
    }
    void SetNext(byte* x, byte level, byte* next) {
        *reinterpret_cast<byte**>(x + NextOffset + level * sizeof(byte*)) = next;
    }
    void SetValuePtr(byte* x, byte* value) {
        *reinterpret_cast<byte**>(x + ValueOffset) = value;
    }
    byte* GetValuePtr(byte* x) const {
        return *reinterpret_cast<byte**>(x + ValueOffset);
    }
    byte GetHeight(byte* x) const {
        return x[0];
    }
    ull GetKeySize(byte* x) const {
        return *reinterpret_cast<ull*>(x + KeySize);
    }
    ull GetValueSizeFromPtr(byte* v) const {
        return *reinterpret_cast<ull*>(v);
    }
    char* GetKeyPtr(byte* x) const {
        return reinterpret_cast<char*>(x + NextOffset + GetHeight(x) * sizeof(byte*));
    }
    Slice GetKey(byte* x) const {
       return Slice(GetKeyPtr(x), GetKeySize(x));
    }
    void GetKey(byte* x, std::string* key) const {
        Slice k = GetKey(x);
        key->assign(k.data(), k.size());
    }
    Slice GetValue(byte* x) const {
        byte* v = GetValuePtr(x);
        if (v == nullptr) return Slice();
        return Slice(reinterpret_cast<char*>(v + sizeof(ull)), GetValueSizeFromPtr(v));
    }
    void SetValue(byte* x, const Slice& value) {
        SetValuePtr(x, NewValue(value));
    }
    byte* NewValue(const Slice& value) {
        if (value.size() == 0) return nullptr;
        byte* v = arena_.Allocate(value.size() + sizeof(ull));
        *reinterpret_cast<ull*>(v) = value.size();
        memcpy(v + sizeof(ull), value.data(), value.size());
        return v;
    }
    byte* NewNode(const Slice& key, const Slice& value, byte height, byte* *prev) {
       size_t total_size = 1 + sizeof(ull) + sizeof(byte*) + sizeof(byte*) * height + key.size();
       byte* x = arena_.Allocate(total_size);
       byte* v = NewValue(value);

       byte* p = x;
       *p = height;
       p ++;
       *reinterpret_cast<ull*>(p) = key.size();
       p += sizeof(ull);
       *reinterpret_cast<byte**>(p) = v;
       p += sizeof(byte*);
       if (prev)
           for (byte i = 0; i < height; ++i) {
               *reinterpret_cast<byte**>(p) = GetNext(prev[i], i);
               p += sizeof(byte*);
           }
       else {
           memset(p, 0, height * sizeof(byte*));
           p += sizeof(byte*) * height;
       }
       if (key.size() > 0) memcpy(p, key.data(), key.size());
       return x;
   }
public:
   D0MemTable()
       : arena_(),
         head_(nullptr),
         max_height_(1), refs__(0), rnd_(0xdeadbeef)
   {
       head_ = NewNode("", "", kMaxHeight, nullptr);
       for (byte i = 0; i < kMaxHeight; ++i)
           SetNext(head_, i, nullptr);
       SetHead(head_);
   }

   virtual ~D0MemTable() {}

 // An iterator is either positioned at a key/value pair, or
 // not valid.  This method returns true iff the iterator is valid.
    byte* Seek(const Slice& target, byte* *prev) {
        byte level = max_height_ - 1;
        byte* x = head_;
        byte* next = nullptr;
        int cmp;

        while (true) {
            next = GetNext(x, level);
            cmp = (next == nullptr ? -1 : target.compare(GetKey(next)));
            if (cmp > 0)
                x = next;      // Right.
            else {
                if (prev)
                    prev[level] = x;
                if (level == 0) {
                    if (cmp == 0) return next;
                    return nullptr;
                } else {
                    // Switch to next list
                    level--;   // Down.
                }
            }
        }
        assert(false);
        return nullptr;
    }
    void Insert(const Slice& key, const Slice& value, byte height, byte** prev) {
        if (height > max_height_)
            for (byte i = max_height_; i < height; ++i) {
                prev[i] = head_;
            }

        byte* x = NewNode(key, value, height, prev);
        if (height > max_height_) {
            for (byte i = max_height_; i < height; ++i)
                SetNext(head_, i, x);
            max_height_ = height;
        }
        for (byte i = 0; i < height; ++i)
            SetNext(prev[i], i, x);
    }
    void Update(byte* x, const Slice& value) {
        SetValue(x, value);
    }
    void Add_(const Slice& key, const Slice& value, byte height) {
        byte* cache_prev[kMaxHeight];
        byte* x = Seek(key, cache_prev);
        if (x == nullptr)
            Insert(key, value, height, cache_prev);
        else
            Update(x, value);
    }
    void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) {
        Add_(key, value, RandomHeight());
    }
    byte* Get_(const Slice& key) {
        return Seek(key, nullptr);
    }
    bool Get(const LookupKey& lkey, std::string* value, Status* s) {
        byte* next = Seek(lkey.user_key(), nullptr);
        if (next == nullptr) return false;
        Slice v = GetValue(next);
        if (v.size() == 0) {
            *s = Status::NotFound(Slice());
            return true;
        }
        value->assign(v.data(), v.size());
        return true;

    }
    virtual ull StorageUsage() const {
        return 0;
    }
    virtual void CheckValid() {
        Slice prev, key;
        for (byte level = 0; level < max_height_; ++level) {
            prev = Slice();
            for (byte* x = GetNext(head_,level); x != nullptr; x = GetNext(x, level)) {
                key = GetKey(x);
                assert(key.compare(prev) > 0);
            }
        }
    }
    void operator=(const D0MemTable&) = delete;
};

}



#endif // D0SKIPLIST_H
