#ifndef D1SKIPLIST_H
#define D1SKIPLIST_H

//#include "l2skiplist.h"
#include "nvmemtable.h"
#include <unordered_map>

namespace leveldb {
struct L2MemTable_D1SkipList;

struct MemTableAllocator {
public:
    byte* main_;
    nvOffset total_size_, rest_size_;
    nvOffset node_bound_, value_bound_;
    static const ull BlockSize = 32768;
    enum {TotalSize = 0, NodeBound = 8, ValueBound = 16, HeadAddress = 24, MemTableInfoSize = 32};
    nvOffset node_record_size_, value_record_size_;

    MemTableAllocator(ul size) :
        main_(new byte[size]),
        total_size_(size), rest_size_(size),
        node_bound_(MemTableInfoSize), value_bound_(total_size_), node_record_size_(BlockSize), value_record_size_(0)
    {
        nvOffset* info = reinterpret_cast<nvOffset*>(main_);
        info[0] = node_record_size_;
        info[1] = total_size_ - value_record_size_;
        info[2] = total_size_;
        info[3] = HeadAddress;
        //assert(size > BlockSize);
    }
    MemTableAllocator(byte* main, ul size, nvOffset lft, nvOffset rgt) :
        main_(main), total_size_(size), rest_size_(0),
        node_bound_(0), value_bound_(size),
        node_record_size_(0), value_record_size_(0) {
        SetBound(lft, rgt);
    }

    ~MemTableAllocator() {
        delete [] main_;
    }
    nvOffset AllocateNode(nvOffset size) {
        if (size > rest_size_) return nulloffset;
        nvOffset ans = node_bound_;
        node_bound_ += size;
        rest_size_ -= size;
        if (node_bound_ > node_record_size_)
            SetNodeRecord();
        return ans;
    }
    nvOffset Allocate(nvOffset size) {
        return AllocateNode(size);
    }
    nvOffset AllocateValue(nvOffset size) {
        if (size > rest_size_) return nulloffset;
        value_bound_ -= size;
        rest_size_ -= size;
        if (value_bound_ + value_record_size_ > total_size_)
            SetValueRecord();
        return value_bound_;
    }

    void Dispose(nvOffset addr, nvOffset size) {
    }

    nvOffset Garbage() const {
        return 0;
    }
    void SetNodeRecord() {
        node_record_size_ = (node_bound_ / BlockSize + (node_bound_ % BlockSize > 0)) * BlockSize;
        *reinterpret_cast<nvOffset*>(main_ + NodeBound) = node_record_size_;
    }
    void SetValueRecord() {
        nvOffset value_size = total_size_ - node_bound_;
        value_record_size_ = (value_size / BlockSize + (value_size % BlockSize > 0)) * BlockSize;
        *reinterpret_cast<nvOffset*>(main_ + ValueBound) = total_size_ - value_record_size_;
    }
    void SetBound(nvOffset node, nvOffset value) {
        node_bound_ = node;
        value_bound_ = value;
        SetNodeRecord();
        SetValueRecord();
        rest_size_ = total_size_ - value_bound_ - (total_size_ - value_bound_);
    }
    byte* Main() const { return main_; }
    nvOffset Size() const { return total_size_; }
    nvOffset StorageUsage() const { return total_size_ - rest_size_; }
    nvOffset RestSpace() const { return rest_size_; }

    MemTableAllocator(const MemTableAllocator&) = delete;
    void operator=(const MemTableAllocator&) = delete;
};

struct D1SkipList {
    MemTableAllocator arena_;
 private:
    nvOffset head_;
    byte max_height_;
    enum { HeightOffset = 0, KeySize = 1, ValueOffset = 5, NextOffset = 9 };
    enum { kMaxHeight = 12 };
    //L2SkipList* msl_;
    byte* mem() const { return arena_.Main(); }
    nvOffset Head() const { return head_; }
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
    nvOffset GetNext(nvOffset x, byte level) const {
        assert(x != nulloffset);
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem() + x + NextOffset + level * 4));
    }
    void SetNext(nvOffset x, byte level, nvOffset next) {
        assert(x != nulloffset);
        EncodeFixed32(reinterpret_cast<char*>(mem() + x + NextOffset + level * 4), next);
    }
    nvOffset GetValuePtr(nvOffset x) const {
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem() + x + ValueOffset));
    }
    void SetValuePtr(nvOffset x, nvOffset newvalue) {
        EncodeFixed32(
                    reinterpret_cast<char*>(
                        mem() + x + ValueOffset), newvalue);
    }
    byte GetHeight(nvOffset x) const {
        return mem()[x + HeightOffset];
    }
    nvOffset GetKeySize(nvOffset x) const {
        return DecodeFixed32(
                    reinterpret_cast<const char*>(
                        mem() + x + KeySize));
    }
    nvOffset GetKeyPtr(nvOffset x) const {
        return x + NextOffset + GetHeight(x) * 4;
    }
    nvOffset ValueGetSize(nvOffset valueptr) const {
        return DecodeFixed32(reinterpret_cast<const char*>(mem() + valueptr));
    }
    nvOffset NewValue(const Slice& value) {
        if (value.size() == 0) return nulloffset;
        nvOffset y = arena_.AllocateValue(value.size() + 4);
        assert(y != nulloffset);
        EncodeFixed32(reinterpret_cast<char*>(mem() + y), value.size());
        memcpy(mem() + y + 4, value.data(), value.size());
        return y;
    }
    nvOffset NewNode(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, byte height, nvOffset *prev) {
        nvOffset total_size = NextOffset + 4 * height + key.size();
        nvOffset v = isDeletion ? nulloffset : NewValue(value);
        nvOffset x = arena_.AllocateNode(total_size);
        assert( x != nulloffset );

        byte *p = mem() + x;
        *p = height;
        p ++;
        *reinterpret_cast<uint32_t*>(p) = key.size();
        p += 4;
        *reinterpret_cast<uint32_t*>(p) = v;
        p += 4;
        if (prev)
            for (byte i = 0; i < height; ++i) {
                *reinterpret_cast<uint32_t*>(p) = GetNext(prev[i],i);
                p += 4;
            }
        else {
            memset(p, 0, height * 4);
            p += 4 * height;
        }
        memcpy(p, key.data(), key.size());
        return x;
    }
 public:
    D1SkipList(uint32_t size) : arena_(size), head_(nulloffset), max_height_(1), rnd_(0xDEADBEEF) {
        head_ = NewNode("", "", 0, true, kMaxHeight, nullptr);
        for (byte i = 0; i < kMaxHeight; ++i)
            SetNext(head_, i, nulloffset);
    }
    D1SkipList(byte* mem, uint32_t size, uint32_t node_bound, uint32_t value_bound, nvOffset head, byte height) :
        arena_(mem, size, node_bound, value_bound), head_(head), max_height_(height), rnd_(0xdeadbeef) {

    }
    virtual ~D1SkipList() {}

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  Slice GetKey(nvOffset x) const {
        return Slice(reinterpret_cast<char*>(mem() + GetKeyPtr(x)), GetKeySize(x));
    }
  Slice GetValue(nvOffset x) const {
      assert(x != nulloffset);
      nvOffset valueptr = GetValuePtr(x);
      if (valueptr == nulloffset)
          return Slice();
      return Slice(reinterpret_cast<char*>(mem() + valueptr), ValueGetSize(valueptr));
  }
  nvOffset Seek(const Slice& target, nvOffset &x, nvOffset *prev = nullptr) const {
      assert(x != nulloffset);
      byte level = max_height_ - 1;
      nvOffset next = nulloffset;
      int cmp = 0;

        while (true) {
            next = GetNext(x, level);
            //assert(x != nulloffset);
            cmp = (next == nulloffset ? -1 : target.compare(GetKey(next)));
            if (cmp > 0) {
                x = next;      // Right.
                //assert(x != nulloffset);
            } else {
                if (prev) prev[level] = x;
                if (level == 0) {
                    if (cmp == 0) {
                        //x_ = next;
                        return next;    // Found.
                    }
                    //x_ = nulloffset;   // Not Found.
                    return nulloffset;
                } else {
                    // Switch to next list
                    level--;   // Down.
                }
            }
        }
        assert(false);
        return nulloffset;
   }
  nvOffset SeekToLast() {
      nvOffset x = head_;
      nvOffset next = nulloffset;
      for (char height = max_height_ - 1; height >= 0; height --)
          for (next = GetNext(x, height); next != nulloffset; next = GetNext(x, height))
              x = next;
      if (x == head_)
          return nulloffset;
      return x;
  }
  nvOffset SeekToFirst() {
      return GetNext(head_, 0);
  }
  nvOffset Insert(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, nvOffset *prev) {
      byte height = RandomHeight();
      if (height > max_height_)
          for (byte i = max_height_; i < height; ++i)
              prev[i] = head_;

      nvOffset x = NewNode(key, value, seq, isDeletion, height, prev);
      if (height > max_height_) {
          for (byte i = max_height_; i < height; ++i)
              SetNext(head_, i, x);
          max_height_ = height;
      }
      for (byte i = 0; i < height; ++i)
          SetNext(prev[i], i, x);
      return x;
  }
  void Update(nvOffset x, const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
      nvOffset old_v = GetValuePtr(x);
      nvOffset new_v = isDeletion ? nulloffset : NewValue(value);
      SetValuePtr(x, new_v);
      if (old_v == nulloffset) return;
      nvOffset size = DecodeFixed32(reinterpret_cast<const char*>(mem() + old_v));
      arena_.Dispose(old_v, size + 4);
  }
  void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) {
      //const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
      nvOffset prev[kMaxHeight];
      nvOffset x = head_;
      assert(x != nulloffset);
      nvOffset next = Seek(key, x, prev);
      if (next == nulloffset)
          Insert(key, value, seq, type == kTypeDeletion, prev);
      else
          Update(next, key, value, seq, type == kTypeDeletion);
  }
  bool Get(const LookupKey& lkey, std::string* value, Status* s) const {
      Slice key = lkey.user_key();
      nvOffset x = head_;
      assert(x != nulloffset);
      nvOffset next = Seek(key, x, nullptr);
      if (next != nulloffset) {
          Slice v = GetValue(next);
          if (v.size() > 0) {
              value->assign(v.data(), v.size());
              return true;
          } else {
              *s = Status::NotFound(Slice());
              return true;
          }
      }
      return false;
  }
  ull StorageUsage() const {
      return arena_.StorageUsage();
  }
    void GarbageCollection() {
        nvOffset l = arena_.Size();
        nvOffset value_bound = l;
        byte* buf = new byte[l];
        for (nvOffset x = GetNext(head_, 0); x != nulloffset; x = GetNext(x, 0)) {
            nvOffset v = GetValuePtr(x);
            if (v == nulloffset) continue;
            nvOffset vsize = ValueGetSize(v) + sizeof(nvOffset);
            value_bound -= vsize;
            memcpy(buf + value_bound, mem() + v, vsize);
            SetValuePtr(x, value_bound);
        }
        memcpy(arena_.Main() + value_bound, buf + value_bound, l - value_bound);
        arena_.SetBound(arena_.node_bound_, value_bound);
        delete [] buf;
    }
    void CheckValid() {
        Slice prev, key;
        for (byte level = 0; level < max_height_; ++level) {
            prev = Slice();
            for (nvOffset x = GetNext(head_,level); x != nulloffset; x = GetNext(x, level)) {
                key = GetKey(x);
                assert(key.compare(prev) > 0);
            }
        }
    }
  friend struct L2MemTable_D1SkipList;
  D1SkipList(const D1SkipList&) = delete;
  void operator=(const D1SkipList&) = delete;
  struct D1Iterator : Iterator {
      D1SkipList* table_;
      nvOffset x_;
      SequenceNumber seq_;
      DRAM_Buffer *buffer_;
      D1Iterator(D1SkipList* table, SequenceNumber seq) : table_(table), x_(nulloffset), seq_(seq),
          buffer_(new DRAM_Buffer(1 * MB)) {}
      virtual ~D1Iterator() {}

      // An iterator is either positioned at a key/value pair, or
      // not valid.  This method returns true iff the iterator is valid.
      virtual bool Valid() const {
          return x_ != nulloffset;
      }

      // Position at the first key in the source.  The iterator is Valid()
      // after this call iff the source is not empty.
      virtual void SeekToFirst() {
          x_ = table_->GetNext(table_->head_, 0);
      }

      // Position at the last key in the source.  The iterator is
      // Valid() after this call iff the source is not empty.
      virtual void SeekToLast() {
          x_ = table_->SeekToLast();
          if (!Valid()) return;
      }

      // Position at the first key in the source that is at or past target.
      // The iterator is Valid() after this call iff the source contains
      // an entry that comes at or past target.
      virtual void Seek(const Slice& target) {
          x_ = table_->head_;
          nvOffset prev = table_->Seek(target, x_, nullptr);
          if (prev == nulloffset)
              x_ = nulloffset;
      }

      // Moves to the next entry in the source.  After this call, Valid() is
      // true iff the iterator was not positioned at the last entry in the source.
      // REQUIRES: Valid()
      virtual void Next() {
          assert(Valid());
          x_ = table_->GetNext(x_, 0);
      }

      // Moves to the previous entry in the source.  After this call, Valid() is
      // true iff the iterator was not positioned at the first entry in source.
      // REQUIRES: Valid()
      virtual void Prev() {
          assert(false);
      }

      // Return the key for the current entry.  The underlying storage for
      // the returned slice is valid only until the next modification of
      // the iterator.
      // REQUIRES: Valid()
      Slice user_key() const {
          return table_->GetKey(x_);
      }

      virtual Slice key() const {
          assert(x_ != nulloffset);
          Slice key = user_key();
          nvOffset v = table_->GetValuePtr(x_);

          char* lkey_data = reinterpret_cast<char*>(buffer_->Allocate(key.size() + 8));
          memcpy(lkey_data, key.data(), key.size());

          if (v == nulloffset) {
              EncodeFixed64(lkey_data + key.size(), (seq_ << 8) | kTypeDeletion);
              //reinterpret_cast<ull*>(data + size) =
          } else {
              EncodeFixed64(lkey_data + key.size(), (seq_ << 8) | kTypeValue);
          }
          return Slice(lkey_data, key.size() + 8);
      }

      // Return the value for the current entry.  The underlying storage for
      // the returned slice is valid only until the next modification of
      // the iterator.
      // REQUIRES: Valid()
      virtual Slice value() const {
          assert(Valid());
          return table_->GetValue(x_);
      }

      // If an error has occurred, return it.  Else return an ok status.
      virtual Status status() const {
          return Status::OK();
      }
  };
  Iterator* NewOfficialIterator(SequenceNumber seq) {
      return new D1Iterator(this, seq);
  }
};

}

#endif // D1SKIPLIST_H
