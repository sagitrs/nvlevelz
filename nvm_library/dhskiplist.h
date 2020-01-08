#ifndef DHSKIPLIST_H
#define DHSKIPLIST_H

#include "nvmemtable.h"
#include "d1skiplist.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include <string>
#include "port/port_posix.h"
#include "d2skiplist.h"

namespace leveldb {
struct DHMemTable : nvMemTable {
private:
    NVM_Manager* mng_;
    CachePolicy cp_;

    Random rnd_;
    int RandomHeight();



    std::string dbname_;
    ull seq_;
    ull pre_write_;
    ull written_size_;
    ull created_time_;

    int refs__;

    port::RWLock lock_;
    bool immutable_;

    std::string lft_bound_;
    std::string rgt_bound_;

    std::string MemName(const Slice& dbname, ull seq);
    std::string HashName(const Slice& dbname, ull seq);
    ull MaxSizeOf(size_t keysize, size_t valuesize) {
        return keysize + valuesize + (1 + 4 + 4 + 4 * kMaxHeight + 4);
    }
public:
    enum { kMaxHeight = 12 };

    DHMemTable(NVM_Manager* mng, const CachePolicy& cp, const Slice& dbname, ull seq);
    ~DHMemTable();
    void DeleteName();

    nvOffset Seek(const Slice& key);
    bool Get(const Slice& key, std::string* value, Status* s);

    void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);
    bool Get(const LookupKey& key, std::string* value, Status* s);
    void GarbageCollection();
    Iterator* NewIterator();
    Iterator* NewOfficialIterator(ull seq);

    void GetMid(std::string* key);
    void GetMin(std::string* key);
    void GetMax(std::string* key);

    double Garbage() const;
    ull StorageUsage() const;
    virtual ull DataWritten() const {
        return written_size_;
    }
    ull UpperStorage() const { return cache_.arena_.Size(); }
    ull LowerStorage() const { return table_->arena_.Size(); }

    nvAddr Location() const;

    bool HasRoomForWrite(const Slice& key, const Slice& value, bool nearly_full);
    virtual bool PreWrite(const Slice& key, const Slice& value, bool nearly_full);
    virtual bool HasRoomFor(ull size) const {
        return table_->HasRoomFor(size);
    }


    void Ref();
    void Unref();

    virtual bool Immutable() const { return immutable_; }
    virtual void SetImmutable(bool state) { immutable_ = state;  }

    void Print();
    void CheckValid();

    virtual ull BlankStorageUsage() const {
        return 89;
    }

    virtual std::string Max() {
        std::string s;
        GetMax(&s);
        return s;
    }
    virtual std::string Min() {
        std::string s;
        GetMin(&s);
        return s;
    }
    virtual void Lock() { lock_.WriteLock(); }
    virtual void Unlock() { lock_.Unlock(); }
    virtual void SharedLock() { lock_.ReadLock(); }
    virtual bool TryLock() {
        return lock_.TryWriteLock();
    }
    virtual bool TrySharedLock() {
        return lock_.TryReadLock();
    }
    std::string& LeftBound() { return lft_bound_; }
    std::string& RightBound() { return rgt_bound_; }
    SequenceNumber Seq() const { return seq_; }

    size_t CountOfLevel(byte level);
    virtual void FillKey(std::vector<std::string> &divider, size_t size);

    virtual nvMemTable* Rebuild(const Slice& lft, ull log_number) {
        D2MemTable* mem = new D2MemTable(mng_, cp_, dbname_, log_number);
        mem->LeftBound() = lft.ToString();
        //mem->Ref();
        return mem;
    }

    virtual void Connect(nvMemTable* b, bool reverse) {
        D2MemTable* d = static_cast<D2MemTable*>(b);
        assert(false);
    }

    virtual MemTable* Immutable(ull seq) { assert(false); }
    virtual ull& Paramenter(ParameterType type) {
        switch (type) {
        case ParameterType::CreatedNumber:
            return seq_;
        case ParameterType::CreatedTime:
            return created_time_;
        case ParameterType::PreWriteSize:
            return pre_write_;
        case ParameterType::WrittenSize:
            return written_size_;
        default:
            assert(false);
        }
    }

    // No copying allowed
    friend struct D2MemTableIterator;
    void operator=(const D2MemTable&) = delete;
};

struct D2MemTableIterator : public Iterator {
 public:
    D2MemTableIterator(D2MemTable* mem, ull seq = 0) :
        mem_(mem), buffer_(new DRAM_Buffer(8192)), seq_(seq), x_(nulloffset) {}
    virtual ~D2MemTableIterator() { delete buffer_; }

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const {
    return x_ != nulloffset;
  }

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() {
      x_ = mem_->table_->GetNext(mem_->table_->head_, 0);
  }

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() {
      x_ = mem_->table_->Tail();
  }

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) {
      x_ = mem_->Seek(target);
   }
  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() {
      x_ = mem_->table_->GetNext(x_, 0);
  }

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() {
      x_ = mem_->table_->GetPrev(x_);
  }

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  Slice user_key() const {
      return mem_->table_->GetKey(x_);
  }

  virtual Slice key() const {
      assert(x_ != nulloffset);
      Slice key = user_key();
      nvOffset v = mem_->table_->GetValuePtr(x_);

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
      return mem_->table_->GetValue(x_);
  }

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const {
      return Status();
  }

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.

 private:
  D2MemTable* mem_;
  DRAM_Buffer* buffer_;
  SequenceNumber seq_;
  nvOffset x_;

  D2MemTableIterator(const D2MemTableIterator&) = delete;
  void operator=(const D2MemTableIterator&) = delete;
};


}
#endif // DHSKIPLIST_H
