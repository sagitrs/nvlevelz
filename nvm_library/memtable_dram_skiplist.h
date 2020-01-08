#pragma once
#ifndef MEMTABLE_DRAM_SKIPLIST_H
#define MEMTABLE_DRAM_SKIPLIST_H

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "skiplist_kvindram.h"
#include "dram_allocator.h"

namespace leveldb {

//class InternalKeyComparator;
//class Mutex;
//class MemTableIterator;

class MemTableII {
 public:
  typedef DramKV_Skiplist Table;
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTableII(const InternalKeyComparator& comparator) : table_(new Table) { }
  explicit MemTableII() : table_(new Table) { }
  explicit MemTableII(Table* table) : table_(table) {}

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  ll ApproximateMemoryUsage() {
      return table_->ApproximateMemoryUsage();
  }

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value){
      table_->Insert(key, value, seq, type == ValueType::kTypeDeletion);
  }

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s){
      bool result = table_->TryGet(key.memtable_key(), value);
      if (result && value == nullptr) {
          *s = Status::NotFound(Slice());
      }
      return result;
  }
  std::string GetMiddle(){
      return table_->GetMiddle();
  }

  DramKV_Skiplist* HandOver() {
      auto result = table_;
      table_ = nullptr;
      return result;
  }

 private:
  ~MemTableII(){
      delete table_;
  }  // Private since only Unref() should be used to delete it

  friend class MemTableIIIterator;
  friend class MemTableBackwardIterator;


  //KeyComparator comparator_;
  int refs_;
  Table *table_;

  // No copying allowed
  MemTableII(const MemTableII&);
  void operator=(const MemTableII&);
};
}  // namespace leveldb

#endif // MEMTABLE_DRAM_SKIPLIST_H
