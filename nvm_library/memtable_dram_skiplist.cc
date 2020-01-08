#include "memtable_dram_skiplist.h"

namespace leveldb {

class MemTableIIIterator: public Iterator {
 public:
  explicit MemTableIIIterator(MemTableII::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(k); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return iter_.key().ToSlice(); }
  virtual Slice value() const {
        StaticSlice *v = iter_.valuePtr();
        if (v){
            return Slice(v->data(),v->size());
        } else
            return Slice();
        //return &v == nullptr ? Slice() : v.ToSlice();
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTableII::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIIIterator(const MemTableIIIterator&);
  void operator=(const MemTableIIIterator&);
};

Iterator* MemTableII::NewIterator() {
  return new MemTableIIIterator(table_);
}

};
