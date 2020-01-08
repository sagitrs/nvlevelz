#ifndef NODETABLE_H
#define NODETABLE_H

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "nvm_library/skiplist_linear.h"
#include "nvm_library/arena.h"

namespace leveldb {
/*
class NodeTable {
// [NodeAddr = 8][ValueAddr = 8][ValueLength = 8][key_size = varint32][key bytes = x]
 public:
  explicit NodeTable(DRAM_Linear_Allocator* arena) :
        comparator_(), arena_(arena), table_(arena) {}
    ~NodeTable() {}

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
    void Add(const Slice& key, const L2MemTableInfo& info) {
        char* key_data = static_cast<char*>(arena_->Allocate(key.size()));
        memcpy(key_data,key.data(),key.size());
        table_.Insert(Slice(key_data, key.size()), info);
    }

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
    bool Get(const Slice& key, L2MemTableInfo& info) {
        return table_.TryGet(key, info);
    }

 private:

  struct NodeComparator {
      explicit NodeComparator() { }
      int operator()(const Slice& a, const Slice& b) const {
        return a.compare(b);
      }
  };

  typedef LinearSkiplist Table;

  NodeComparator comparator_;
  DRAM_Linear_Allocator* arena_;
  Table table_;

  // No copying allowed
  NodeTable(const NodeTable&);
  void operator=(const NodeTable&);
};
*/
}  // namespace leveldb

#endif // NODETABLE_H
