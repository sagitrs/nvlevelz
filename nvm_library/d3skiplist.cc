#include "d3skiplist.h"
#include "leveldb/iterator.h"

namespace leveldb {

Iterator* D3MemTable::NewIterator() { return new D3MemTableIterator(this); }

}
