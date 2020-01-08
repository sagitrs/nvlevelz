#ifndef INDEX_H
#define INDEX_H
#include "leveldb/slice.h"
#include "leveldb/iterator.h"
#include <string>
namespace leveldb {
struct nvMemTable;
struct IndexIterator : public Iterator {
    virtual nvMemTable* Data() const = 0;

};

struct AbstractIndex {
public:
    virtual ~AbstractIndex();

    virtual void Add(const Slice& key, nvMemTable* data) = 0;

    virtual bool Delete(const Slice& key, nvMemTable* *data = nullptr) = 0;

    virtual bool Get(const Slice& key, nvMemTable* *data) = 0;

    virtual nvMemTable* FuzzyFind(const Slice& key) = 0;

    virtual void FuzzyLeftBoundary(const Slice& key, std::string* value) = 0;
    virtual void FuzzyRightBoundary(const Slice& key, std::string* value) = 0;

    virtual IndexIterator* NewIterator() = 0;

};
}

#endif // INDEX_H
