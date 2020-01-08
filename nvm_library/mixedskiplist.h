#ifndef MIXEDSKIPLIST_H
#define MIXEDSKIPLIST_H
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/iterator.h"
#include "global.h"

namespace leveldb {

struct MixedSkipList {
    virtual ~MixedSkipList() = 0;
    virtual void Add(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) = 0;
    virtual bool Get(const Slice& key, std::string* value, Status* s) = 0;

    virtual void Connect(MixedSkipList* b, bool reverse) = 0;
    virtual void GarbageCollection() = 0;

    virtual Iterator* NewIterator() = 0;
    virtual Iterator* NewOfficialIterator(ull seq) = 0;

    virtual void SetCommonSize(size_t common_size) = 0;
    virtual void GetMiddle(std::string& key) = 0;

    virtual double Garbage() const = 0;
    virtual ll StorageUsage() const = 0;
    virtual ull UpperStorage() const = 0;
    virtual ull LowerStorage() const = 0;
    virtual nvAddr Location() const = 0;
    virtual bool HasRoomForWrite(const Slice& key, const Slice& value) = 0;

    virtual void CheckValid() = 0;
    virtual void Print() = 0;
};

}


#endif // MIXEDSKIPLIST_H
