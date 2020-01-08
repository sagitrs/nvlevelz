#include "nvmemtable.h"

namespace leveldb {

AbstractMemTable::~AbstractMemTable() {}
nvMemTable::~nvMemTable() {
    //printf("Destory a MemTable.\n");
    //fflush(stdout);
}

nvOffset nvMemTable::FindNode(const Slice& key) {
    return nulloffset;
}
void nvMemTable::Update(nvOffset node, SequenceNumber seq, ValueType type, const Slice& value) {
    assert(false);
}

/*
const L2MemTable::DefaultComparator L2MemTable::cmp_;

// Increase reference count.
void L2MemTable::Ref() { ++refs_; }

// Drop reference count.  Delete if no more references exist.
void L2MemTable::Unref() {
  --refs_;
  assert(refs_ >= 0);
  if (refs_ <= 0) {
    //printf("Dispose L2MemTable : [%s]", lft_bound_.c_str());
    //fflush(stdout);
    delete this;
  }
}

void L2MemTable::Connect(nvMemTable* b, bool reverse) {
    //string a_min = this->list_->Min();
    //string b_min = b->list_->Min();
    //string a_max = this->list_->Max();
    //string b_max = b->list_->Max();
    //this->list_->CheckValid();
    //b->list_->CheckValid();
    //if (reverse) {
    //    assert(Slice(b_max).compare(a_min) < 0);
    //} else {
    //    assert(Slice(a_max).compare(b_min) < 0);
    //}
    this->list_->Connect(static_cast<L2MemTable*>(b)->list_, reverse);
}
*/
}
