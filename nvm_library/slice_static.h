#ifndef SLICE_STATIC_H
#define SLICE_STATIC_H

#include <string>
#include <cstdlib>
#include <string.h>
#include <assert.h>
#include "include/leveldb/slice.h"
namespace leveldb {
class StaticSlice : private Slice {
 public:
  // Create an empty slice.
  StaticSlice() : size_(0), data_(nullptr) { }

  // Create a slice that refers to d[0,n-1].
  StaticSlice(const char* d, size_t n) : size_(n), data_(size_ == 0 ? nullptr : new char[size_]) {
      if (size_) memcpy(data_,d,size_);
  }

  // Create a slice that refers to the contents of "s"
  StaticSlice(const std::string& s) : size_(s.size()), data_(size_ == 0 ? nullptr : new char[size_]) {
      if (size_) memcpy(data_,s.c_str(),size_);
  }

  // Create a slice that refers to s[0,strlen(s)-1]
  StaticSlice(const char* s) : size_(strlen(s)), data_(size_ == 0 ? nullptr : new char[size_]) {
      if (size_) memcpy(data_, s, size_);
  }

  // Intentionally copyable.
  StaticSlice(const StaticSlice& s) : size_(s.size()), data_(size_ == 0 ? nullptr : new char[size_]) {
      if (size_) memcpy(data_, s.data(), size_);
  }
  // Create a copy of the contents of "s"
  StaticSlice(const Slice& s) : size_(s.size()), data_(size_ == 0 ? nullptr : new char[size_]) {
      if (size_) memcpy(data_, s.data(), size_);
  }

  ~StaticSlice() {
      clear();
  }
/*
  StaticSlice& operator=(const StaticSlice& s) {
      clear();
      if (s.size() == 0)
          return *this;
      data_ = new char[size_];
      memcpy(data_,s.data(),size_);
      size_ = s.size();
      return *this;
  }

  StaticSlice& operator=(const Slice& s) {
      clear();
      if (s.size() == 0)
          return *this;
      data_ = new char[size_];
      memcpy(data_,s.data(),size_);
      size_ = s.size();
      return *this;
  }
*/
  StaticSlice& operator=(const StaticSlice&) = delete;
  // Return a pointer to the beginning of the referenced data
  const char* data() const { return (data_ == nullptr? "" : data_); }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {
      char* tmp = data_;
      size_ = 0;
      data_ = nullptr;
      delete[] tmp;
  }

  // Return a string that contains the copy of the referenced data.
  std::string ToString() const { return std::string(data_, size_); }
  const leveldb::Slice ToSlice() const { return Slice(data_, size_); }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const StaticSlice& b) const;
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const StaticSlice& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_, x.data_, x.size_) == 0));
  }
private:
  size_t size_;
  char* data_;
};

inline bool operator==(const StaticSlice& x, const StaticSlice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const StaticSlice& x, const StaticSlice& y) {
  return !(x == y);
}

inline bool operator==(const StaticSlice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const StaticSlice& x, const Slice& y) {
  return !(x == y);
}
/*
inline bool operator==(const Slice& x, const StaticSlice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const StaticSlice& y) {
  return !(x == y);
}
*/

inline int StaticSlice::compare(const StaticSlice& b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}

inline int StaticSlice::compare(const Slice& b) const {
  const size_t min_len = (size_ < b.size()) ? size_ : b.size();
  int r = memcmp(data_, b.data(), min_len);
  if (r == 0) {
    if (size_ < b.size()) r = -1;
    else if (size_ > b.size()) r = +1;
  }
  return r;
}
};
#endif // SLICE_STATIC_H
