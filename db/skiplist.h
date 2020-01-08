// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"
//#include "nvm_library/nvskiplist.h"
//#include "util/coding.h"

namespace leveldb {
class Arena;

template<typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);
  //explicit SkipList(Comparator cmp, Arena* arena, nvSkiplist::Iterator *iter);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  const Key& GetMiddle() const;
  void AppendStart();
  void Append(const Key& key);
  void AppendStop();

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;    // Arena used for allocations of nodes

  Node* const head_;
  Node* const tail_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list

  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  Random rnd_;

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
struct SkipList<Key,Comparator>::Node {
  explicit Node(const Key& k) : key(k) { }

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  port::AtomicPointer next_[1];
};

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
  return new (mem) Node(key);
}

template<typename Key, class Comparator>
inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = NULL;
}

template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != NULL;
}

template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {
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

template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == NULL || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator>
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      tail_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(reinterpret_cast<void*>(1)),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, NULL);
  }
}
/*
template<typename Key, class Comparator>
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena, nvSkiplist::Iterator *iter)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0, kMaxHeight)),
      max_height_(reinterpret_cast<void*>(1)),
      rnd_(0xdeadbeef) {
    for (int i = 0; i < kMaxHeight; i++) {
      head_->SetNext(i, NULL);
    }

    Node* tail = NewNode(0,kMaxHeight);
    for (int i = 0; i < kMaxHeight; i++) {
      tail->SetNext(i, NULL);
    }

    Node* x = nullptr;
    for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        //------------------------------------------------------------
        string key = iter->key();
        string value = iter->value();
        size_t key_size = key.size();
        size_t val_size = value.size();
        size_t internal_key_size = key_size + 8;
        ValueType type = (value.size() == 0 ? kTypeDeletion : kTypeValue);
        static const uint64_t s = 0;
        const size_t encoded_len =
            VarintLength(internal_key_size) + internal_key_size +
            VarintLength(val_size) + val_size;
        char* buf = arena_->Allocate(encoded_len);
        char* p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);
        p += key_size;
        EncodeFixed64(p, (s << 8) | type);
        p += 8;
        p = EncodeVarint32(p, val_size);
        memcpy(p, value.data(), val_size);
        assert((p + val_size) - buf == encoded_len);
        //-------------------------------------------------------------

        int height = RandomHeight();
        if (height > GetMaxHeight()) {
          for (int i = GetMaxHeight(); i < height; i++) {
            tail[i] = head_;
          }
          //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

          // It is ok to mutate max_height_ without any synchronization
          // with concurrent readers.  A concurrent reader that observes
          // the new value of max_height_ will see either the old value of
          // new level pointers from head_ (NULL), or a new value set in
          // the loop below.  In the former case the reader will
          // immediately drop to the next level since NULL sorts after all
          // keys.  In the latter case the reader will use the new node.
          max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
        }

        x = NewNode(buf, height);
        for (int i = 0; i < height; i++) {
          // NoBarrier_SetNext() suffices since we will add a barrier when
          // we publish a pointer to "x" in prev[i].
          x->NoBarrier_SetNext(i, tail[i]->NoBarrier_Next(i));
          tail[i]->SetNext(i, x);
        }
    }
    delete tail;
}
*/

template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (NULL), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since NULL sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, NULL);
  if (x != NULL && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

template<typename Key, class Comparator>
const Key& SkipList<Key,Comparator>::GetMiddle() const {
    int top = GetMaxHeight();
    std::vector<Node*> a;
    for (int i = top-1; i >= 0; --i){
        for (Node* x = head_->NoBarrier_Next(i); x; x = x->NoBarrier_Next(i))
            a.push_back(x);
        if (i == 0){
            assert(a.size() > 0);
            return a[a.size()/2]->key;
        }
        if (a.size() >= 10)
            return a[a.size()/2]->key;
        else
            a.clear();
    }
    assert(false);
}


template<typename Key, class Comparator>
void SkipList<Key,Comparator>::AppendStart() {
    for (int i = 0; i < kMaxHeight; ++i) {
        assert(head_->NoBarrier_Next(i) == nullptr);
        tail_->NoBarrier_SetNext(i, head_);
    }
//    tail_ = FindLast();
//    for (int i = GetMaxHeight(); i < kMaxHeight; ++i)
//        tail_[i] = head_;
}
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Append(const Key& key) {
    // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
    // here since Insert() is externally synchronized.
    //Node* prev[kMaxHeight];
    //Node* x = FindGreaterOrEqual(key, prev);

    // Our data structure does not allow duplicate insertion
    //assert(x == NULL || !Equal(key, x->key));
    Node* prev = tail_->NoBarrier_Next(0);
    assert(prev == head_ || compare_(prev->key, key) < 0);
    int height = RandomHeight();
    if (height > GetMaxHeight()) {
      max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
    }
    Node* x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
      Node* tail_i = tail_->NoBarrier_Next(i);
      x->NoBarrier_SetNext(i, tail_i->NoBarrier_Next(i));
      tail_i->SetNext(i, x);

      tail_->NoBarrier_SetNext(i, x);
    }
}
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::AppendStop() {
    Node* prev_x = head_->NoBarrier_Next(0);
    if (prev_x == nullptr ||
        prev_x->NoBarrier_Next(0) == nullptr) return;
    for (Node *x = prev_x->NoBarrier_Next(0); x != nullptr; x = x->NoBarrier_Next(0)) {
         assert (compare_(prev_x->key,x->key) < 0);
         prev_x = x;
    }
}


}  // namespace leveldb



#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
