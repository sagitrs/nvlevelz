#ifndef ALLOCATOR_H
#define ALLOCATOR_H
#include <stddef.h>
#include "global.h"

template<typename AddressType>
struct Allocator {
 public:
  // When Allocator has no space, call master_->ALlocate to obtain a new page.
  Allocator() {}
  // When destoryed, Allocator return all pages to it's master.
  virtual ~Allocator() {}

  // Allocate.
  virtual AddressType Allocate(size_t size) = 0;

  // Dispose.
  virtual void Dispose(AddressType ptr, size_t size) = 0;

  virtual void Print(int level) = 0;
  // No copying allowed
  Allocator(const Allocator&) = delete;
  void operator=(const Allocator&) = delete;
};

typedef Allocator<nvAddr> NVM_Allocator;
typedef Allocator<void*> DRAM_Allocator;


#endif // ALLOCATOR_H
