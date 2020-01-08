#pragma once

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "port/port.h"
#include "dram_allocator.h"
#include "nvm_library/nvm_manager.h"
#include <unordered_map>

struct NVM_Manager;

template<typename AddressType>
struct Linear_Allocator {
 public:
  // When Allocator has no space, call master_->ALlocate to obtain a new page.
  Linear_Allocator() {}
  // When destoryed, Allocator return all pages to it's master.
  virtual ~Linear_Allocator() {}

  // Allocate.
  virtual AddressType Allocate(size_t size) = 0;

  // Dispose.
  virtual void DisposeAll() = 0;

  virtual void Print(int level) = 0;
  // No copying allowed
  Linear_Allocator(const Linear_Allocator&) = delete;
  void operator=(const Linear_Allocator&) = delete;
};

class DRAM_Linear_Allocator : public Linear_Allocator<void*> {
 public:
  static const int kBlockSize = 4096;

  DRAM_Linear_Allocator() : memory_usage_(0) {
      alloc_ptr_ = nullptr;  // First allocation will allocate a block
      alloc_bytes_remaining_ = 0;
  }
  ~DRAM_Linear_Allocator() {
      DisposeAll();
  }

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  inline void* Allocate(size_t bytes) {
      // The semantics of what to return are a bit messy if we allow
      // 0-byte allocations, so we disallow them here (we don't need
      // them for our internal use).
      assert(bytes > 0);
      if (bytes <= alloc_bytes_remaining_) {
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
      }
      return AllocateFallback(bytes);
    }
  char* AllocateAligned(size_t bytes) {
      const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
      assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
      size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
      size_t slop = (current_mod == 0 ? 0 : align - current_mod);
      size_t needed = bytes + slop;
      char* result;
      if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
      } else {
        // AllocateFallback always returned aligned memory
        result = AllocateFallback(bytes);
      }
      assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
      return result;
    }

  // Allocate memory with the normal alignment guarantees provided by malloc
  void DisposeAll() {
      for (size_t i = 0; i < blocks_.size(); i++) {
          delete[] blocks_[i];
      }
      blocks_.clear();
  }

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return memory_usage_;
  }

 private:
  char* AllocateFallback(size_t bytes) {
      if (bytes > kBlockSize / 4) {
        // Object is more than a quarter of our block size.  Allocate it separately
        // to avoid wasting too much space in leftover bytes.
        char* result = AllocateNewBlock(bytes);
        return result;
      }

      // We waste the remaining space in the current block.
      alloc_ptr_ = AllocateNewBlock(kBlockSize);
      alloc_bytes_remaining_ = kBlockSize;

      char* result = alloc_ptr_;
      alloc_ptr_ += bytes;
      alloc_bytes_remaining_ -= bytes;
      return result;
  }

  char* AllocateNewBlock(size_t block_bytes) {
      char* result = new char[block_bytes];
      blocks_.push_back(result);
      memory_usage_ += block_bytes + sizeof(char*);
      return result;
  }

  void Print(int level = 0) {
      printf("Linear Allocator :\n");
      printf("%lu Blocks in use, %llu bytes in total.\n", blocks_.size(), memory_usage_);
      printf("%lu bytes in last block.\n", alloc_bytes_remaining_);
  }

  // Allocation state
  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  uint64_t memory_usage_;

  // No copying allowed
  DRAM_Linear_Allocator(const DRAM_Linear_Allocator&);
  void operator=(const DRAM_Linear_Allocator&);
};

class DRAM_Buffer : public Linear_Allocator<void*> {
 public:
  char* alloc_ptr_;
  char* old_alloc_ptr_;
  size_t alloc_bytes_used_, alloc_bytes_remaining_;
  const int kBlockSize;


  DRAM_Buffer(size_t size) :
      alloc_ptr_(nullptr), old_alloc_ptr_(nullptr),
      alloc_bytes_used_(0), alloc_bytes_remaining_(0), kBlockSize(size / 2) {
  }
  ~DRAM_Buffer() {
      DisposeAll();
  }

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  inline void* Allocate(size_t bytes) {
      assert(bytes > 0 && bytes <= kBlockSize);
      if (bytes <= alloc_bytes_remaining_) {
        char* result = alloc_ptr_ + alloc_bytes_used_;
        alloc_bytes_used_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
      }
      return AllocateFallback(bytes);
    }

  void DisposeAll() {
      if (alloc_ptr_)
          delete[] alloc_ptr_;
      if (old_alloc_ptr_)
          delete[] old_alloc_ptr_;
  }

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return kBlockSize * (old_alloc_ptr_ != nullptr) + alloc_bytes_used_;
  }

 private:
  char* AllocateFallback(size_t bytes) {
      AllocateNewBlock();

      char* result = alloc_ptr_;
      alloc_bytes_used_ += bytes;
      alloc_bytes_remaining_ -= bytes;
      return result;
  }

  char* AllocateNewBlock() {
      if (old_alloc_ptr_ != nullptr)
          delete[] old_alloc_ptr_;
      //assert(old_alloc_ptr_ == nullptr);
      old_alloc_ptr_ = alloc_ptr_;
      alloc_ptr_ = new char[kBlockSize];
      alloc_bytes_used_ = 0;
      alloc_bytes_remaining_ = kBlockSize;

      return alloc_ptr_;
  }

  void Print(int level = 0) {
  }

  // No copying allowed
  DRAM_Buffer(const DRAM_Buffer&);
  void operator=(const DRAM_Buffer&);
};

class NVM_Linear_Allocator  {
public:
 // When Allocator has no space, call master_->ALlocate to obtain a new page.
 NVM_Linear_Allocator(NVM_Manager* mng, ul size);
 // When destoryed, Allocator return all pages to it's master.
 ~NVM_Linear_Allocator() {
     if (dispose_when_destoryed_)
         DisposeAll();
 }

 // Allocate.
 nvAddr AllocateFullAddress(ul size) {
     ul offset = Allocate(size);
     if (offset == nulloffset) return nvnullptr;
     return main_ + offset;
 }
 ul Allocate(ul size);
 void Dispose(nvOffset addr, ul size);
 ul StorageUsage() const { return alloc_ptr_; }
 nvAddr Main() const { return main_; }
 size_t TotalUsage() const { return total_size_; }
 void SetDisposeMethod(bool dispose_when_destoryed) {
     dispose_when_destoryed_ = dispose_when_destoryed;
 }
 void Append(byte *buf, size_t size);
 void Reset(byte* buf, nvOffset size);
 // Dispose.
 virtual void DisposeAll();

 virtual void Print(int level) {

 }
 // No copying allowed
 NVM_Linear_Allocator(const NVM_Linear_Allocator&) = delete;
 void operator=(const NVM_Linear_Allocator&) = delete;

private:
 NVM_Manager* mng_;
 nvAddr main_;
 const size_t total_size_;
 nvOffset alloc_ptr_;
 size_t alloc_bytes_remaining_;
 bool dispose_when_destoryed_;

 std::unordered_map<uint32_t,nvOffset> puzzle_;
};
