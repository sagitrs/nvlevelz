#pragma once
#include "global.h"
#include <stdlib.h>
#include "nvm_options.h"
#include <stdio.h>
#include <stdint.h>
#include <dlfcn.h>
#include <string.h>
#include <stdint.h>
#include <dlfcn.h>

typedef void (*Cleaner)(byte*);

#define MaxBlockSize (512 * MB)
//@@@@@
#define NVDIMM_ENABLED
//#define NVDIMM_ENABLED
#define NO_READ_DELAY
struct NVM_MemoryBlock;

void sys_nvm_dispose(NVM_MemoryBlock* block);
NVM_MemoryBlock* sys_nvm_allocate(ull size, Cleaner cl);

struct NVM_MemoryBlock {
    struct MemoryBlock {
        byte* main_;
        ull size_;
        MemoryBlock() : main_(nullptr), size_(0) {}
        MemoryBlock(byte* main, ull size) : main_(main), size_(size) {}
    };
    MemoryBlock *block_;
    //byte** block_;
    size_t block_num_;

    MemoryBlock global_;
    ull Size() const { return global_.size_; }
    byte* Main() const { return global_.main_; }
//    ull size_;
  NVM_MemoryBlock(MemoryBlock* block, size_t block_num)
      : block_(block), block_num_(block_num), global_(nullptr, 0)
  {
      global_.main_ = block_[0].main_;
      global_.size_ = block_[0].size_;
#ifdef NVDIMM_ENABLED
      assert(block_num_ == 1);
#endif
      for (ull i = 0; i < block_num; ++i)
          global_.size_ += block_[i].size_;
  }
  inline void* operator[] (nvAddr addr) {
      return reinterpret_cast<byte*>(addr);
  }
  inline void* Decode(nvAddr addr) {
      return reinterpret_cast<byte*>(addr);
  }
  void Dispose() {
      sys_nvm_dispose(this);
      delete [] block_;
  }
};
