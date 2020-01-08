#ifndef DRAM_ALLOCATOR_H
#define DRAM_ALLOCATOR_H
#include "allocator.h"
#include <vector>
#include <string.h>
using std::vector;

typedef Allocator<void*> DRAM_Allocator;

class DRAM_StandardAllocator : public DRAM_Allocator {
public:
  // DRAM_StandardAllocator is system allocator. it dispose and allocate by malloc()/free(), not by master.
  DRAM_StandardAllocator() : Allocator() {}
  // When destoryed, Allocator return all pages to it's master.
  virtual ~DRAM_StandardAllocator() {}

  // Allocate.
  virtual void* Allocate(size_t size) {
      return malloc(size);
  }

  // Dispose.
  virtual void Dispose(void* ptr, size_t size) {
      free(ptr);
  }

  virtual void Print(int level = 0) {
      printbyte(' ', level);
      printf("Standard Allocator.\n");
  }

  // No copying allowed
  DRAM_StandardAllocator(const DRAM_StandardAllocator&) = delete;
  void operator=(const DRAM_StandardAllocator&) = delete;
};

struct DRAM_PuzzleAllocator : public DRAM_Allocator {
private:
    static const int Page = 4096;
    byte* head_[512];   // head_[x] -> head of space of 8*(x+1)
    std::vector<byte*> blocks_;
    DRAM_Allocator * master_;
 public:
  // When Allocator has no space, call master_->ALlocate to obtain a new page.
  DRAM_PuzzleAllocator(DRAM_Allocator* master) : Allocator(), blocks_(), master_(master) {
      for (int i = 0; i < 512; ++i){
          head_[i] = nullptr;
      }
  }
  // When destoryed, Allocator return all pages to it's master.
  virtual ~DRAM_PuzzleAllocator() {
      for (auto i = blocks_.begin(); i != blocks_.end(); ++i)
          master_->Dispose(*i,Page);
  }

  // Allocate.
  virtual void* Allocate(size_t size) {
      assert(0 < size && size < Page);
      int p = size / 8;
      int real_size = 8 * (p+1);
      if (head_[p] == nullptr) {
          void* page_addr = master_->Allocate(1 * Page);
          byte* page = static_cast<byte*>(page_addr);
          if (page == nullptr){
              printf("Error in Puzzle Allocator: Master has no enougt space.\n");
              return nullptr;
          }
          blocks_.push_back(page);
          int i;
          for (i = 0; i + real_size < Page; i += real_size) {
              byte* next = page + i + real_size;
              memcpy(page+i,&next,sizeof(byte*));
          }
          byte* next = nullptr;
          memcpy(page+i,&next,sizeof(byte*));
          head_[p] = page;
      }
      assert(head_[p] != nullptr);
      byte* ans = head_[p];
      memcpy(head_+p,ans,sizeof(byte*));
      return ans;
  }

  // Dispose.
  virtual void Dispose(void* ptr, size_t size) {
      assert(0 < size && size < Page);
      if (ptr == nullptr) return;
      int p = size / 8;
      int real_size = 8 * (p+1);
      byte * byte_ptr = static_cast<byte*>(ptr);
      byte* next = head_[p];
      memcpy(byte_ptr,&next,sizeof(byte*));
      //*byte_ptr = head_[p];
      head_[p] = byte_ptr;
  }

  virtual void Print(int level = 0) {
      printbyte(' ',level);
      printf("Block Avaliable For :");
      for (int i = 0; i < 512; ++i) if (head_[i]){
           printf("%d,",i);
      }
      printf("\n");
  }

  // No copying allowed
  DRAM_PuzzleAllocator(const DRAM_PuzzleAllocator&) = delete;
  void operator=(const DRAM_PuzzleAllocator&) = delete;
};

class DRAM_MainAllocator : public DRAM_Allocator {
private:
    static const int Page = 4096;
    DRAM_StandardAllocator * large_allocator_;
    DRAM_PuzzleAllocator * small_allocator_;
public:
  // StandardAllocator is system allocator. it dispose and allocate by malloc()/free(), not by master.
  DRAM_MainAllocator() :
      large_allocator_(new DRAM_StandardAllocator()),
      small_allocator_(new DRAM_PuzzleAllocator(large_allocator_)),
      Allocator() {}
  // When destoryed, Allocator return all pages to it's master.
  virtual ~DRAM_MainAllocator() {
      delete small_allocator_;
      delete large_allocator_;
  }

  // Allocate.
  virtual void* Allocate(size_t size) {
      assert(0 < size);
      if (size < Page)
          return small_allocator_->Allocate(size);
      else
          return large_allocator_->Allocate(size);
  }

  // Dispose.
  virtual void Dispose(void* ptr, size_t size) {
      assert(0 < size);
      if (size < Page)
          return small_allocator_->Dispose(ptr, size);
      else
          return large_allocator_->Dispose(ptr, size);
  }

  virtual void Print(int level = 0) {
      printbyte(' ',level);printf("Large Allocator:\n");
      large_allocator_->Print(level+2);
      printbyte(' ',level);printf("Small Allocator:\n");
      small_allocator_->Print(level+2);
  }

  // No copying allowed
  DRAM_MainAllocator(const DRAM_MainAllocator&) = delete;
  void operator=(const DRAM_MainAllocator&) = delete;
};

#endif // DRAM_ALLOCATOR_H
