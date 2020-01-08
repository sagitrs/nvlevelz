#include "nvm_allocator.h"


  // When Allocator has no space, call master_->ALlocate to obtain a new page.
  NVM_PuzzleAllocator::NVM_PuzzleAllocator(NVM_Allocator* master, NVM_Manager * io, const NVM_Options & options) :
      Allocator(),
      Page(options.page_size),
      Division(8),
      BlockTypes(Page / Division),
      master_(master),
      head_(new nvAddr[BlockTypes]),
      //locks_(new std::mutex[BlockTypes]),
      blocks_(),
      io_(io) {
      for (int i = 0; i < BlockTypes; ++i){
          head_[i] = nvnullptr;
      }
      //main_ = master_->Main();
  }
  // When destoryed, Allocator return all pages to it's master.
  NVM_PuzzleAllocator::~NVM_PuzzleAllocator() {
      for (auto i = blocks_.begin(); i != blocks_.end(); ++i)
          master_->Dispose(*i,Page);
      delete[] head_;
      //delete[] locks_;
  }

  // Allocate.
  nvAddr NVM_PuzzleAllocator::Allocate(size_t size) {
      assert(0 < size && size < Page);
      ull p = (size-1) / Division;
      ull real_size = Division * (p+1);
      //locks_[p].lock();
      if (head_[p] == nvnullptr) {
          nvAddr page = master_->Allocate(1 * Page);
          if (page == nvnullptr){
              printf("Error in NVM Puzzle Allocator: Master has no enough space.\n");
              //locks_[p].unlock();
              return nvnullptr;
          }
          blocks_.push_back(page);
          ull i;
          const ull bound = Page - 2 * real_size;
          for (i = 0; i <= bound; i += real_size) {
              nvAddr next = page + i + real_size;
              io_->write_addr(page+i, next);
          }
          io_->write_addr(page+i, nvnullptr);
          head_[p] = page;
          //if (head_[p] % 8 != 0 || head_[p] > pow2(1,32)){
          //    printf("Wait.");
          //}
      }
      assert(head_[p] != nvnullptr);
      nvAddr ans = head_[p];
      //if (ans % 8 != 0 || ans > pow2(1,32)){
      //    printf("Wait.");
      //}
      head_[p] = io_->read_addr(ans);
      //if (head_[p] % 8 != 0 || head_[p] > pow2(1,32)){
      //    printf("Ans = %llu, head[%d] = %llu\n.",ans,p,head_[p]);
      //}
      //locks_[p].unlock();
      return ans;
  }

  // Dispose.
  void NVM_PuzzleAllocator::Dispose(nvAddr ptr, size_t size) {
      assert(0 < size && size < Page);
      //if (ptr % 8 != 0){
      //    printf("Wait.");
      //}
      if (ptr == nvnullptr) return;
      int p = (size-1) / Division;
      int real_size = Division * (p+1);
      //locks_[p].lock();
      io_->write_addr(ptr, head_[p]);
      head_[p] = ptr;
      //locks_[p].unlock();
  }

  void NVM_PuzzleAllocator::Print(int level) {
      printbyte(' ',level);
      printf("Block Avaliable For :");
      for (int i = 0; i < BlockTypes; ++i) if (head_[i] != nvnullptr){
          int count = 0;
          for (nvAddr p = head_[i]; p != nvnullptr; p = io_->read_addr(p)) count ++;
           printf("%d(%d),",i,count);
      }
      printf("\n");
  }
