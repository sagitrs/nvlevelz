#ifndef NVM_ALLOCATOR_H
#define NVM_ALLOCATOR_H
#include "allocator.h"
#include "skiplist_dynamic.h"
#include "dram_allocator.h"
//include "allocator_main_dram.h"
#include "global.h"
#include "nvm_options.h"
#include "nvm_directio_manager.h"
#include <mutex>
#include <unordered_map>
#include <pthread.h>
#include "nvm_manager.h"

struct NVM_Manager;

struct MemoryBlock {
    byte level_;
    nvAddr addr_offset_;
    MemoryBlock() : level_(128), addr_offset_(0) {}
    MemoryBlock(nvAddr addr) : level_(128), addr_offset_(addr) {}
    MemoryBlock(byte level, nvAddr addr) : level_(level), addr_offset_(addr) {}
    int compare(const MemoryBlock& b) const {
        if (level_ != b.level_) return level_ < b.level_ ? -1:1;
        if (addr_offset_ != b.addr_offset_) return addr_offset_ < b.addr_offset_ ? -1:1;
        return 0;
    }
    std::string ToString() const {
        static char buf[256] = {};
        sprintf(buf,"(%d,%llx)",level_,addr_offset_);
        return std::string(buf);
    }
};

struct MemoryBlockComparator {
  virtual int operator()(const MemoryBlock& a, const MemoryBlock& b) const {
      return a.compare(b);
  }
};


struct NVM_BuddyAllocator : public NVM_Allocator {
public:
    typedef DynamicSkipList<MemoryBlock,MemoryBlockComparator> MemoryBlockTable;
private:
    //byte* main_;
    //ull size_;
    NVM_MemoryBlock* main_blocks_;
    MemoryBlockComparator cmp_;
    DRAM_MainAllocator dram_allocator_;
    MemoryBlockTable table_;
    std::mutex mutex_;

    const byte MaxLevel;
    const size_t Page;
    const nvAddr BasicOffset;
    nvAddr Buddy(nvAddr addr, byte level) {
        if (NV_PAGE(addr) == 0)
            return ((addr - BasicOffset) ^ (1 << level)) + BasicOffset;
        else
            return addr ^ (1 << level);
    }
public:
    NVM_BuddyAllocator(const NVM_Options& option) :
        //main_(option.main),
        //size_(option.main_size),
        main_blocks_(option.block),
        // the first space of nvAddr records address of name_book.
        // this is why nvAddr couldn't be zero, why zero is an invalid address.
        cmp_(),
        dram_allocator_(),
        table_(cmp_,&dram_allocator_),
        MaxLevel(option.max_level),
        Page(option.page_size),
        BasicOffset(option.basic_offset),
        Allocator()
    {

        for (size_t block = 0; block < main_blocks_->block_num_; ++block) {
            ull offset = block == 0 ? BasicOffset : 0;
            byte* main_ = main_blocks_->block_[block].main_;
            ull size_ = main_blocks_->block_[block].size_ - offset;
            //ull page_part = (1ULL * block) << 32;
            for (char l = log2_downfit(size_); l >= 0; l--) {
                //printf("%d ",l);
                ull i = pow2(1, l);
                if (i & size_) {
                    //ull check = basic_offset & (i-1);
                    //assert(check == 0);
                    table_.Insert(MemoryBlock(static_cast<byte>(l), reinterpret_cast<nvAddr>(main_ + offset)));
                    offset += i;
                }
            }
        }
    }

    virtual nvAddr Allocate(size_t size){
        assert(Page <= size);
        mutex_.lock();
        nvAddr result = Allocate_(log2_upfit(size));
        mutex_.unlock();
        return result;
    }
    nvAddr Allocate_(byte level){
        if (level >= MaxLevel){
            printf("Error : Space not enough in NVM-Buddy-Allocator.\n");
            assert(false);
            return nvnullptr;
        }
        MemoryBlock user_key(level,nvnullptr);
        MemoryBlockTable::Iterator iter(&table_);
        iter.Seek(user_key);
        if (!iter.Valid() || iter.key().level_ != level){
            nvAddr high_block = Allocate_(level + 1);
            table_.Insert(MemoryBlock(level, high_block + (1ULL<<level)));
            return high_block;
        }
        nvAddr result = iter.key().addr_offset_;
        table_.Delete(iter.key());
        return result;
    }

    virtual void Dispose(nvAddr ptr, size_t size){
        assert(ptr != nvnullptr && size > 0);
        byte level = log2_upfit(size);
        mutex_.lock();
        Dispose_(ptr, level);
        mutex_.unlock();
    }
    void Dispose_(nvAddr ptr, byte level){
        nvAddr buddy = Buddy(ptr,level);
        //mutex_.lock();
        if (table_.Delete(MemoryBlock(level,buddy)))
            Dispose_((buddy < ptr ? buddy : ptr) , level + 1);
            //table_.Insert(MemoryBlock(level+1,(buddy < ptr ? buddy : ptr)));
        else
            //assert(static_cast<int>(ptr & (pow2(1,level)-1)) == 0);
            table_.Insert(MemoryBlock(level,ptr));
        //mutex_.unlock();
    }

    virtual void Print(int level = 0){
        printbyte(' ',level);printf("NVM Buddy Allocator:\n");
        printbyte(' ',level + 2);
        MemoryBlockTable::Iterator iter(&table_);
        for (iter.SeekToFirst();iter.Valid();iter.Next()) {
            printf("%s,",iter.key().ToString().c_str());
        }
        printf("\n");
    }

    // No copying allowed
    NVM_BuddyAllocator(const NVM_BuddyAllocator&) = delete;
    void operator=(const NVM_BuddyAllocator&) = delete;
};

struct NVM_PuzzleAllocator : public NVM_Allocator {
private:
    const size_t Page;
    const size_t Division;
    const size_t BlockTypes;
    NVM_Allocator * master_;
    nvAddr *head_;   // head_[x] -> head of space of 8*(x+1)
    //std::mutex *locks_;
    std::vector<nvAddr> blocks_;
    NVM_Manager* io_;
 public:
  // When Allocator has no space, call master_->ALlocate to obtain a new page.
  NVM_PuzzleAllocator(NVM_Allocator* master, NVM_Manager* io, const NVM_Options & options);
  // When destoryed, Allocator return all pages to it's master.
  virtual ~NVM_PuzzleAllocator();

  // Allocate.
  virtual nvAddr Allocate(size_t size);
  // Dispose.
  virtual void Dispose(nvAddr ptr, size_t size);
  virtual void Print(int level = 0);

  // No copying allowed
  NVM_PuzzleAllocator(const NVM_PuzzleAllocator&) = delete;
  void operator=(const NVM_PuzzleAllocator&) = delete;
};

class NVM_MainAllocator : public NVM_Allocator {
private:
    NVM_Options options_;
    NVM_Manager * io_;
    const size_t Page;
    std::mutex mutex_;
    NVM_BuddyAllocator * large_allocator_;
    //NVM_PuzzleAllocator * small_allocator_;
    std::unordered_map<pthread_t, NVM_PuzzleAllocator*> thread_allocator_;
    long long used_;
    long long rest_;
public:
  // StandardAllocator is system allocator. it dispose and allocate by malloc()/free(), not by master.
  NVM_MainAllocator(const NVM_Options& options, NVM_Manager * io) :
      Allocator(),
      options_(options),io_(io),
      Page(options.page_size),
      mutex_(),
      large_allocator_(new NVM_BuddyAllocator(options)),
      thread_allocator_(),
      used_(0), rest_(options.block->Size() - 8)
      //small_allocator_(new NVM_PuzzleAllocator(large_allocator_, io, options))
  {}
  // When destoryed, Allocator return all pages to it's master.
  virtual ~NVM_MainAllocator() {
      //delete small_allocator_;
      for (auto i = thread_allocator_.begin(); i != thread_allocator_.end(); ++i)
          delete i->second;
//      large_allocator_->Print();
      delete large_allocator_;
  }

  // Allocate.
  virtual nvAddr Allocate(size_t size) {
      assert(0 < size);
      //assert(mutex_.try_lock());
      nvAddr result;
      //mutex_.lock();
      if (size < Page){
          pthread_t pid = pthread_self();
          auto i = thread_allocator_.find(pid);
          if (i == thread_allocator_.end()){
              thread_allocator_[pid] = new NVM_PuzzleAllocator(large_allocator_, io_, options_);
              i = thread_allocator_.find(pid);
          }
          result = i->second->Allocate(size);
      }
          //result = small_allocator_->Allocate(size);
      else{
          result = large_allocator_->Allocate(size);
      }
      //mutex_.unlock();
      rest_ -= size;
      used_ += size;
      assert(result != nvnullptr);
      return result;
  }

  // Dispose.
  virtual void Dispose(nvAddr ptr, size_t size) {
      assert(size >= 0);
      if (size == 0) return;
      //mutex_.lock();
      if (size < Page){
          pthread_t pid = pthread_self();
          auto i = thread_allocator_.find(pid);
          if (i == thread_allocator_.end()){
              thread_allocator_[pid] = new NVM_PuzzleAllocator(large_allocator_, io_, options_);
              i = thread_allocator_.find(pid);
          }
          i->second->Dispose(ptr, size);
      }
          //small_allocator_->Dispose(ptr, size);
      else{
          large_allocator_->Dispose(ptr, size);
      }
      //mutex_.unlock();
      rest_ += size;
      used_ -= size;
  }

  long long StorageUsage() const { return used_; }

  virtual void Print(int level = 0) {
      printbyte(' ',level);printf("Large Allocator:\n");
      large_allocator_->Print(level+2);
      printbyte(' ',level);printf("Small Allocator:\n");
      //small_allocator_->Print(level+2);
      for (auto i = thread_allocator_.begin(); i != thread_allocator_.end(); ++i)
          i->second->Print(level+2);
  }

  // No copying allowed
  NVM_MainAllocator(const NVM_MainAllocator&) = delete;
  void operator=(const NVM_MainAllocator&) = delete;
};

#endif // NVM_ALLOCATOR_H
