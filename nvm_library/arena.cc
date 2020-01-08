#include "arena.h"
 NVM_Linear_Allocator::NVM_Linear_Allocator(NVM_Manager* mng, ul size) : mng_(mng), total_size_(size), dispose_when_destoryed_(true) {
    main_ = mng_->Allocate(total_size_);
    alloc_ptr_ = 0;
    alloc_bytes_remaining_ = total_size_;
 }

 void NVM_Linear_Allocator::DisposeAll() {
     mng_->Dispose(main_, total_size_);
 }

 void NVM_Linear_Allocator::Append(byte *buf, size_t size) {
     mng_->write(main_ + alloc_ptr_, buf, size);
     alloc_ptr_ += size;
 }

 void NVM_Linear_Allocator::Reset(byte* buf, nvOffset size) {
     mng_->write(main_, buf, size);
     alloc_ptr_ = size;
     alloc_bytes_remaining_ = total_size_ - size;
     puzzle_.clear();
 }


 ul NVM_Linear_Allocator::Allocate(ul size) {
     std::unordered_map<uint32_t,nvOffset>::iterator p = puzzle_.find(size);
     if (p != puzzle_.end()) {//assert(p->first == size);
         ul ans = p->second;
         nvOffset next = mng_->read_ul(main_ + ans);
         if (next == nulloffset)
             puzzle_.erase(p);
         else
             puzzle_[size] = next;
         return ans;
     }
     ul ans = alloc_ptr_;
     if (size > alloc_bytes_remaining_)
        return nulloffset;
     alloc_ptr_ += size;
     alloc_bytes_remaining_ -= size;
    return ans;
 }
 void NVM_Linear_Allocator::Dispose(nvOffset addr, ul size) {
    std::unordered_map<uint32_t,nvOffset>::iterator p = puzzle_.find(size);
    nvOffset next = p == puzzle_.end() ? nulloffset : p->second;
    mng_->write_ul(main_ + addr, next);
    puzzle_[size] = addr;
     //DisposeFullAddress(main_ + addr, size);
 }
