#ifndef NVQUEUE_H
#define NVQUEUE_H
#include "nvm_manager.h"
#include "leveldb/slice.h"

namespace leveldb {

struct nvQueue {
    NVM_Manager* mng_;
    nvAddr main_;
    nvAddr st_;
    nvOffset n_;
    nvOffset head_, tail_;
    enum { SizeOffset = 0, HeadOffset = 8, TailOffset = 16, TotalLength = 24 };
    nvOffset StorageUsage() const {
        ll head = head_, tail = tail_;
        if (tail > head)
            return static_cast<nvOffset>(tail - head);
        else
            return static_cast<nvOffset>(tail + n_ - head);
    }
    nvQueue(NVM_Manager* mng, nvOffset size) :
        mng_(mng),
        main_(mng_->Allocate(TotalLength + size)), st_(main_ + TotalLength),
        n_(size), head_(0), tail_(size - 1) {}
    bool push_back(const Slice& key) {
        // for writer, tail will not change.
        if (key.size() + StorageUsage() >= n_) {  // should be '>'.
            assert(false);
            return false;
        }
        if (tail_ + key.size() < n_) {
            mng_->write(st_ + tail_, reinterpret_cast<const byte*>(key.data()), key.size());
        } else {
            nvOffset a = n_ - tail_, b;
            mng_->write(st_ + tail_, reinterpret_cast<const byte*>(key.data()), a);
            mng_->write(st_ + 0,     reinterpret_cast<const byte*>(key.data() + a), key.size() - a);
        }
        MoveTail(key.size());
        return true;
    }
    bool pop_front(char* key, size_t size) {
        // for reader, head will not change.
        if (StorageUsage() < size) {
            assert(false);
            return false;
        }
        if (head_ + size < n_) {
            mng_->read(reinterpret_cast<byte*>(key), st_ + head_, size);
        } else {
            size_t a = n_ - head_;
            mng_->read(reinterpret_cast<byte*>(key), st_ + head_, a);
            mng_->read(reinterpret_cast<byte*>(key + a), st_, size - a);
        }
        MoveHead(size);
        return true;
    }

};
*/
}

#endif // NVQUEUE_H
