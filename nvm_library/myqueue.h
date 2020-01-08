#ifndef MYQUEUE_H
#define MYQUEUE_H
#include "global.h"
#include "port/atomic_pointer.h"
#include "port/port_posix.h"
#include <cstring>
//#include <stdlib.h>

struct MyQueue {
    const uint32_t size_;
    const uint32_t element_size_;
    byte* a;
    ull head_;
    leveldb::port::AtomicPointer tail_;
    leveldb::port::RWLock lock_;

    ull Next(ull x) { return (x + 1 < size_ ? x + 1 : 0);}
    void* operator[] (ull x) { return a + x * element_size_; }
    ull Head() const { return head_; }
    ull Tail() const { return reinterpret_cast<ull>(tail_.NoBarrier_Load()); }
    ull Size() const {
        ull tail = Tail(), head = Head();
        return (tail < head ? tail + size_ - head : tail - head);
    }
#define MyQueue_Next(x) (x + 1 < size_ ? x + 1 : 0)
#define MyQueue_Tail (reinterpret_cast<ull>(tail_.NoBarrier_Load()))
#define MyQueue_Head (head_)
#define MyQueue_SetTail(x) (tail_.NoBarrier_Store(reinterpret_cast<void*>(x)))
#define MyQueue_SetHead(x) (head_ = x)
    // a[head_] : First element in queue;
    // a[tail_] : Next element of the Last element in queue.
    // if tail_ == head_, it means queue is empty.
    // We don't allow queue.size() == size_, that's why size_ = size+1 during initialization.
    MyQueue(uint32_t size, uint32_t element_size) :
        size_(size+1), element_size_(element_size), a(nullptr),
        head_(0), tail_()
    {
        a = new byte[size_ * element_size];
        MyQueue_SetHead(0);
        MyQueue_SetTail(0);
        assert(Empty());
    }
    void PushBack(const void* src) {    // For Producer: head_ should be a signal mark, to check if queue is full. but...
        ull locate = MyQueue_Tail;
        //assert(Empty() || Next(Tail()) != Head());
        memcpy(a + locate * element_size_,
               src,
               element_size_);
        MyQueue_SetTail(MyQueue_Next(locate));
    }
    void PopFront(void *src) {
        //assert(!Empty());
        ull locate = MyQueue_Head;
        memcpy(src, a + locate * element_size_, element_size_);
        MyQueue_SetHead(MyQueue_Next(locate));
    }
    void Front(void* src) {
        ull locate = MyQueue_Head;
        memcpy(src, a + locate * element_size_, element_size_);
    }

    void Back(void* src) {
        ull locate = MyQueue_Tail;
        memcpy(src, a + locate * element_size_, element_size_);
    }
    bool Empty() const {                  // For Consumer: tail_ should be a signal mark, to check if queue is empty.
        return MyQueue_Tail == MyQueue_Head;
    }
};

#endif // MYQUEUE_H
