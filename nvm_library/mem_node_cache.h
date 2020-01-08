#ifndef MEM_NODE_CACHE_H
#define MEM_NODE_CACHE_H

#include "nvm_library/arena.h"
#include "db/skiplist.h"
#include "nodetable.h"
#include "math.h"

namespace leveldb {

struct CachePolicy {
//  First Level Paramenter:
    ull dram_size_;
    ull nvm_size_;
    double nearly_full_rate_;
    double hash_rate_;
    double immutablequeue_rate_;
// Second Level Paramenter:
    ull nvskiplist_size_;
    ull nearly_full_size_;
    ull hash_range_;
    ull standard_nvmemtable_size_;
    ull standard_multimemtable_size_;
    ull standard_immutablequeue_size_;
 // Unknown Paramenter:
    ull garbage_cache_percentage_;
    ull garbage_cache_size_;
    double hash_full_limit_;
    ull node_cache_size_;
    ull cover_range_;
    int height_;
    int p_;
    bool cache_disable_;
    CachePolicy(ull nvmem, ull nvmem_nearly_full, ull dram_buffer, ull nvm_buffer, ull cover_range, ul hash_div):
        dram_size_(dram_buffer), nvm_size_(nvm_buffer),
        nearly_full_rate_(1. * nvmem_nearly_full / nvmem),
        hash_rate_(1. / hash_div),
        immutablequeue_rate_(0.25),

        nvskiplist_size_(nvmem >= nulloffset ? nulloffset - 1 : nvmem),
        nearly_full_size_(nvmem_nearly_full),
        hash_range_(trunc(nvskiplist_size_ * hash_rate_ / 4)),
        standard_nvmemtable_size_(trunc(nvm_size_ / (nvskiplist_size_ * (1 + hash_rate_)))),
        standard_multimemtable_size_(standard_nvmemtable_size_ * (1. - immutablequeue_rate_)),
        standard_immutablequeue_size_(standard_nvmemtable_size_ - standard_multimemtable_size_),

        garbage_cache_percentage_(10),
        garbage_cache_size_(64),
        hash_full_limit_(0.8),
        node_cache_size_((dram_size_ - garbage_cache_size_) / standard_nvmemtable_size_),
        cover_range_(cover_range * (nvskiplist_size_ / (2 * MB))),
        cache_disable_(dram_size_ / nvskiplist_size_ > 16 * KB ? false : true) {

        if (dram_size_ >= nvm_size_) { height_ = 0; p_ = 1; return; }
        if (cache_disable_) { height_ = 12; p_ = 1; return; }
        size_t X = trunc(nvm_buffer / dram_buffer);
        height_ = ceil(log2(X) / 2.0);
        ull a = 3 * X, b = pow2(1, 2*height_) - X;
        if (b == 0)
            p_ = 0;
        else
            p_ = ceil(a / b);
    }
};

struct PuzzleCache {
    struct Puzzle {
        nvOffset addr;
        nvOffset size;
        Puzzle() : addr(nulloffset), size(0) {}
        bool blank() { return size == 0; }
        void set(nvOffset ad, nvOffset si) { addr = ad; size = si; }
        void clear() { set(nulloffset,0); }
    };
    const nvOffset size_;
    ull lost_, found_;

    struct Cache {
        Puzzle *a_;
        nvOffset count_, size_;
        Cache(size_t size) : a_(nullptr), count_(0), size_(size) {
            a_ = new Puzzle[size];
            Clear();
        }
        ~Cache() {
            delete[] a_;
        }
        static const int NotFound = -1;
        Puzzle* Min(nvOffset mark = 1) {
            if (count_ == 0) //return -1;
                return nullptr;
            int j = NotFound;
            nvOffset min_size = 0xFFFFFFFFUL;
            for (int i = 0; i < size_; ++i)
                if (min_size > a_[i].size && a_[i].size >= mark) {
                    j = i;
                    min_size = a_[i].size;
                }
            if (j == NotFound) return nullptr;
            return a_ + j;
        }
        Puzzle* BlankOne() {
            for (int i = 0; i < size_; ++i)
                if (a_[i].blank()) {
                    return a_ + i;
                }
            return nullptr;
        }

        void Clear() {
            for (nvOffset i = 0; i < size_; ++i)
                a_[i].clear();
        }

        void Eat(Cache* b) {
            for (int i = b->size_ - 1; i >= 0; --i) if (!b->a_[i].blank()) {
                Puzzle* x = nullptr;
                if (count_ < size_) {
                    x = BlankOne();
                    count_++;
                }
                else
                    x = Min();
                Puzzle* y = b->a_ + i;
                x->set(y->addr, y->size);
                y->clear();
            }
            b->count_ = 0;
        }
    };
    Cache a_, b_;

    PuzzleCache(nvOffset size) :
        size_(size / sizeof(Puzzle)),
        lost_(0), found_(0), a_(size_), b_(size_) {
    }
    ~PuzzleCache() {
    }
    nvOffset Allocate(nvOffset size) {
        if (size_ == 0) return nulloffset;
        Puzzle* x = a_.Min(size);
        if (x == nullptr)
            return nulloffset;
        nvOffset result = x->addr;
        lost_ += x->size - size;
        found_ -= x->size;
        x->clear();
        a_.count_ --;
        return result;
    }
    void Reserve(nvOffset addr, nvOffset size) {
        if (b_.count_ + 1 >= size_)
            a_.Eat(&b_);
        assert(b_.count_ < size_);
        /*
        if (b_.count_ == size_) {
            if (size_ == 0) {
                lost_ += size;
                return;
            }
            int j = findMin();
            found_ -= b_[j].size;
            lost_ += b_[j].size;
            b_[j].set(addr, size);
            found_ += size;
            return;
        }
        */
        Puzzle* x = b_.BlankOne();
        x->set(addr, size);
        found_ += size;
        b_.count_++;
    }
    void Clear() {
        a_.Clear();
        b_.Clear();
        lost_ = 0;
        found_ = 0;
    }
    ul Bytes() const {
        return size_ * sizeof(Puzzle);
    }
};

struct LargePuzzleCache {
    std::unordered_map<ull, nvAddr> puzzle_;
    const size_t max_size_;
    ull garbage_;
    static const size_t PuzzleSize = 8 + 8;
    NVM_Manager* mng_;

    LargePuzzleCache(NVM_Manager* mng, ull size) :
        puzzle_(), max_size_(size / PuzzleSize), garbage_(0), mng_(mng) {
    }

    ~LargePuzzleCache() {
    }
    nvAddr Allocate(ull size) {
        auto p = puzzle_.find(size);
        if (p == puzzle_.end()) return nvnullptr;
        nvAddr ans = p->second;
        garbage_ -= size;
        if (ans != nvnullptr)
            p->second = mng_->read_addr(ans);
        else
            puzzle_.erase(p);
        return ans;
    }
    void Dispose(nvAddr addr, ull size) {
        auto p = puzzle_.find(size);
        //nvAddr old = (p == puzzle_.end() ? nvnullptr : p->second);
        if (p == puzzle_.end()) {
            mng_->write_addr(addr, nvnullptr);
            puzzle_[size] = addr;
        } else {
            mng_->write_addr(addr, p->second);
            p->second = addr;
        }
        //mng_->write_addr(addr, old);
        //puzzle_[size] = addr;
        garbage_ += size;
    }

    ull Garbage() const { return garbage_; }


};

}

#endif // MEM_NODE_CACHE_H
