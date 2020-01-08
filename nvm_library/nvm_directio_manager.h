#ifndef NVM_DIRECTIO_MANAGER_H
#define NVM_DIRECTIO_MANAGER_H
#include "global.h"
#include <mutex>
//#include <shared_mutex>
#include <vector>
#include <map>
//#include "nvm_delay_simulator.h"
#include "nvm_options.h"
#include <string.h>
#include "leveldb/slice.h"
#include <unistd.h>

struct NVM_DirectIO_Manager {
public:
    //std::shared_mutex locks_[1024];
    //std::shared_mutex global_lock_;
    struct NVM_Delay_Simulator {
            const ull w_delay;
            const ull r_delay;
            const ull bandwidth;
            ull w_rest, r_rest;
            std::mutex mutex_;
            const ull cache_line;
            nvAddr cache_line_start;
            std::map<pid_t, ll> rest_;

            NVM_Delay_Simulator(const NVM_Options& options) :
                w_delay(options.write_delay_per_cache_line),
                r_delay(options.read_delay_per_cache_line),
                bandwidth(options.bandwidth / 1000000000),
                w_rest(0), r_rest(0),
                cache_line(options.cache_line_size),
                cache_line_start(nvnullptr), rest_()
            {
            }
            inline void readDelay(ull operation, nvAddr addr) {
                if (r_delay <= 30) return;
                ll a = GetNano();
                pid_t id = getpid();
                auto p = rest_.find(id);
                if (p == rest_.end()) { rest_[id] = 0; p = rest_.find(id); }
                nvAddr st = addr / cache_line, ed = (addr + operation - 1) / cache_line;
                int cache_line_delayed = ed - st + 1;
                if (st <= cache_line_start && cache_line_start <= ed)
                    cache_line_delayed --;
                cache_line_start = ed;
                if (cache_line_delayed > 0)
                    //nanodelay_clock_gettime(r_delay);
                    p->second = nanodelay_until(a + p->second + r_delay);
                //nanodelay_clock_gettime(
                //            MIN(cache_line_delayed * r_delay ,
                //                operation / bandwidth + r_delay));
            }

            inline void writeDelay(ull operation, nvAddr addr) {
                if (w_delay <= 30) return;
                ll a = GetNano();
                pid_t id = getpid();
                auto p = rest_.find(id);
                if (p == rest_.end()) { rest_[id] = 0; p = rest_.find(id); }
                nvAddr st = addr / cache_line, ed = (addr + operation - 1) / cache_line;
                int cache_line_delayed = ed - st + 1;
                if (st <= cache_line_start && cache_line_start <= ed)
                    cache_line_delayed --;
                cache_line_start = ed;
                if (cache_line_delayed > 0)
                    //nanodelay_clock_gettime(w_delay);
                    p->second = nanodelay_until(a + p->second + w_delay);

            }

        };

    NVM_DirectIO_Manager(const NVM_Options& options) :
        main_blocks_(options.block),
        //limit_(options.main_size),
        delayer_(options)
        {}
    inline void read(byte* dest, const nvAddr src, ull bytes) {
        assert(dest != nullptr && src != nvnullptr);
        //LockSet(0,1,src,bytes);
        delayer_.readDelay(bytes, src);
        memcpy(dest, main_blocks_->Decode(src), bytes);
        //memcpy(dest, main_+src, bytes);
        //LockSet(0,0,src,bytes);
    }
    inline leveldb::Slice GetSlice(nvAddr src, ull bytes) {
        delayer_.readDelay(bytes, src);
        return leveldb::Slice(reinterpret_cast<const char*>(main_blocks_->Decode(src)), bytes);
    }
    inline ull read_ull(nvAddr src){
        static const ull bytes = sizeof(ull);
        ull dest;
        //byte dest[bytes];
        //static byte* const dest = new byte[bytes];
        assert(src != nvnullptr);
        read(reinterpret_cast<byte*>(&dest), src, bytes);
        return dest;
    }
    inline uint32_t read_ul(nvAddr src){
        static const ull bytes = sizeof(uint32_t);
        uint32_t dest;
        //byte dest[bytes];
        //static byte* const dest = new byte[bytes];
        assert(src != nvnullptr);
        read(reinterpret_cast<byte*>(&dest), src, bytes);
        return dest;
    }
    inline nvAddr read_addr(nvAddr src){
        static const ull bytes = sizeof(nvAddr);
        nvAddr dest;
        //byte dest[bytes];
        //static byte* const dest = new byte[bytes];
        assert(src != nvnullptr);
        read(reinterpret_cast<byte*>(&dest), src, bytes);
        //if (dest >= limit_){ printf("wait."); }
        //assert(dest < limit_);
        return dest;
        //return *(reinterpret_cast<nvAddr*>(dest));
    }
    inline byte read_byte(const nvAddr src) {
        assert(src != nvnullptr);
        //LockSet(0,1,src,bytes);
        delayer_.readDelay(1, src);
        return *static_cast<byte*>(main_blocks_->Decode(src));
        //memcpy(dest, main_+src, bytes);
        //LockSet(0,0,src,bytes);
    }

    inline void write(nvAddr dest, const byte* src, ull bytes) {
        //LockSet(1,1,dest,bytes);
        assert(dest != nvnullptr && src != nullptr);
        //assert(dest+bytes < limit_);
        delayer_.writeDelay(bytes, dest);
        memcpy(main_blocks_->Decode(dest), src, bytes);
        //LockSet(1,0,dest,bytes);
    }
    inline void write_zero(nvAddr dest, ull bytes) {
        //LockSet(1,1,dest,bytes);
        assert(dest != nvnullptr);
        //assert(dest+bytes < limit_);
        delayer_.writeDelay(bytes, dest);
        memset(main_blocks_->Decode(dest), 0, bytes);
        //LockSet(1,0,dest,bytes);
    }
    inline void write_ull(nvAddr dest, ull number){
        static const ull bytes = sizeof(ull);
        //static byte* const src = reinterpret_cast<byte*>(new ull);
        assert(dest != nvnullptr);
        //*src = number;
        //write(dest, src, bytes);
        write(dest, reinterpret_cast<const byte*>(&number), bytes);
    }
    inline void write_ul(nvAddr dest, uint32_t number){
        static const ull bytes = sizeof(uint32_t);
        //static byte* const src = reinterpret_cast<byte*>(new ull);
        assert(dest != nvnullptr);
        //*src = number;
        //write(dest, src, bytes);
        write(dest, reinterpret_cast<const byte*>(&number), bytes);
    }

    inline void write_addr(nvAddr dest, nvAddr addr){
        static const ull bytes = sizeof(nvAddr);
        //static byte* src = reinterpret_cast<byte*>(new nvAddr);
        assert(dest != nvnullptr);
        write(dest, reinterpret_cast<const byte*>(&addr), bytes);
    }

    inline void nvmcpy(nvAddr dest, const nvAddr src, ull bytes) {
        assert(dest != nvnullptr && src != nvnullptr);
        //assert(src+bytes < limit_);
        //assert(dest+bytes < limit_);
        delayer_.readDelay(bytes, src);
        delayer_.writeDelay(bytes, dest);
        memcpy(main_blocks_->Decode(dest), main_blocks_->Decode(src), bytes);
    }

    void Print(int level = 0){
        printbyte(' ', level);
        printf("NVM IO Manager (%llu)\n",main_blocks_->Size());
    }
/*
    void LockSet(bool write, bool lock, nvAddr src, ull bytes){
        ull h = src / 1024 % 1024;
        ull t = (src+bytes) / 1024 % 1024;
        if (h == t || (h+1)%1024 == t){
            if (lock)
                global_lock_.lock_shared();

            if (write){
                if (lock)
                    locks_[h].lock();
                else
                    locks_[h].unlock();
            } else {
                if (lock)
                    locks_[h].lock_shared();
                else
                    locks_[h].unlock_shared();
            }

            if (!lock)
                global_lock_.unlock_shared();
        } else {
            if (lock)
                global_lock_.lock();
            else
                global_lock_.unlock();
        }
    }*/
    NVM_MemoryBlock* main_blocks_;
    ull limit_;
    NVM_Delay_Simulator delayer_;
};

#endif // NVM_DIRECTIO_MANAGER_H
