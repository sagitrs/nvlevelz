#ifndef NVM_MANAGER_H
#define NVM_MANAGER_H
#include "global.h"
#include "nvm_io_manager.h"
#include "nvm_options.h"
#include "nvm_allocator.h"
#include "sysnvm.h"
//#include "nvfile.h"
#include <unordered_map>
#include <pthread.h>
#include "sysnvm.h"
#include "port/atomic_pointer.h"
#include "leveldb/slice.h"
//#include "nvtrie.h"

struct nvFile;
struct nvTrie;
class NVM_MainAllocator;

struct NVM_Manager {
public:
    //byte* main_;
    NVM_MemoryBlock* main_block_;
    NVM_Options options_;
    //NVM_DirectIO_Manager io_;
    NVM_MainAllocator *memory_;
    //nvFile* index_;
    nvTrie* index_;

    const ull w_delay, r_delay;
    const ull cache_line, bandwidth;
    const ull w_opbase, r_opbase;
    struct ThreadInfo {
        nvAddr last_cache_line_;
        ull rest_;
        ThreadInfo(ull cache_line, ull rest) : last_cache_line_(cache_line), rest_(rest) {}
        ThreadInfo() :last_cache_line_(nvnullptr), rest_(0) {}
    };
    std::unordered_map<pid_t, ThreadInfo> info_;

    static void RecoverFunction(byte* main) {}
    NVM_Manager(size_t size);

    ~NVM_Manager();
// 1. IO Management
    inline void readDelay(ull operation, nvAddr addr) {
        static __thread ull rest = 0;
        static __thread ull prev = 0;
        if (r_delay <= rest) {
            rest -= r_delay;
            return;
        }
        ll a = GetNano();

        nvAddr cl = (addr + operation) / cache_line;
        if (addr / cache_line == cl && prev == cl) {
            rest += GetNano() - a;
            return;
        }
        prev = cl;

        ull delay_time = r_delay;
        if (operation > r_opbase)
            delay_time += (operation - r_opbase) / bandwidth;

        rest = nanodelay_until(a + rest + delay_time);
    }

    inline void writeDelay(ull operation, nvAddr addr) {
        static __thread ull rest = 0;
        static __thread ull prev = 0;
        if (w_delay <= rest) {
            rest -= w_delay;
            return;
        }
        ll a = GetNano();

        nvAddr cl = (addr + operation) / cache_line;
        if (addr / cache_line == cl && prev == cl) {
            rest += GetNano() - a;
            return;
        }
        prev = cl;

        ull delay_time = w_delay;
        if (operation > w_opbase)
            delay_time += (operation - w_opbase) / bandwidth;

        rest = nanodelay_until(a + rest + delay_time);
    }
    inline void read(byte* dest, nvAddr src, ull bytes){
#ifndef NO_READ_DELAY
        readDelay(bytes, src);
        memcpy(dest, main_block_->Decode(src), bytes);
#else
        memcpy(dest, reinterpret_cast<byte*>(src), bytes);
#endif
        //assert(dest != nullptr && src != nvnullptr);
        //return dest;
    }

    inline leveldb::Slice GetSlice(nvAddr src, ull bytes) {
#ifndef NO_READ_DELAY
        readDelay(bytes, src);
        return leveldb::Slice(reinterpret_cast<const char*>(main_block_->Decode(src)), bytes);
#else
        return leveldb::Slice(reinterpret_cast<const char*>(src), bytes);
#endif
    }
    inline void write(nvAddr dest, const byte* src, ull bytes){
        //assert(dest != nvnullptr && src != nullptr);
        byte* dest_ = reinterpret_cast<byte*>(main_block_->Decode(dest));
        writeDelay(bytes, dest);
        memcpy(dest_, src, bytes);
        asm_clflush(dest_);

        //return dest;
    }
    inline void write_barrier(nvAddr dest, const byte* src, ull bytes){
        leveldb::port::MemoryBarrier();
        write(dest, src, bytes);
    }
    inline nvAddr write_zero(nvAddr dest, ull bytes){
        //assert(dest != nvnullptr);
        byte* dest_ = reinterpret_cast<byte*>(main_block_->Decode(dest));
        //writeDelay(bytes, dest);
        memset(dest_, 0, bytes);
        asm_clflush(dest_);
    }
    /*
    inline nvAddr nvmcpy(nvAddr dest, nvAddr src, ull bytes){
        io_.nvmcpy(dest,src,bytes);
        return dest;
    }
    inline byte* dramcpy(byte* dest, byte* src, ull bytes){
        memcpy(dest,src,bytes);
    }*/
    inline ull read_ull(nvAddr src){
#ifndef NO_READ_DELAY
        readDelay(8, src);
        return *reinterpret_cast<ull*>(main_block_->Decode(src));
#else
        return *reinterpret_cast<ull*>(src);
#endif
    }
    inline ull read_ull_barrier(nvAddr src) {
        leveldb::port::MemoryBarrier();
        return read_ull(src);
    }
    inline void read_barrier(byte* dest, nvAddr src, ull bytes) {
        leveldb::port::MemoryBarrier();
        read(dest, src, bytes);
    }
    inline ul read_ul_barrier(nvAddr src) {
        leveldb::port::MemoryBarrier();
        return read_ul(src);
    }
    inline uint32_t read_ul(nvAddr src){
#ifndef NO_READ_DELAY
        readDelay(4, src);
        return *reinterpret_cast<ul*>(main_block_->Decode(src));
#else
        return *reinterpret_cast<ul*>(src);
#endif
    }
    inline byte read_byte(nvAddr src) {
#ifndef NO_READ_DELAY
        readDelay(1, src);
        return *reinterpret_cast<byte*>(main_block_->Decode(src));
#else
        return *reinterpret_cast<byte*>(src);
#endif
    }
    inline void write_ul(nvAddr dest, const uint32_t number){
        byte* dest_ = reinterpret_cast<byte*>(main_block_->Decode(dest));
        *reinterpret_cast<ul*>(main_block_->Decode(dest)) = number;
        writeDelay(4, dest);
        asm_clflush(dest_);
    }
    inline nvAddr read_addr(nvAddr src){
#ifndef NO_READ_DELAY
        readDelay(8, src);
        return *reinterpret_cast<nvAddr*>(main_block_->Decode(src));
#else
        return *reinterpret_cast<nvAddr*>(src);
#endif
    }
    inline void write_ull(nvAddr dest, const ull number){
        byte* dest_ = reinterpret_cast<byte*>(main_block_->Decode(dest));
        *reinterpret_cast<ull*>(main_block_->Decode(dest)) = number;
        writeDelay(8, dest);
        asm_clflush(dest_);
    }
    inline void write_ull_barrier(nvAddr dest, const ull number) {
        leveldb::port::MemoryBarrier();
        write_ull(dest, number);
    }
    inline void write_ul_barrier(nvAddr dest, const ul number) {
        leveldb::port::MemoryBarrier();
        write_ul(dest, number);
    }
    inline void write_addr(nvAddr dest, const nvAddr addr) {
        writeDelay(8, dest);
        byte* dest_ = reinterpret_cast<byte*>(main_block_->Decode(dest));
        *reinterpret_cast<nvAddr*>(main_block_->Decode(dest)) = addr;
        asm_clflush(dest_);
    }

// 2. allocate and dispose
    nvAddr Allocate(size_t size);
    void Dispose(nvAddr ptr, size_t size);


// 3. bind "name" with "address"
    int bind_name(std::string name, nvAddr addr);
    int delete_name(std::string name);
// 4. debug function
    void Print();
// 5. old-type function
    inline nvAddr alloc(size_t size){
        return Allocate(size);
    }

    inline void dispose(nvAddr addr, size_t size){
        return Dispose(addr,size);
    }
};

#endif // NVM_MANAGER_H
