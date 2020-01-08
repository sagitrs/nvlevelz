#pragma once
#include "global.h"
#include "nvm_directio_manager.h"
#include "nvm_allocator.h"
#include <mutex>
#include <pthread.h>
struct NVM_Guardian {
    // Guardian is a atomic method for nvm_manager.
    // Recover area : [Stable_mark][Data_mark][dest_addr][src_addr][bytes]
    // redo_log process:
    // 1. Set stable_mark to false;     Crash will do nothing (Data = false)
    // 2. Set Data_mark to true;        Crash will do nothing (Stable = false)
    // 3. Save data to dest,src,bytes;  Crash will do nothing (Stable = false)
    // 4. Set Stable_mark to true;      Crash will do or not do (no matter)
    // 5. Do the operation;             Crash will recover (Stable = true, Data = true)
    // 6. Set Data_mark to false;       Crash will do or not do (no matter)

    NVM_DirectIO_Manager * io_;
    NVM_Allocator * allocator_;
    //std::mutex mutex_;
    const nvAddr main_;
    //nvAddr stable_mark, data_mark;
    //nvAddr dest_addr_, src_addr_, bytes_addr_;

    enum { StableMark = 0, DataMark = 8, DestAddr = 16, SrcAddr = 24, BytesAddr = 32, TotalLength = 40 };
    // rely on sizeof(nvAddr) <= 8.

    nvAddr src_copy_addr_;
    ull src_copy_size_;

    NVM_Guardian(NVM_DirectIO_Manager * io, NVM_Allocator * allocator) :
        io_(io),
        allocator_(allocator),
        main_(allocator->Allocate(TotalLength)){
    }
    ~NVM_Guardian(){
        allocator_->Dispose(main_,TotalLength);
    }
    nvAddr Address() { return main_; }

    //Guardian(NVM_Manager * mng, byte* main_address);
    void addrInit(nvAddr main_address);
    void write_guard(nvAddr dest, const void* src, ull bytes){
        assert(bytes > 0);

        src_copy_addr_ = allocator_->Allocate(bytes);
        assert(src_copy_addr_ != nvnullptr);
        src_copy_size_ = bytes;
        io_->write(src_copy_addr_, static_cast<const byte*>(src), bytes);
        copy_guard(dest,src_copy_addr_,src_copy_size_);
    }

    void copy_guard(nvAddr dest, nvAddr src, ull bytes){
        assert(bytes > 0);
        write(StableMark,0LL);

        write(DataMark,1LL);

        write(DestAddr,dest);
        write(SrcAddr,src);
        write(BytesAddr,bytes);

        write(StableMark,1LL);
    }

    void cancel(byte k = 0, bool force = false){
        pthread_t pid = pthread_self();

        assert(src_copy_size_ != 0);
        assert(src_copy_addr_ != nvnullptr);
        write(DataMark,0LL);
//        if (src_copy_size_ == 0LL) return;
//        if (src_copy_addr_ == 0LL) return;
        allocator_->Dispose(src_copy_addr_, src_copy_size_);
        src_copy_addr_ = nvnullptr;
        src_copy_size_ = 0LL;
        //write(StableMark,1LL,k);
    }

    void print(){
        ull data = io_->read_ull(main_+DataMark);
        ull stable = io_->read_ull(main_+StableMark);
        printf("Guardian State of Unit 0: Stable=%s, Data=%s\n",(stable?"TRUE":"FALSE"),(data?"TRUE":"FALSE"));
        //ull destination = io_->read_ull(main_+DestAddr);
        //ull source = io_->read_ull(main_+SrcAddr);
        //ull bytes = io_->read_ull(main_+BytesAddr);
        //printf("%llu bytes From M[%llu] to M[%llu]\n",bytes,source,destination);
    }
private:
    inline void write(nvAddr pos, ull data){
        io_->write_ull(main_ + pos, data);
        // rely on sizeof(nvAddr) = 8.
    }
};

/*

struct NVM_Guardian {
    // Guardian is a atomic method for nvm_manager.
    // Recover area : [Stable_mark][Data_mark][dest_addr][src_addr][bytes]
    // redo_log process:
    // 1. Set stable_mark to false;     Crash will do nothing (Data = false)
    // 2. Set Data_mark to true;        Crash will do nothing (Stable = false)
    // 3. Save data to dest,src,bytes;  Crash will do nothing (Stable = false)
    // 4. Set Stable_mark to true;      Crash will do or not do (no matter)
    // 5. Do the operation;             Crash will recover (Stable = true, Data = true)
    // 6. Set Data_mark to false;       Crash will do or not do (no matter)

    NVM_DirectIO_Manager * io_;
    NVM_Allocator * allocator_;
    //std::mutex mutex_;
    static const byte MaxGuardUnit = 16;
    const nvAddr main_;
    //nvAddr stable_mark, data_mark;
    //nvAddr dest_addr_, src_addr_, bytes_addr_;

    enum { StableMark = 0, DataMark = 8, DestAddr = 16, SrcAddr = 24, BytesAddr = 32, TotalLength = 40 };
    // rely on sizeof(nvAddr) <= 8.

    struct GuardUnit {
        pthread_t pid_;
        nvAddr src_copy_addr_;
        ull src_copy_size_;
        bool in_use_;
        GuardUnit() : pid_(0), src_copy_addr_(nvnullptr), src_copy_size_(0), in_use_(false) {}
    } unit[MaxGuardUnit];

    NVM_Guardian(NVM_DirectIO_Manager * io, NVM_Allocator * allocator) :
        io_(io),
        allocator_(allocator),
        main_(allocator->Allocate(TotalLength * MaxGuardUnit)){
        for (int i = 0; i < MaxGuardUnit; ++i)
            unit[i] = GuardUnit();
    }
    ~NVM_Guardian(){
        for (byte i = 0; i < MaxGuardUnit; ++i){
            if (unit[i].in_use_ && unit[i].src_copy_addr_ != nvnullptr)
                cancel(i,true);
        }
        allocator_->Dispose(main_,TotalLength * MaxGuardUnit);
    }
    nvAddr Address() { return main_; }

    //Guardian(NVM_Manager * mng, byte* main_address);
    void addrInit(nvAddr main_address);
    void write_guard(nvAddr dest, const void* src, ull bytes, byte k = 0){
        assert(bytes > 0);
        pthread_t pid = pthread_self();
        if (unit[k].pid_ != pid){
            for (k = 0; k < MaxGuardUnit; ++k)
                if (unit[k].pid_ == pid) break;
            if (k == MaxGuardUnit)
                for (k = 0; k < MaxGuardUnit; ++k)
                    if (!unit[k].in_use_) {
                        unit[k].pid_ = pid;
                        if (pid == unit[k].pid_){
                            unit[k].in_use_ = true;
                            break;
                        }
                    }
            assert( k != MaxGuardUnit );
        }
        //}
        assert(unit[k].pid_ == pid);

        unit[k].src_copy_addr_ = allocator_->Allocate(bytes);
        assert(unit[k].src_copy_addr_ != nvnullptr);
        unit[k].src_copy_size_ = bytes;
        io_->write(unit[k].src_copy_addr_, static_cast<const byte*>(src), bytes);
        copy_guard(dest,unit[k].src_copy_addr_,unit[k].src_copy_size_, k);
    }

    void copy_guard(nvAddr dest, nvAddr src, ull bytes, byte k = 0){
        assert(bytes > 0);
        pthread_t pid = pthread_self();
        if (unit[k].pid_ != pid){
            for (k = 0; k < MaxGuardUnit; ++k)
                if (unit[k].pid_ == pid) break;
            if (k == MaxGuardUnit)
                for (k = 0; k < MaxGuardUnit; ++k)
                    if (!unit[k].in_use_) {
                        unit[k].pid_ = pid;
                        if (pid == unit[k].pid_)
                            unit[k].in_use_ = true;
                    }
            assert( k != MaxGuardUnit );
        }

        write(StableMark,0LL,k);

        write(DataMark,1LL,k);

        write(DestAddr,dest,k);
        write(SrcAddr,src,k);
        write(BytesAddr,bytes,k);

        write(StableMark,1LL,k);
    }

    void cancel(byte k = 0, bool force = false){
        pthread_t pid = pthread_self();
        if (unit[k].pid_ != pid && !force){
            for (k = 0; k < MaxGuardUnit; ++k)
                if (unit[k].pid_ == pid) break;
            assert( k != MaxGuardUnit );
        }
        if (unit[k].src_copy_size_ == 0){
            printf("Wait.\n");
        }
        assert(unit[k].src_copy_size_ != 0);
        assert(unit[k].src_copy_addr_ != nvnullptr);
        write(DataMark,0LL,k);
//        if (src_copy_size_ == 0LL) return;
//        if (src_copy_addr_ == 0LL) return;
        allocator_->Dispose(unit[k].src_copy_addr_, unit[k].src_copy_size_);
        unit[k].src_copy_addr_ = nvnullptr;
        unit[k].src_copy_size_ = 0LL;
        //write(StableMark,1LL,k);
    }

    void print(){
        ull data = io_->read_ull(main_+DataMark);
        ull stable = io_->read_ull(main_+StableMark);
        printf("Guardian State of Unit 0: Stable=%s, Data=%s\n",(stable?"TRUE":"FALSE"),(data?"TRUE":"FALSE"));
        //ull destination = io_->read_ull(main_+DestAddr);
        //ull source = io_->read_ull(main_+SrcAddr);
        //ull bytes = io_->read_ull(main_+BytesAddr);
        //printf("%llu bytes From M[%llu] to M[%llu]\n",bytes,source,destination);
    }
private:
    void write(nvAddr pos, ull data, byte k){
        io_->write_ull(main_+ k * TotalLength + pos, data);
        // rely on sizeof(nvAddr) = 8.
    }
};

*/
