#include "sysnvm.h"

struct ArxAllocator {
    typedef void* (*ArxMallocFunction)(uint32_t, uint64_t);
    typedef void (*ArxFreeFunction)(uint32_t, void*, uint64_t);
    void* (*ArxMalloc)(uint32_t, uint64_t*);
    ArxFreeFunction ArxFree;
    int32_t (*ArxNVDimmsDetected)(uint64_t*, uint64_t*);
    int32_t (*ArxEnableSave)(int, int);

    uint64_t length_;
    unsigned char *main_;
    ArxAllocator() {}

    void Init() {
        void *handle;
        char *error;

        handle = dlopen("libArxcisAPI.so", RTLD_LAZY);
        if (handle == NULL) {
            printf("Failed loading library: ArxcisAPI.so (Error Message: %s)", dlerror());
            return;
        }

        *(void**)&ArxMalloc = dlsym(handle, "ArxMalloc");
        *(void**)&ArxFree = dlsym(handle, "ArxFree");
        *(void**)&ArxNVDimmsDetected = dlsym(handle, "ArxNVDimmsDetected");
        *(void**)&ArxEnableSave = dlsym(handle, "ArxEnableSave");

        //char* buf = (char*)(*ArxMalloc)(0, 100);
    }

    void AllocateAll() {
        main_ = (unsigned char*)(*ArxMalloc)(0b01, &length_);
    }

    void DisposeAll() {
            (*ArxFree)(0b01, main_, length_);
    }

    int32_t Detected(uint64_t *address, uint64_t *length) {
        return (*ArxNVDimmsDetected)(address, length);
    }

    int32_t EnableSave() {
        return (*ArxEnableSave)(0, 0b0011);
    }
};

void sys_nvm_dispose(NVM_MemoryBlock* block) {
#ifdef NVDIMM_ENABLED
    ArxAllocator *arc = new ArxAllocator;
    arc->main_ = block->Main();
    arc->length_ = block->Size();
    arc->DisposeAll();
    delete arc;
#else
    for (int i = 0; i < block->block_num_; ++i)
        delete[] block->block_[i].main_;
#endif
}

NVM_MemoryBlock* sys_nvm_allocate(ull size, Cleaner cl) {

#ifdef NVDIMM_ENABLED
    size_t block_num = 1;
    NVM_MemoryBlock::MemoryBlock* blocks = new NVM_MemoryBlock::MemoryBlock[block_num];

    ArxAllocator *arc = new ArxAllocator;
    arc->Init();
    uint64_t addr, length;
    arc->EnableSave();
    arc->Detected(&addr, &length);
    arc->AllocateAll();
        printf("NVM State : [%llx] + %llx\n", addr, length);
        printf("%llx[%llx] Allocated.\n", arc->main_, arc->length_);
        fflush(stdout);
    blocks[0].main_ = arc->main_;
    blocks[0].size_ = arc->length_;
#else
    size_t block_num = size / MaxBlockSize;
    size_t rest = size % MaxBlockSize;
    block_num += (rest > 0);
    NVM_MemoryBlock::MemoryBlock* blocks = new NVM_MemoryBlock::MemoryBlock[block_num];
    for (size_t i = 0; i < block_num; ++i) {
        blocks[i].size_ = ((i == block_num - 1 && rest > 0) ? rest : MaxBlockSize);
        blocks[i].main_ = new byte[blocks[i].size_];
    }
#endif
    return new NVM_MemoryBlock(blocks, block_num);
    //return static_cast<byte*>(malloc(size));
}

