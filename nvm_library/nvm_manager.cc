#include "nvm_manager.h"
#include "nvtrie.h"

NVM_Manager::NVM_Manager(size_t size) :
    main_block_(sys_nvm_allocate(size,RecoverFunction)),
    options_(main_block_),
    memory_(nullptr),
    //guardian_(new NVM_Guardian(&io_, memory_)),
    index_(nullptr),
    w_delay(options_.write_delay_per_cache_line), r_delay(options_.read_delay_per_cache_line),
    cache_line(options_.cache_line_size), bandwidth(options_.bandwidth / 1000000000), // (byte/s) -> (byte/ns)
    w_opbase(bandwidth * w_delay), r_opbase(bandwidth * r_delay)
{
    memory_ = new NVM_MainAllocator(options_, this);
    index_ = new nvTrie(this);
//    index_->openType_ = nvFile::READ_WRITE;
    //bind_name("GUARDIAN",guardian_->Address());
    bind_name("/index/index.nvTrie", index_->main_);

}

NVM_Manager::~NVM_Manager() {
    delete index_;

    delete memory_;

    main_block_->Dispose();
    delete main_block_;
    //delete [] main_;
}

int NVM_Manager::bind_name(std::string name, nvAddr addr){
    std::string s;
    s += name;
    s += '\t';
    s += std::to_string(addr);
    s += '\n';
    //index_->Insert(name, addr);
//    byte* info = new byte[s.size()];
//    sprintf((char*)info,"%s",s.c_str())
//    index_->append(s.c_str(), s.size());
    return 0;
}
int NVM_Manager::delete_name(std::string name) {
    //index_->Delete(name);
    //return bind_name(name,nvnullptr);
}
// 4. debug function
void NVM_Manager::Print(){
    printf("NVM Manager :\n");
}

nvAddr NVM_Manager::Allocate(size_t size) {
    return memory_->Allocate(size);
}
void NVM_Manager::Dispose(nvAddr ptr, size_t size) {
    memory_->Dispose(ptr,size);
}
