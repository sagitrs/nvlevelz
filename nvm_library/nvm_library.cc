#include "nvm_library.h"
/*
void bad_cleaner(byte* blackbox){
    return;
}
void Sag::nvm_init(ull size){
    Sag::mng_ = new NVM_Manager(size);      // to be deleted.
    Sag::nvmfs_ = new FakeFS(mng_);                     // to be deleted.
}
void Sag::nvm_terminate(){
    delete Sag::nvmfs_;
    delete Sag::mng_;
}

ul Sag::fread(void *ptr, ull size, ull count, nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fread(ptr,size,count,stream);
}
ul Sag::readWithOffset(void *ptr, ull count, ull offset, nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->refer(stream).read(
                reinterpret_cast<byte*>(ptr),count,offset);
}

ull Sag::fwrite(const void* buffer, ull size, ull count, nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fwrite(buffer,size,count,stream);
}
int Sag::feof(nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->feof(stream);
}
//    int fseek(nvFileHandle stream, long offset, int whence);
int Sag::fflush(nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fflush(stream);
}
nvFileHandle Sag::fopen(const char * file_name, const char* mode){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    if (mode[0] == 'a' || (strlen(mode)>1 && mode[1] == '+')){
        if (!nvmfs_->checkFile(file_name))
            nvmfs_->copyFromDiskToNVM(file_name);
    }
    return nvmfs_->fopen(file_name,mode);
}
int Sag::fclose(nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fclose(stream);
}

int Sag::rename(const char *oldname, const char *newname){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->rename(oldname, newname);
}
bool Sag::fileExist(const std::string& fname){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->checkFile(fname);
}
ull Sag::fileHandle(const std::string& fname){
     if (nvmfs_ == nullptr) nvm_init(Memory_Size);
     if (!fileExist(fname)) return NO_FILE;
     return nvmfs_->dict_[fname];
}

bool Sag::remove(const char* path){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    if (!nvmfs_->checkFile(path)) return 0;
    ull id = nvmfs_->dict_[path];
    return nvmfs_->deleteFile(id);
}
ull Sag::fileSize(const char * path){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fileSize(path);
}
ull Sag::fileSize(nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fileSize(stream);
}

int Sag::fseek(nvFileHandle stream, long offset, int whence){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fseek(stream,offset,whence);
}
ul Sag::fread_ulkd(void *ptr, ull size, ull count, nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fread_ulkd(ptr,size,count,stream);
}
ull Sag::fwrite_ulkd(const void* buffer, ull size, ull count, nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fwrite_ulkd(buffer,size,count,stream);
}
int Sag::fflush_ulkd(nvFileHandle stream){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->fflush_ulkd(stream);
}

void Sag::print(){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    nvmfs_->print();

}
int Sag::AddChildren(const std::string& dir,vector<string>* result){
    if (nvmfs_ == nullptr) nvm_init(Memory_Size);
    return nvmfs_->AddChildren(dir, result);
}

FakeFS* Sag::fileSystem(){
    return nvmfs_;
}

int Sag::LockOrUnlock(int fd, bool lock) {
    if (nvmfs_ == nullptr)
        nvm_init(Memory_Size);
    nvFileHandle file = static_cast<ull>(fd);
    if (nvmfs_->info_[file]->setLock(lock))
        return 0;
    else
        return -1;
}
*/
