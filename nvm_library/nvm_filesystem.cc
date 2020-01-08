#include "nvm_filesystem.h"

FakeFS::FakeFS(NVM_Manager * mng) :
    mng_(mng),
    dict_(),
    info_(new nvFile*[MAX_FILE_COUNT]),
    id_(1LL) {
    }
FakeFS::~FakeFS() {
    for (ull i = 1;i < id_; ++i) if (info_[i] != nullptr){
        delete info_[i];
    }
    dict_.clear();
    delete[] info_;
}

nvFile& FakeFS::refer(ull id){
    return *(info_[id]);
    //return info_.at(id);
}

bool FakeFS::checkFile(const std::string& fname){
    return dict_.find(fname) != dict_.end();
}

bool FakeFS::checkFile(ull file_id){
    return (file_id < id_ && info_[file_id] != nullptr);
    //return info_.find(file_id) != info_.end();
}

ull FakeFS::newFile(const char* file_name){
    if (checkFile(file_name)){
        deleteFile(dict_[file_name]);
    }
    ull file_id = id_;
    id_ ++;
    info_[file_id] = new nvFile(mng_);
    dict_[file_name] = file_id;
    info_[file_id]->setName(file_name);
    return file_id;
}

bool FakeFS::deleteFile(ull id){
    if (!checkFile(id)) return 0;
    dict_.erase(info_[id]->name());
    delete info_[id];
    info_[id] = nullptr;
    //info_.erase(id);
    return 1;
    /*
    if (!checkFile(id)) return 0;
    nvFile* file;
    dict_.erase(file->name());
    file->release();
    delete file;
    info_[id] = nullptr;
    //info_.erase(id);
    return 1;
    */
}

ull FakeFS::readFile(ull id, void* dest, ull bytes){
    if (!checkFile(id)) return 0;
    return refer(id).read(static_cast<char*>(dest),bytes);
}

ull FakeFS::appendFile(ull id, const char* src, ull bytes){
    if (!checkFile(id)) return 0;
    return refer(id).append(src,bytes);
}

int FakeFS::rename(const char *oldname, const char *newname){
    auto i = dict_.find(oldname);
    if (i == dict_.end()) return -1;
    ull id = i->second;
    refer(id).setName(newname);
    dict_.erase(oldname);
    dict_[newname] = id;
    //info_[id].setName(newname);
    return 0;
}

ll FakeFS::fileSize(const char * path){
    auto i = dict_.find(path);
    if (i == dict_.end()) return -1;
    return static_cast<long long>(refer(i->second).totalSize());
}
ll FakeFS::fileSize(nvFileHandle stream){
    return static_cast<long long>(refer(stream).totalSize());
}

bool FakeFS::copyFromDiskToNVM(const char* file_name){
    FILE* g = std::fopen(file_name,"r");
    if (g == nullptr) return 0;
    nvFileHandle f = fopen(file_name,"w");
    static const int L = 4096;
    static char buf[L];
    while (1){
        size_t count = std::fread(buf,1,L,g);
        if (count > 0){
            //printf("%s",buf);
            fwrite(buf,1,count,f);
        }
        if (count < L)
            break;
    }
    std::fclose(g);
    fclose(f);
    return 1;
}
bool FakeFS::copyFromNVMToDisk(const char* file_name){
    nvFileHandle f = fopen(file_name,"r");
    if (f == NO_FILE) return 0;
    FILE* g = std::fopen(file_name,"w");
    static const int L = 4096;
    static char buf[L];
    while (1){
        size_t count = fread(buf,1,L,f);
        if (count > 0){
            //printf("%s",buf);
            std::fwrite(buf,1,count,g);
        }
        if (count < L)
            break;
    }
    fclose(f);
    std::fclose(g);
    return 1;
}

ul FakeFS::fread(void *ptr, ull size, ull count, nvFileHandle stream){
    return readFile(stream,ptr,size*count);
}

ull FakeFS::fwrite(const void* buffer, ull size, ull count, nvFileHandle stream){
    return appendFile(stream,static_cast<const char*>(buffer),size*count);
}

int FakeFS::feof(nvFileHandle stream){
    return refer(stream).readCursor() >= refer(stream).totalSize();
}

//    int fseek(nvFileHandle stream, long offset, int whence);

int FakeFS::fflush(nvFileHandle stream){
    return 0;
}

nvFileHandle FakeFS::fopen(const char * file_name, const char* mode){
    ull id = NO_FILE;
    if (checkFile(file_name))
        id = dict_[file_name];
    switch (mode[0]){
    case 'w':
        if (id != NO_FILE) deleteFile(id);
        id = newFile(file_name);
        info_[id]->openType_ = nvFile::WRITE_ONLY;
        break;
    case 'r':
        if (id == NO_FILE) return NO_FILE;
        info_[id]->openType_ = nvFile::READ_ONLY;
        info_[id]->setReadCursor(0);
        break;
    case 'a':
        info_[id]->openType_ = nvFile::WRITE_APPEND;
        break;
    default:
        info_[id]->openType_ = nvFile::UNDEFINED;
        break;
    }
    return id;
}

int FakeFS::fclose(nvFileHandle stream){
    if (!checkFile(stream)) return -1;
    info_[stream]->setReadCursor(0);
    return 0;
}

int FakeFS::fseek(nvFileHandle stream, long offset, int whence){
    long a;
    long l = fileSize(stream);
    // read only. do not use it for write file, it's not supported.
    switch(whence){
    case SEEK_CUR:
        a = info_[stream]->readCursor();
        break;
    case SEEK_SET:
        a = 0;
        break;
    case SEEK_END:
        a = l;
        break;
    default:
        a = 0;
    }
    a += offset;
    if (a < 0) a = 0;
    if (a > l) a = l;
    //if (a < 0 || a > fileSize(stream)) return -1;
    info_[stream]->setReadCursor(a);
    return 0;
}
ul FakeFS::fread_ulkd(void *ptr, ull size, ull count, nvFileHandle stream){
    return fread(ptr,size,count,stream);
}
ull FakeFS::fwrite_ulkd(const void* buffer, ull size, ull count, nvFileHandle stream){
    return fwrite(buffer,size,count,stream);
}
int FakeFS::fflush_ulkd(nvFileHandle stream){
    return fflush(stream);
}

int FakeFS::AddChildren(const std::string& dir,vector<string>* result){
    int count = 0;
    auto size = dir.size();
    for (auto i = dict_.begin(); i != dict_.end(); ++i){
        auto p = i->first.find(dir);
        if (p != string::npos){
            count ++;
            result->push_back(i->first.substr(p+size+1));
        }
    }
    return count;
}
void FakeFS::print(){
    printf("Print FakeFS (%llu) \n",id_);
    for (ull i = 1; i < id_; ++i) if (info_[i] != nullptr){
        printf("ID = %llu:\n",i);
        info_[i]->print();
    }
}
