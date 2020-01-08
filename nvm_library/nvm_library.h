#pragma once
#include "nvfile.h"
#include "nvm_manager.h"
#include "global.h"
#include "nvm_filesystem.h"
#include <string>
#include <vector>
#include <sys/file.h>
#include <sys/stat.h>
#include <port/port_posix.h>
//#define NVM_FILE_ENABLED_0
#define NVM_FILE_ENABLED_1

using std::string;
using std::vector;
struct FakeFS;
struct NVM_Manager;
/*
namespace Sag {
    static const ull Memory_Size = (1<<28);
    static NVM_Manager* mng_;
    static FakeFS* nvmfs_;
    void nvm_init(ull size);
    void nvm_terminate();

    ul fread(void *ptr, ull size, ull count, nvFileHandle stream);
    ull fwrite(const void* buffer, ull size, ull count, nvFileHandle stream);
    int feof(nvFileHandle stream);
//    int fseek(nvFileHandle stream, long offset, int whence);
    int fflush(nvFileHandle stream);
    nvFileHandle fopen(const char * file_name, const char* mode);
    int fclose(nvFileHandle stream);

    int rename(const char *oldname, const char *newname);
    int LockOrUnlock(int fd, bool lock);
    bool fileExist(const std::string& fname);
    ull fileHandle(const std::string& fname);
    bool remove(const char* path);
    ull fileSize(const char * path);
    ull fileSize(nvFileHandle stream);
    ul readWithOffset(void *ptr, ull count, ull offset, nvFileHandle stream);

    int fseek(nvFileHandle stream, long offset, int whence);
    ul fread_ulkd(void *ptr, ull size, ull count, nvFileHandle stream);
    ull fwrite_ulkd(const void* buffer, ull size, ull count, nvFileHandle stream);
    int fflush_ulkd(nvFileHandle stream);

    int AddChildren(const std::string& dir,vector<string>* result);

    FakeFS* fileSystem();
    void print();
};*/
/*
struct NVM_Library {
 public:
  NVM_Library() { }
  virtual ~NVM_Library() {}

  virtual ul fread(void *ptr, ull size, ull count, nvFileHandle stream) = 0;
  virtual ull fwrite(const void* buffer, ull size, ull count, nvFileHandle stream) = 0;
  virtual int feof(nvFileHandle stream) = 0;
//   virtual int fseek(nvFileHandle stream, long offset, int whence);
  virtual int fflush(nvFileHandle stream) = 0;
  virtual nvFileHandle fopen(const char * file_name, const char* mode) = 0;
  virtual int fclose(nvFileHandle stream) = 0;

  virtual int rename(const char *oldname, const char *newname) = 0;
  virtual int LockOrUnlock(int fd, bool lock) = 0;
  virtual bool fileExist(const std::string& fname) = 0;
  virtual ull fileHandle(const std::string& fname) = 0;
  virtual bool remove(const char* path) = 0;
  virtual long fileSize(const char * path) = 0;
  virtual long fileSize(nvFileHandle stream) = 0;
  virtual ul readWithOffset(void *ptr, ull count, ull offset, nvFileHandle stream) = 0;

  virtual int fseek(nvFileHandle stream, long offset, int whence) = 0;
  virtual ul fread_ulkd(void *ptr, ull size, ull count, nvFileHandle stream) = 0;
  virtual ull fwrite_ulkd(const void* buffer, ull size, ull count, nvFileHandle stream) = 0;
  virtual int fflush_ulkd(nvFileHandle stream) = 0;

  virtual int AddChildren(const std::string& dir,vector<string>* result) = 0;

  virtual void print() = 0;
  virtual NVM_Manager* Manager() = 0;

  NVM_Library(const NVM_Library&) = delete;
  void operator=(const NVM_Library&) = delete;
};
*/

struct /*NVM_Library_Simulator : public*/ NVM_Library {
    NVM_Manager* mng_;
    FakeFS* nvmfs_;
    NVM_Library(ull size) :
        mng_(new NVM_Manager(size)),
        nvmfs_(new FakeFS(mng_)) {}
    ~NVM_Library(){
        delete nvmfs_;
        delete mng_;
    }
    inline ul fread(void *ptr, ull size, ull count, nvFileHandle stream){
        return nvmfs_->fread(ptr,size,count,stream);
    }
    inline ull fwrite(const void* buffer, ull size, ull count, nvFileHandle stream){
        return nvmfs_->fwrite(buffer,size,count,stream);
    }
    inline int feof(nvFileHandle stream){
        return nvmfs_->feof(stream);
    }
//    int fseek(nvFileHandle stream, long offset, int whence);
    inline int fflush(nvFileHandle stream){
        return nvmfs_->fflush(stream);
    }
    inline nvFileHandle fopen(const char * file_name, const char* mode){
        if (mode[0] == 'a' || (strlen(mode)>1 && mode[1] == '+')){
            if (!nvmfs_->checkFile(file_name))
                nvmfs_->copyFromDiskToNVM(file_name);
        }
        return nvmfs_->fopen(file_name,mode);
    }
    inline int fclose(nvFileHandle stream){
        return nvmfs_->fclose(stream);
    }

    inline int rename(const char *oldname, const char *newname){
        return nvmfs_->rename(oldname, newname);
    }
    inline int LockOrUnlock(int fd, bool lock){
        nvFileHandle file = static_cast<ull>(fd);
        if (nvmfs_->info_[file]->setLock(lock))
            return 0;
        else
            return -1;
    }
    inline bool fileExist(const std::string& fname){
        return nvmfs_->checkFile(fname);
    }
    inline ull fileHandle(const std::string& fname){
        if (!fileExist(fname)) return NO_FILE;
        return nvmfs_->dict_[fname];
    }
    inline bool remove(const char* path){
        if (!nvmfs_->checkFile(path)) return 0;
        ull id = nvmfs_->dict_[path];
        return nvmfs_->deleteFile(id);
    }
    inline long fileSize(const char * path){
        return nvmfs_->fileSize(path);
    }
    inline long fileSize(nvFileHandle stream){
        return nvmfs_->fileSize(stream);
    }
    inline ul readWithOffset(void *ptr, ull count, ull offset, nvFileHandle stream){
        return nvmfs_->refer(stream).read(
                    reinterpret_cast<byte*>(ptr),count,offset);
    }

    inline int fseek(nvFileHandle stream, long offset, int whence){
        return nvmfs_->fseek(stream,offset,whence);
    }
    inline ul fread_ulkd(void *ptr, ull size, ull count, nvFileHandle stream){
        return nvmfs_->fread_ulkd(ptr,size,count,stream);
    }
    inline ull fwrite_ulkd(const void* buffer, ull size, ull count, nvFileHandle stream){
        return nvmfs_->fwrite_ulkd(buffer,size,count,stream);
    }
    inline int fflush_ulkd(nvFileHandle stream){
        return nvmfs_->fflush_ulkd(stream);
    }

    inline int AddChildren(const std::string& dir,vector<string>* result){
        return nvmfs_->AddChildren(dir, result);
    }

    inline FakeFS* fileSystem(){ return nvmfs_; }
    NVM_Manager* Manager() { return mng_; }
    inline void print() { nvmfs_->print(); }
};
/*
struct NVM_Library_Basedon_TMPFS : public NVM_Library {
    std::string p_;

    std::map<ull,FILE*> map_;
    std::map<ull,std::string> name_;
    std::map<std::string,ull> handle_;
    ull id_;
    std::string nvm_encode(const char* name, bool code_part_only = false){
        int s0 = 0;
        std::string s1;
        std::string s2;
        for (int i = 0; name[i]; ++i){
            if (name[i] == '/' || name[i] == '\\'){
                s0++;
                s1 += std::to_string(i) + '_';
                s2 += '_';
            } else {
                s2 += name[i];
            }
        }
        if (code_part_only)
            return s2;
        return (p_ + "/" + std::to_string(s0) + "_" + s1 + s2);
    }
    std::string nvm_decode(const char* name){
        int s0, total = 0;
        bool v[256] = {};
        for (size_t i = strlen(name)-1; i; --i) v[i] = 0; v[0] = 0;
        std::string s;
        sscanf(name,"%d",&s0);
        for (int i = 0; name[i]; ++i) if (name[i] == '_'){
            if (total < s0){
                int x;
                sscanf(name+i+1,"%d",&x);
                v[x] = 1;
                total ++;
            } else {
                const char* p = name + i + 1;
                for (int j = 0; p[j]; ++j)
                    s += (v[j] ? '/' : p[j]);
                break;
            }
        }
        return s;
    }

    NVM_Library_Basedon_TMPFS(std::string tmpath) :
        NVM_Library (), p_(tmpath), id_(0) {
    }
    ~NVM_Library_Basedon_TMPFS(){
        for (auto i = name_.begin(); i != name_.end(); ++i)
            remove(nvm_encode(i->second.c_str()).c_str());
    }
    const char* nvm_name(const char* path){
        std::string s = p_ + "/" + path;
        return s.c_str();
    }

    inline ul fread(void *ptr, ull size, ull count, nvFileHandle stream){
        return std::fread(ptr,size,count,map_[stream]);
    }
    inline ull fwrite(const void* buffer, ull size, ull count, nvFileHandle stream){
        return std::fwrite(buffer,size,count,map_[stream]);
    }
    inline int feof(nvFileHandle stream){
        return std::feof(map_[stream]);
    }
//    int fseek(nvFileHandle stream, long offset, int whence);
    inline int fflush(nvFileHandle stream){
        return std::fflush(map_[stream]);
    }
    inline nvFileHandle fopen(const char * file_name, const char* mode){
        FILE* f = std::fopen(nvm_encode(file_name).c_str(),mode);
        ull id = id_;
        id_++;
        map_[id] = f;
        name_[id] = file_name;
        handle_[file_name] = id;
        return id;
    }
    inline int fclose(nvFileHandle stream){
        int result = std::fclose(map_[stream]);
        if (result == 0){
            handle_.erase(name_[stream]);
            map_.erase(stream);
            name_.erase(stream);
        }
        return result;
    }

    inline int rename(const char *oldname, const char *newname){
        return std::rename(nvm_encode(oldname).c_str(), nvm_encode(newname).c_str());
    }
    inline int LockOrUnlock(int fd, bool lock){
        errno = 0;
        struct flock f;
        memset(&f, 0, sizeof(f));
        f.l_type = (lock ? F_WRLCK : F_UNLCK);
        f.l_whence = SEEK_SET;
        f.l_start = 0;
        f.l_len = 0;        // Lock/unlock entire file
        return fcntl(fd, F_SETLK, &f);
    }
    inline bool fileExist(const std::string& fname){
        return access(nvm_encode(fname.c_str()).c_str(), F_OK) == 0;
    }
    inline ull fileHandle(const std::string& fname){
        std::string s = nvm_encode(fname.c_str());
        if (!fileExist(s.c_str())) return NO_FILE;
        return handle_[s];
    }
    inline bool remove(const char* path){
        return unlink(nvm_encode(path).c_str()) == 0;
    }
    inline long fileSize(const char * path){
        struct stat sbuf;
        if (stat(nvm_encode(nvm_name(path)).c_str(), &sbuf) != 0) {
          return -1;
        } else {
          return sbuf.st_size;
        }
    }
    inline long fileSize(nvFileHandle stream){
        struct stat sbuf;
        if (stat(
                    nvm_encode(
                        name_[stream].c_str()
                    ).c_str(), &sbuf) != 0) {
          return -1;
        } else {
          return sbuf.st_size;
        }
    }
    inline ul readWithOffset(void *ptr, ull count, ull offset, nvFileHandle stream){
        fseek(stream, static_cast<long>(offset), SEEK_SET);
        fread(ptr,1,count,stream);
    }

    inline int fseek(nvFileHandle stream, long offset, int whence){
        return std::fseek(map_[stream],offset,whence);
    }
    inline ul fread_ulkd(void *ptr, ull size, ull count, nvFileHandle stream){
        return fread(ptr,size,count,stream);
    }
    inline ull fwrite_ulkd(const void* buffer, ull size, ull count, nvFileHandle stream){
        return fwrite(buffer,size,count,stream);
    }
    inline int fflush_ulkd(nvFileHandle stream){
        return fflush(stream);
    }

    inline int AddChildren(const std::string& dir,vector<string>* result){
        //std::string dir_encoded = nvm_encode(dir.c_str(),true);
        int count = 0;
        auto length = dir.size();//dir_encoded.size();
        for (auto i = handle_.begin(); i != handle_.end(); ++i){
            auto p = i->first.find(dir);
            if (p != string::npos){
                std::string s = i->first.substr(p+length+1);
                if (s.find('_') != string::npos) {
                    printf("Warning : \'_\' in filename.\n");
                    continue;
                }
                count ++;
                result->push_back(s);
            }
        }
        return count;
    }

    inline void print() {
        printf("%lu file in nvm file system : \n",name_.size());
        for (auto i = name_.begin(); i != name_.end(); ++i){
            printf(" %llu : [%s] \n",i->first, i->second.c_str());
        }

    }
    NVM_Manager* Manager() { return nullptr; }
};
*/
