#pragma once
#include "nvm_manager.h"
#include "nvfile.h"
#include <string>
#include <vector>
#include <map>
using std::string;
using std::vector;
using std::map;

/* FakeFS: Fake File System, to provide c-like file management function.
 * it keeps information including "file name", "file id", and other file info that nvfile contains.
 * usually, if you need to use many nvfile at one time,
 * using FakeFS is a good choice.
 */
struct NVM_Manager;
struct nvFile;
using std::string;
using std::vector;
struct FakeFS {
    NVM_Manager *mng_;
    static const byte DEFAULT_FILE_LEVEL = 22;
    static const ull MAX_FILE_COUNT = 4000000;
    map<string, ull> dict_;
    //map<ull, nvFile> info_;
    nvFile** info_;
    ull id_;

    FakeFS() = delete;
    FakeFS(NVM_Manager * mng);
    ~FakeFS();

    nvFile& refer(ull id);
    bool checkFile(const std::string& fname);
    bool checkFile(ull file_id);
    ull newFile(const char* file_name);
    bool deleteFile(ull id);
    ull readFile(ull id, void* dest, ull bytes);
    ull appendFile(ull id, const char* src, ull bytes);

    int rename(const char *oldname, const char *newname);
    ll fileSize(const char * path);
    ll fileSize(nvFileHandle stream);

    ul fread(void *ptr, ull size, ull count, nvFileHandle stream);
    ull fwrite(const void* buffer, ull size, ull count, nvFileHandle stream);
    int feof(nvFileHandle stream);
//    int fseek(nvFileHandle stream, long offset, int whence);
    int fflush(nvFileHandle stream);
    nvFileHandle fopen(const char * file_name, const char* mode);
    int fclose(nvFileHandle stream);

    bool copyFromDiskToNVM(const char* file_name);
    bool copyFromNVMToDisk(const char* file_name);

    int fseek(nvFileHandle stream, long offset, int whence);
    ul fread_ulkd(void *ptr, ull size, ull count, nvFileHandle stream);
    ull fwrite_ulkd(const void* buffer, ull size, ull count, nvFileHandle stream);
    int fflush_ulkd(nvFileHandle stream);

    int AddChildren(const std::string& dir,vector<string>* result);

    void print();
};
