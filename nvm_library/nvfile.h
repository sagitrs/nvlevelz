#pragma once
#include <string>
#include "nvm_manager.h"
#include <pthread.h>
#include <unistd.h>
#include "nvm_file.h"
using std::string;
struct NVM_Manager;
#define NO_FILE 0
#define NEW_VERSION_NVM_FILE 1

#ifdef NEW_VERSION_NVM_FILE
//#define nvFile NVM_File
#define nvFile SkiplistFile
#else

struct nvFile {
    static const ull MinimumFileSize = 4096;
    static const ull MaximumFileLevel = 32;     // largest file block is 4096 << 32 = 2^44 bytes = 16TB
    static const ull NVFILE_SIZE = sizeof(ull)*4+sizeof(nvAddr)*MaximumFileLevel;
    enum OpenType {READ_ONLY, WRITE_ONLY, WRITE_APPEND, READ_WRITE, CLOSED, UNDEFINED};
    nvFile();
    ~nvFile();
    nvFile(const nvFile& b);
    nvFile(NVM_Manager *mng);                       // create a new nvFile by mng_
    nvFile(NVM_Manager *mng, nvAddr main_address);   // read nvFile from nvm

    void initial(ull size);                         // create a file with "size" size
    void open(nvAddr main_address);                  // read nvFile from nvm

    nvAddr location();                               // return main_;
    void setName(string s);                         // set file name
    void setReadCursor(ull offset);                 // set read cursor
    ull readCursor();                               // get read cursor
    string name();                                  // get file name

    bool isLocked();
    bool setLock(bool lock);
    bool isReadOnly();
    bool isReadable();
    bool isWritable();
    bool isOpen();

    ull totalSize();                                // get file size
    ull totalSpace();                               // get all space this file can use
    ull rest();                                     // get rest space this file can use

    void edit(byte* data, ull bytes, ull offset);   // not supported.
    // it depends on mng_->atom_write() operation.
    // if we use this operation for redo_log, it will loop forever.
    // it's not effective, because atom_write() takes double time.

    ull append(const char* data, ull bytes);             // append data to file.
//    ull append(char* data, ull bytes);
    // append opetaion is atomic, but not rely on redo_log in mng_.
    // that's why we can keep global atomicity.

    ull read(byte* dest, ull bytes, ull from);      // read data from file.
    ull read(char* dest, ull bytes);

    void clear();                                   // clean this file.
    void print();                                   // for debugging.

    OpenType openType_;
    NVM_Manager* mng_;
    ull lastLevel_, lastUsed_;
    ull readCursor_;
    bool lock_;
    pid_t locker_;
    nvAddr locates[MaximumFileLevel];
    nvAddr main_, lastLevelAddr_, lastUsedAddr_, locatesAddr_;
    // nvFile : [minimumFileSize][MaximumFileLevel][lastLevel][LastUsed][addr_of_1st_block]...[addr_of_last_block]
    string name_;

private:

    void addrInit(nvAddr main_address);             // set default address by main_address, no meaning. private.
    void locate(ull pos, byte &page, ull &offset);   // calculate (page,offset) by pos. private.
    ull sizeOfPage(ull page);                       // get size of "page" page. private.
    void setSize(ull page, ull offset);             // edit size of this file to (page,offset), atomicly. private.

    void resize(ull size);                          // resize file. if file become smaller, data will lost; if larger, data will be unknown.
                                                    // private in principle.
};

#endif
