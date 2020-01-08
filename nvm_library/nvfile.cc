#include "nvfile.h"

#ifndef NEW_VERSION_NVM_FILE

nvFile::nvFile(){
    mng_ = nullptr;
    lock_ = false;
    openType_ = OpenType::UNDEFINED;
}
nvFile::nvFile(const nvFile& b){
    mng_ = b.mng_;
    lastLevelAddr_ = b.lastLevelAddr_;
    lastUsedAddr_ = b.lastUsedAddr_;
    locatesAddr_ = b.locatesAddr_;
    lastLevel_ = b.lastLevel_;
    lastUsed_ = b.lastUsed_;
    main_ = b.main_;
    for (byte i=0; i < MaximumFileLevel; ++i)
        locates[i] =b.locates[i];
    name_ = b.name_;
    readCursor_ = b.readCursor_;
    lock_ = false;
    openType_ = OpenType::UNDEFINED;
}

nvFile::nvFile(NVM_Manager *mng, nvAddr main_address){
    mng_ = mng;
    open(main_address);
    lock_ = false;
    openType_ = OpenType::UNDEFINED;
}

nvFile::nvFile(NVM_Manager *mng){
    name_ = "";
    mng_ = mng;
    lock_ = false;
    openType_ = OpenType::UNDEFINED;
    initial(0);
}
nvFile::~nvFile(){
    mng_->bind_name(name_.c_str(),nvnullptr);
    clear();
    // size = 0 but first page still exist.
    mng_->dispose(locates[0],MinimumFileSize);
    mng_->dispose(main_,NVFILE_SIZE);
}

nvAddr nvFile::location(){return main_;}
void nvFile::setName(string name){
    name_ = name;
    mng_->bind_name(name_.c_str(), main_);
}
string nvFile::name(){ return name_;}

void nvFile::addrInit(nvAddr main_address){
    main_ = main_address;
    lastLevelAddr_ = main_+sizeof(ull)*2;
    lastUsedAddr_ = lastLevelAddr_+sizeof(ull);
    locatesAddr_ = lastUsedAddr_ + sizeof(ull);
}

void nvFile::initial(ull size){
    // nvFile : [minimumFileSize][MaximumFileLevel][lastLevel][LastUsed][addr_of_1st_block]...[addr_of_last_block]
    addrInit(mng_->alloc(NVFILE_SIZE));
    mng_->write_ull(main_,MinimumFileSize);
    mng_->write_ull(main_+sizeof(ull),MaximumFileLevel);
    ull tot = 0, pageSize = 0;
    for (lastLevel_ = 0; lastLevel_ < MaximumFileLevel; ++lastLevel_){
        pageSize = MinimumFileSize << lastLevel_;
        locates[lastLevel_] = mng_->alloc(pageSize);
        mng_->write_addr(locatesAddr_+sizeof(nvAddr)*lastLevel_,locates[lastLevel_]);
        tot += pageSize;
        if (tot > size) {
            lastUsed_ = pageSize - (tot - size);
            break;
        }
    }
    mng_->write_ull(lastLevelAddr_,lastLevel_);
    mng_->write_ull(lastUsedAddr_, lastUsed_);
    readCursor_ = 0;
}

void nvFile::open(nvAddr main_address){
    addrInit(main_address);
    lastLevel_ = mng_->read_ull(lastLevelAddr_);
    lastUsed_ = mng_->read_ull(lastUsedAddr_);
    for (byte i=0; i <= lastLevel_; ++i){
        locates[i] = mng_->read_addr(locatesAddr_+sizeof(byte*)*i);
    }
    readCursor_ = 0;
}

ull nvFile::totalSize(){
    return (MinimumFileSize << lastLevel_) - MinimumFileSize + lastUsed_;
}
ull nvFile::totalSpace(){
    return (MinimumFileSize << (lastLevel_+1)) - MinimumFileSize;
}
ull nvFile::rest(){
    return totalSpace() - totalSize();
}

void nvFile::locate(ull pos, byte &page, ull &offset){
    // pos = page*SP*(2^n-1) + offset
    // page 0 : 1 SP
    // page 1 : 1+2 SP
    // page 2 : 1+2+4 SP
    ull p = pos / MinimumFileSize + 1;
    page = log2_downfit(p);
    offset = pos - (pow2(1,page)-1) * MinimumFileSize;
}

ull nvFile::sizeOfPage(ull page){
    return MinimumFileSize << page;
}

void nvFile::setSize(ull page, ull offset){
//    ull *src = new ull[2];
//    src[0] = page;
//    src[1] = offset;
//    mng_->atom_write(lastLevelAddr_,reinterpret_cast<byte*>(src),sizeof(ull)*2); // because lastUsed is followed by lastLevel, we keep atomic by write them together.
//    delete[] src;
    mng_->atom_write_ull(lastLevelAddr_, page);
    mng_->atom_write_ull(lastUsedAddr_, offset);
    // WRANING!!!!!!!!! The right operation is to combin these two value into 8 bytes.
    // Don't use Guardian.
}

void nvFile::setReadCursor(ull offset){
    readCursor_ = offset;
}

ull nvFile::readCursor(){
    return readCursor_;
}

bool nvFile::isLocked(){ return lock_; }
bool nvFile::setLock(bool lock){
    pid_t pid = getpid();
    if (isLocked() && pid != locker_)
        return 0;
    //if (lock_ == lock) return 0;
    locker_ = pid;
    lock_ = lock;
    return 1;
}

void nvFile::edit(byte* data, ull bytes, ull from){
    // not finished
    // not supported
}
// it depends on mng_->atom_write() operation.
// if we use this operation for redo_log, it will loop forever.
// it's not effective, because atom_write() takes double time.

ull nvFile::read(byte* dest, ull bytes, ull from){
    byte page;
    ull offset;
    locate(from,page,offset);
    if (!isReadable()) return 0;
    if (page > lastLevel_) return 0;
    if (page == lastLevel_){
        if (offset >= lastUsed_) return 0;
        if (offset + bytes > lastUsed_)
            bytes = lastUsed_ - offset;
        mng_->read(dest, locates[page]+offset, bytes);
        return bytes;
    }
    // page < last level
    if (offset >= sizeOfPage(page))
        {printf("Error! offset >= sizeofpage.\n"); return 0;}
    if (offset + bytes <= sizeOfPage(page)){
        mng_->read(dest, locates[page]+offset, bytes);
        return bytes;
    }
    // sizeof(page)-bytes < offset < sizeof(page)
    ull x = sizeOfPage(page) - offset;
    mng_->read(dest, locates[page]+offset, x);
    return x + read(dest+x, bytes-x, from+x);
}

ull nvFile::append(const char* data, ull bytes){
    if (bytes == 0) return 0;
    if (!isWritable()) return 0;
    byte page;
    ull offset;
    locate(bytes,page,offset);
    ull restSpace = rest(), restBytes = 0;
    if (restSpace == 0){
        printf("Error! rest space = 0\n");
        //printf("Rest() = %lld = %lld - %lld\n",rest(),totalSpace(),totalSize());
        //printf("Level : %lld, Used = %lld, byte = %lld\n",lastLevel_,lastUsed_,bytes);
        return 0;
    }
    if (bytes >= restSpace){
        restBytes = bytes - restSpace;
        bytes = restSpace;
    }
    mng_->write(locates[lastLevel_]+lastUsed_, reinterpret_cast<const byte*>(data), bytes);

    lastUsed_ += bytes;
    if (lastUsed_ >= sizeOfPage(lastLevel_)){
        //lastUsed_ = 0;
        lastUsed_ -= sizeOfPage(lastLevel_);
        lastLevel_ += 1;
        locates[lastLevel_] = mng_->alloc(sizeOfPage(lastLevel_));
        mng_->atom_write_addr(locatesAddr_+lastLevel_,locates[lastLevel_]);
    }
    if (restBytes > 0)
        return append(data+bytes,restBytes) + bytes;
    else {
        setSize(lastLevel_,lastUsed_);
        return bytes;
    }
}

ull nvFile::read(char* dest, ull bytes){
    ull readin = read(reinterpret_cast<byte*>(dest),bytes,readCursor_);
    setReadCursor(readCursor_ + readin);
    return readin;
}

void nvFile::resize(ull size) {
    byte page;
    ull offset;
    locate(size,page,offset);
    setSize(page,offset);
    for (byte i=page+1;i<=lastLevel_;++i)
        mng_->dispose(locates[i],MinimumFileSize << i);
    lastLevel_ = page;
    lastUsed_ = offset;
}

void nvFile::clear(){
    resize(0);
}

void nvFile::print(){
    printf("File [%s] :\n", name().c_str());
    printf("Size = %llu (%llu,%llu), Read Cursor = %llu\n",totalSize(), lastLevel_, lastUsed_, readCursor());
    byte *buf = new byte[256];
    printf("Text:\n");
    if (totalSize() >= 256){
        for (ull i=0; i <= lastLevel_; ++i){
            printf("%2lld (%7lld) : [",i,(i==lastLevel_?lastUsed_:pow2(MinimumFileSize,i)));
            mng_->read(buf,locates[i],15);
            for (ull j = 0; j < 15; ++j) printbyte(buf[j]);
            printf("   ...   ");
            mng_->read(buf,locates[i]+(i==lastLevel_?lastUsed_:pow2(MinimumFileSize,i))-15,15);
            for (ull j = 0; j < 15; ++j) printbyte(buf[j]);
            printf("]\n");
        }
    } else {
        ull l = totalSize();
        printf("%llu : [",l);
        mng_->read(buf,locates[0],totalSize());
        for (ull j = 0; j < l; ++j)
            printbyte(buf[j]);
        printf("]\n");
    }
    delete[] buf;
}


bool nvFile::isReadOnly(){
    return openType_ == READ_ONLY;
}
bool nvFile::isReadable(){
    return openType_ == READ_ONLY || openType_ == READ_WRITE;
}
bool nvFile::isWritable(){
    return !isLocked() &&
            (openType_ == WRITE_ONLY || openType_ == WRITE_APPEND || openType_ == READ_WRITE);
}
bool nvFile::isOpen(){
    return openType_ != CLOSED && openType_ !=UNDEFINED;
}

#endif
