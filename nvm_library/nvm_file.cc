#include "nvm_file.h"

AbstractFile::~AbstractFile() {}

    nvAddr SkiplistFile::NewPage(nvOffset num) {
        nvAddr page = mng_->Allocate(SizePerBlock);
        blocks_.Add(num, page);
        return page;
    }

    void SkiplistFile::locate(ull pos, ul *page, ul *offset) {
        pos += FileInfoSize;
        *page = pos / SizePerBlock;
        *offset = pos % SizePerBlock;
    }   // calculate (page,offset) by pos. private.
    void SkiplistFile::setSize(ul size) {
        mng_->write_ull(fileinfo_ + LengthAddr, size);
    }
    void SkiplistFile::open(nvAddr main_address) {
        assert(false);
    }
    SkiplistFile::~SkiplistFile() {
        mng_->delete_name(file_name_);
        clear();
        // size = 0 but first page still exist.
        mng_->dispose(fileinfo_, SizePerBlock);
    }
    SkiplistFile::SkiplistFile(NVM_Manager *mng) :
        mng_(mng), fileinfo_(nvnullptr), blocks_(mng, BlockListSize), file_name_(""),
        last_page_(0), last_used_(FileInfoSize), last_block_(nvnullptr),
        read_page_(0), read_cursor_(FileInfoSize), read_block_(nvnullptr),
        openType_(UNDEFINED), lock_(false), locker_(0)
    {
        last_block_ = mng_->Allocate(SizePerBlock);
        fileinfo_ = last_block_;
        read_block_ = last_block_;
        blocks_.Add(0, fileinfo_);
        mng_->write_addr(fileinfo_ + BlockListAddr, blocks_.Head());
        mng_->write_ull(fileinfo_ + LengthAddr, 0);
        //mng_->bind_name(filename, fileinfo_);
    }
    SkiplistFile::SkiplistFile(NVM_Manager *mng, const std::string& filename) :
        mng_(mng), fileinfo_(nvnullptr), blocks_(mng, BlockListSize), file_name_(filename),
        last_page_(0), last_used_(FileInfoSize), last_block_(nvnullptr),
        read_page_(0), read_cursor_(FileInfoSize), read_block_(nvnullptr),
        openType_(UNDEFINED), lock_(false), locker_(0)
    {
        last_block_ = mng_->Allocate(SizePerBlock);
        fileinfo_ = last_block_;
        read_block_ = last_block_;
        blocks_.Add(0, fileinfo_);
        mng_->write_addr(fileinfo_ + BlockListAddr, blocks_.Head());
        mng_->write_ull(fileinfo_ + LengthAddr, 0);
        mng_->bind_name(filename, fileinfo_);
    }
    SkiplistFile::SkiplistFile(NVM_Manager *mng, nvAddr recover_address) :
        mng_(mng), fileinfo_(nvnullptr), blocks_(mng, BlockListSize), file_name_(""),
        last_page_(0), last_used_(FileInfoSize), last_block_(nvnullptr),
        read_page_(0), read_cursor_(FileInfoSize), read_block_(nvnullptr),
        openType_(UNDEFINED), lock_(false), locker_(0)
    {
        last_block_ = mng_->Allocate(SizePerBlock);
        fileinfo_ = last_block_;
        read_block_ = last_block_;
        blocks_.Add(0, fileinfo_);
        mng_->write_addr(fileinfo_ + BlockListAddr, blocks_.Head());
        mng_->write_ull(fileinfo_ + LengthAddr, 0);
        //mng_->bind_name(filename, fileinfo_);
        assert( false ); // recover is not finished.
    }
    nvAddr SkiplistFile::location() {
        return fileinfo_;
    }                               // return main_;

    string SkiplistFile::name() {
        return file_name_;
    }                                  // get file name
    void SkiplistFile::setName(string s) {
        if (file_name_ == s)
            return;
        mng_->bind_name(s, fileinfo_);
        if (file_name_ != "")
            mng_->delete_name(file_name_);
        file_name_ = s;
    }                         // set file name

    ul SkiplistFile::readCursor() {
        return read_page_ * SizePerBlock + read_cursor_ - FileInfoSize;
    }                               // get read cursor
    void SkiplistFile::setReadCursor(ull offset){
        locate(offset, &read_page_, &read_cursor_);
    }                 // set read cursor

    bool SkiplistFile::setLock(bool lock) {
        pid_t pid = getpid();
        if (isLocked() && pid != locker_)
            return 0;
        //if (lock_ == lock) return 0;
        locker_ = pid;
        lock_ = lock;
        return 1;
    }
    bool SkiplistFile::isLocked() {
        return lock_;
    }

    bool SkiplistFile::isReadOnly() {
        return openType_ == READ_ONLY;
    }
    bool SkiplistFile::isReadable() {
        return openType_ == READ_ONLY || openType_ == READ_WRITE;
    }
    bool SkiplistFile::isWritable() {
        return !isLocked() &&
                (openType_ == WRITE_ONLY || openType_ == WRITE_APPEND || openType_ == READ_WRITE);
    }
    bool SkiplistFile::isOpen() {
        return openType_ != CLOSED && openType_ !=UNDEFINED;
    }

    ull SkiplistFile::totalSize() {
        return last_page_ * SizePerBlock + last_used_ - FileInfoSize;
    }                                // get file size
    ull SkiplistFile::totalSpace() {
        return last_page_ * SizePerBlock + SizePerBlock - FileInfoSize;
    }                               // get all space this file can use
    ull SkiplistFile::rest() {
        return SizePerBlock - last_used_;
    }                                     // get rest space this file can use

    void SkiplistFile::edit(byte* data, ull bytes, ull offset) { assert(false); }

    ull SkiplistFile::append(const char* data, ull bytes) {
        if (!isWritable() || bytes == 0) return 0;
        nvOffset total_size = bytes;
        while (total_size > 0) {
            bool full = last_used_ + total_size >= SizePerBlock;
            nvOffset size = (full ? SizePerBlock - last_used_ : total_size);
            assert(size > 0);
            mng_->write(last_block_ + last_used_, reinterpret_cast<const byte*>(data), size);
            data += size;
            last_used_ += size;
            total_size -= size;
            if (full) {
                last_block_ = NewPage(++last_page_);
                last_used_ = 0;
            }
        }
        setSize(totalSize());
        return bytes;
    }
    ull SkiplistFile::read(byte* dest, ull bytes, ull from) {
        if (!isReadable()) return 0;
        if (from >= totalSize()) return 0;
        if (from + bytes > totalSize())
            bytes = totalSize() - from;
        nvOffset total_size = bytes;

        nvOffset page, offset;
        locate(from, &page, &offset);
        nvAddr read_block = nvnullptr;
        nvOffset iter = blocks_.Get(page, &read_block);
        assert(iter != nulloffset);

        while (total_size > 0) {
            bool full = offset + total_size >= SizePerBlock;
            nvOffset size = (full ? SizePerBlock - offset : total_size);
            assert(size > 0);
            mng_->read(reinterpret_cast<byte*>(dest), read_block + offset, size);
            dest += size;
            offset += size;
            total_size -= size;
            if (full) {
                blocks_.IterNext(&iter);
                assert(iter != nulloffset);
                read_block = blocks_.IterGet(iter);
                offset = 0;
            }
        }
        return bytes;
    }      // read data from file.
    ull SkiplistFile::read(char* dest, ull bytes) {
        ull from = readCursor();
        ull readin = read(reinterpret_cast<byte*>(dest), bytes, from);
        setReadCursor(from + readin);
        return readin;
    }

    void SkiplistFile::clear() {
        resize(0);
    }                                   // clean this file.

    void SkiplistFile::print() {
    }                                   // for debugging.


    void SkiplistFile::resize(ull size) {
        setSize(size);
        ul page, used;
        locate(size, &page, &used);
        nvAddr abandoned_block = nvnullptr;
        nvOffset iter = blocks_.Get(page + 1, &abandoned_block);
        while (iter != nulloffset) {
            mng_->Dispose(abandoned_block, SizePerBlock);
            blocks_.IterUpdate(iter, nvnullptr);
            blocks_.IterNext(&iter);
            if (iter != nulloffset) {
                abandoned_block = blocks_.IterGet(iter);
            } else {
                abandoned_block = nvnullptr;
            }
        }
        last_page_ = page;
        last_used_ = used;
        assert ( blocks_.Get(page, &last_block_)  != nulloffset );
    }

