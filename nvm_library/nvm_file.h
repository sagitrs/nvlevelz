#ifndef List_File_H
#define List_File_H
#include "global.h"
#include <string>
#include "nvm_manager.h"

struct AbstractFile {
    virtual ~AbstractFile();
    virtual nvAddr location() = 0;
    virtual string name() = 0;
    virtual void setName(string s) = 0;
    virtual ul readCursor() = 0;
    virtual void setReadCursor(ull offset) = 0;
    virtual bool setLock(bool lock) = 0;
    virtual bool isLocked() = 0;
    virtual bool isReadOnly() = 0;
    virtual bool isReadable() = 0;
    virtual bool isWritable()  = 0;
    virtual bool isOpen() = 0;
    virtual ull totalSize() = 0;
    virtual ull totalSpace() = 0;
    virtual ull rest() = 0;
    virtual void edit(byte* data, ull bytes, ull offset) = 0;
    virtual ull append(const char* data, ull bytes) = 0;
    virtual ull read(byte* dest, ull bytes, ull from) = 0;
    virtual ull read(char* dest, ull bytes) = 0;
    virtual void clear() = 0;
    virtual void print() = 0;
};


struct List_File : AbstractFile {
public:
    enum OpenType {READ_ONLY, WRITE_ONLY, WRITE_APPEND, READ_WRITE, CLOSED, UNDEFINED};
    enum {LengthOffset = 4, FirstBlockOffset = 8, TotalLength = 16};
    NVM_Manager* mng_;
    vector<nvAddr> locator_;
    nvAddr info_block_, last_block_;
    ul read_page_;
    ul last_used_, total_size_;
    ul read_offset_;

    OpenType openType_;
    bool lock_;
    pid_t locker_;
    string name_;

    static const uint32_t SizePerBlock = 1 * MB;
    static const uint32_t BytePerBlock = SizePerBlock - sizeof(nvAddr);

private:
    nvAddr GetNext(nvAddr block) {
        return mng_->read_addr(block + BytePerBlock);
    }
    void SetNext(nvAddr block, nvAddr next) {
        mng_->write_ull_barrier(block + BytePerBlock, next);
    }
    void NewNextPage() {
        nvAddr next = mng_->Allocate(SizePerBlock);
        locator_.push_back(next);
        SetNext(next, nvnullptr);
        SetNext(last_block_, next);
        last_block_ = next;
    }

    void locate(ull pos, ul *page, ul *offset) {
        *page = pos / BytePerBlock;
        *offset = pos % BytePerBlock;
    }   // calculate (page,offset) by pos. private.
    ul relocate(ul page, ul offset) {
        return page * BytePerBlock + offset;
    }
    void setSize(ul size) {
        total_size_ = size;
        last_used_ = size % BytePerBlock;
        mng_->write_ul_barrier(info_block_ + LengthOffset, size);
    }             // edit size of this file to (page,offset), atomicly. private.
    ul lastUsed() const { return last_used_; }

    void initial(ull size) {
        info_block_ = mng_->Allocate(TotalLength);
        last_block_ = mng_->Allocate(SizePerBlock);
        read_page_ = 0;
        locator_.push_back(last_block_);
        mng_->write_ul(info_block_, SizePerBlock);
        mng_->write_addr(info_block_ + FirstBlockOffset, last_block_);
        setSize(0);
    }                         // create a file with "size" size

    void open(nvAddr main_address) {
        info_block_ = main_address;
        setSize(mng_->read_ul(info_block_ + LengthOffset));
        last_block_ = mng_->read_addr(info_block_ + FirstBlockOffset);
        read_page_ = 0;
        read_offset_ = 0;
        assert(last_block_ != nvnullptr);
        assert(total_size_ < 2100000000);
        while (1) {
            locator_.push_back(last_block_);
            nvAddr next = GetNext(last_block_);
            if (next == nvnullptr) break;
            last_block_ = next;
        }
    }                  // read nvFile from nvm

public:
    // NVM : [BytePerBlock = 4][TotalLength = 4][FirstBlockAddr = 8]
    //List_File() {}
    ~List_File() {
        mng_->delete_name(name_);
        clear();
        // size = 0 but first page still exist.
        mng_->dispose(locator_[0], SizePerBlock);
        mng_->dispose(info_block_, TotalLength);
    }
    List_File(NVM_Manager *mng) :
        mng_(mng), locator_(),
        info_block_(nvnullptr), last_block_(nvnullptr),
        read_page_(0), last_used_(0), total_size_(0), read_offset_(0),
        openType_(UNDEFINED), lock_(false), locker_(0), name_("")
    {
        initial(0);
    }                       // create a new nvFile by mng_
    List_File(NVM_Manager *mng, nvAddr main_address) :
      mng_(mng), locator_(),
      info_block_(nvnullptr), last_block_(nvnullptr),
      read_page_(0), last_used_(0), total_size_(0), read_offset_(0),
      openType_(UNDEFINED), lock_(false), locker_(0), name_("")
    {
        open(main_address);
    }   // read nvFile from nvm


    nvAddr location() {
        return info_block_;
    }                               // return main_;

    string name() {
        return name_;
    }                                  // get file name
    void setName(string s) {
        if (name_ == s)
            return;
        mng_->bind_name(s, info_block_);
        if (name_ != "")
            mng_->delete_name(name_);
        name_ = s;
    }                         // set file name

    ul readCursor() {
        return read_page_ * BytePerBlock + read_offset_;
    }                               // get read cursor
    void setReadCursor(ull offset){
        locate(offset, &read_page_, &read_offset_);
    }                 // set read cursor

    bool setLock(bool lock) {
        pid_t pid = getpid();
        if (isLocked() && pid != locker_)
            return 0;
        //if (lock_ == lock) return 0;
        locker_ = pid;
        lock_ = lock;
        return 1;
    }
    bool isLocked() {
        return lock_;
    }

    bool isReadOnly() {
        return openType_ == READ_ONLY;
    }
    bool isReadable() {
        return openType_ == READ_ONLY || openType_ == READ_WRITE;
    }
    bool isWritable() {
        return !isLocked() &&
                (openType_ == WRITE_ONLY || openType_ == WRITE_APPEND || openType_ == READ_WRITE);
    }
    bool isOpen() {
        return openType_ != CLOSED && openType_ !=UNDEFINED;
    }

    ull totalSize() {
        return total_size_;
    }                                // get file size
    ull totalSpace() {
        return locator_.size() * BytePerBlock;
    }                               // get all space this file can use
    ull rest() {
        return totalSpace() - totalSize();
    }                                     // get rest space this file can use

    void edit(byte* data, ull bytes, ull offset) { assert(false); }
    // it depends on mng_->atom_write() operation.
    // if we use this operation for redo_log, it will loop forever.
    // it's not effective, because atom_write() takes double time.

    ull append(const char* data, ull bytes){
        if (!isWritable() || bytes == 0) return 0;
        if (bytes + lastUsed() >= BytePerBlock) {
            ul tbw = BytePerBlock - lastUsed();
            assert(tbw > 0);
            mng_->write(last_block_ + lastUsed(), reinterpret_cast<const byte*>(data), tbw);
            data += tbw;
            NewNextPage();
            ul rest;

            for (rest = bytes - tbw; rest >= BytePerBlock; rest -= BytePerBlock) {
                mng_->write(last_block_, reinterpret_cast<const byte*>(data), BytePerBlock);
                data += BytePerBlock;
                NewNextPage();
            }
            if (rest > 0)
                mng_->write(last_block_, reinterpret_cast<const byte*>(data), rest);
        } else
            mng_->write(last_block_ + lastUsed(), reinterpret_cast<const byte*>(data), bytes);
        setSize(total_size_ + bytes);
        return bytes;
    }             // append data to file.
//    ull append(char* data, ull bytes);
    // append opetaion is atomic, but not rely on redo_log in mng_.
    // that's why we can keep global atomicity.

    ull read(byte* dest, ull bytes, ull from) {
        ul page;
        ul offset;
        locate(from, &page, &offset);
        if (!isReadable()) return 0;
        if (page >= locator_.size()) return 0;
        if (page == locator_.size() - 1) {
            if (offset >= lastUsed()) return 0;
            if (offset + bytes > lastUsed())
                bytes = lastUsed() - offset;
            mng_->read(dest, locator_[page] + offset, bytes);
            return bytes;
        }
        // page < locator_.size()
        if (offset + bytes <= BytePerBlock) {
            mng_->read(dest, locator_[page]+offset, bytes);
            return bytes;
        }
        // sizeof(page)-bytes < offset < sizeof(page)
        ull x = BytePerBlock - offset;
        mng_->read(dest, locator_[page]+offset, x);
        dest += x;
        bytes -= x;
        from += x;
        ull y = x;
        for (page++; page+1 < locator_.size() && bytes >= BytePerBlock; page++) {
            mng_->read(dest, locator_[page], BytePerBlock);
            dest += BytePerBlock;
            bytes -= BytePerBlock;
            from += BytePerBlock;
            y += BytePerBlock;
        }
        return y + read(dest, bytes, from);
    }      // read data from file.
    ull read(char* dest, ull bytes) {
        ull readin = read(reinterpret_cast<byte*>(dest), bytes, readCursor());
        setReadCursor(readCursor() + readin);
        return readin;
    }

    void clear() {
        resize(0);
    }                                   // clean this file.

    void print() {
        printf("File Info:\n");
        printf("Total Size = %d (%lu, %d)\n", total_size_, locator_.size(), lastUsed());
        printf("Locker = %d, Read Cursor = %d\n", (lock_?locker_:-1), readCursor());
        char buf[17];
        buf[16] = 0;
        for (size_t i = 0; i < locator_.size(); ++i) {
            mng_->read(reinterpret_cast<byte*>(buf), locator_[i], 16);
            printf("%d(%llu) : [%s]", i, locator_[i], buf);
        }
    }                                   // for debugging.


private:
    void resize(ull size) {
        ul page, rest;
        locate(size, &page, &rest);
        last_block_ = locator_[page];
        setSize(size);

        for (ull p = page+1; p < locator_.size(); ++p)
            mng_->Dispose(locator_[p], SizePerBlock);

        locator_.erase(locator_.begin() + page + 1, locator_.end());

        SetNext(last_block_, nvnullptr);
    }                          // resize file. if file become smaller, data will lost; if larger, data will be unknown.
                                                    // private in principle.
};

struct SkiplistFile : AbstractFile {
public:
    enum OpenType {READ_ONLY, WRITE_ONLY, WRITE_APPEND, READ_WRITE, CLOSED, UNDEFINED};
    enum {BlockListAddr = 0, LengthAddr = 8, FileInfoSize = 16};
    NVM_Manager* mng_;
    nvAddr fileinfo_;
    struct FileBlockIndexSkiplist {
     private:
        struct Allocator {
        public:
            NVM_Manager* mng_;
            nvAddr main_;
            nvOffset total_size_, rest_size_;
            nvOffset node_bound_;

            Allocator(NVM_Manager* mng, ul size) :
                mng_(mng),
                main_(mng->Allocate(size)),
                total_size_(size), rest_size_(size),
                node_bound_(0)
            {
            }
            ~Allocator() {
                mng_->Dispose(main_, total_size_);
            }
            nvOffset AllocateNode(nvOffset size) {
                if (size > rest_size_) return nulloffset;
                nvOffset ans = node_bound_;
                node_bound_ += size;
                rest_size_ -= size;
                return ans;
            }

            nvAddr Main() const { return main_; }
            nvOffset Size() const { return total_size_; }
            nvOffset StorageUsage() const { return total_size_ - rest_size_; }
            nvOffset RestSpace() const { return rest_size_; }

            Allocator(const Allocator&) = delete;
            void operator=(const Allocator&) = delete;
        };
        NVM_Manager* mng_;
        Allocator arena_;
        nvOffset head_;
        byte max_height_;
        enum { HeightOffset = 0, NumOffset = 1, AddrOffset = 5, NextOffset = 13 };
        enum { kMaxHeight = 12 };

        leveldb::Random rnd_;
        int RandomHeight() {
            // Increase height with probability 1 in kBranching
            static const unsigned int kBranching = 4;
            int height = 1;
            while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
              height++;
            }
            assert(height > 0);
            assert(height <= kMaxHeight);
            return height;
        }

        nvAddr mem() const { return arena_.Main(); }

        nvOffset GetNext(nvOffset x, byte level) const {
            assert(x != nulloffset);
           return mng_->read_ul(mem() + x + NextOffset + level * 4);
        }
        void SetNext(nvOffset x, byte level, nvOffset next) {
            assert(x != nulloffset);
            mng_->write_ul(mem() + x + NextOffset + level * 4, next);
        }
        nvAddr GetAddr(nvOffset x) const {
            return mng_->read_addr(mem() + x + AddrOffset);
        }
        void SetAddr(nvOffset x, nvAddr addr) {
            mng_->write_addr(mem() + x + AddrOffset, addr);
        }
        byte GetHeight(nvOffset x) const {
            mng_->read_byte(mem() + x + HeightOffset);
        }
        nvOffset GetNum(nvOffset x) const {
            return mng_->read_ul(mem() + x + NumOffset);
        }
        nvOffset NewNode(nvOffset num, nvAddr addr, byte height, nvOffset* prev) {
           nvOffset total_size = NextOffset + 4 * height;
           byte buf[total_size];
           nvOffset x = arena_.AllocateNode(total_size);
           assert( x != nulloffset );

           byte* p = buf;
           *p = height;
           p ++;
           *reinterpret_cast<nvOffset*>(p) = num;
           p += 4;
           *reinterpret_cast<nvAddr*>(p) = addr;
           p += 8;
           if (prev)
               for (byte i = 0; i < height; ++i) {
                   *reinterpret_cast<nvOffset*>(p) = GetNext(prev[i], i);
                   p += 4;
               }
           else {
               memset(p, nulloffset, height * 4);
               p += 4 * height;
           }
           assert(buf + total_size == p);
           mng_->write(mem() + x, buf, total_size);
           return x;
       }

        nvOffset Insert(nvOffset blockNum, nvAddr blockAddr, byte height, nvOffset *prev) {
            if (height > max_height_)
                for (byte i = max_height_; i < height; ++i)
                    prev[i] = head_;

            nvOffset x = NewNode(blockNum, blockAddr, height, prev);
            if (height > max_height_) {
                for (byte i = max_height_; i < height; ++i)
                    SetNext(head_, i, x);
                max_height_ = height;
            }
            for (byte i = 0; i < height; ++i)
                SetNext(prev[i], i, x);
            return x;
        }
        void Update(nvOffset x, nvAddr blockAddr) {
            SetAddr(x, blockAddr);
        }

    public:
        FileBlockIndexSkiplist(NVM_Manager* mng, size_t size)
            : mng_(mng), arena_(mng, size), head_(nulloffset), max_height_(1), rnd_(0xDEADBEEF) {
            head_ = NewNode(nulloffset, nvnullptr, kMaxHeight, nullptr);
            for (byte i = 0; i < kMaxHeight; ++i)
                SetNext(head_, i, nulloffset);
        }
        virtual ~FileBlockIndexSkiplist() {}

      // An iterator is either positioned at a key/value pair, or
      // not valid.  This method returns true iff the iterator is valid.
      nvOffset Seek(nvOffset num, nvOffset *prev) {
            byte level = max_height_ - 1;
            static const byte min_level = 0;
            nvOffset x = head_;
            nvOffset next = nulloffset;
            int64_t cmp;

            while (true) {
                next = GetNext(x, level);
                if (next == nulloffset) {
                    cmp = -1;
                } else {
                    nvOffset next_num = GetNum(next);
                    cmp = static_cast<int64_t>(num) - next_num;
                }
                if (cmp > 0)
                    x = next;      // Right.
                else {
                    if (prev) prev[level] = x;
                    if (level <= min_level) {
                        if (prev)
                            for (byte h = 0; h < level; ++h)
                                prev[h] = x;
                        if (cmp == 0) {
                            return next;    // Found.
                        }
                        return nulloffset;       // Not Found.
                    } else {
                        // Switch to next list
                        level--;   // Down.
                    }
                }
            }
            assert(false);
            return nulloffset;
       }
      void Add(nvOffset num, nvAddr addr) {
          //const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
          nvOffset x = head_;
          nvOffset prev[kMaxHeight];
          nvOffset loc = Seek(num, prev);
          //assert(exist == false);
          if (loc == nulloffset) {
              byte height = RandomHeight();
              Insert(num, addr, height, prev);
          } else {
              Update(loc, addr);
          }
      }
      nvOffset Get(nvOffset num, nvAddr* value) {
          nvOffset iter = Seek(num, nullptr);
          if (iter == nulloffset)
              return nulloffset;         //please search from head.
          *value = GetAddr(iter);
          return iter;
      }
      void IterNext(nvOffset *iter) {
          *iter = GetNext(*iter, 0);
      }
      void IterUpdate(nvOffset iter, nvAddr addr) {
          Update(iter, addr);
      }
      nvAddr IterGet(nvOffset iter) {
          return GetAddr(iter);
      }
      nvOffset Head() const { return head_; }
      ull StorageUsage() const {
          return arena_.StorageUsage();
      }
      bool HasRoomForWrite() {
          return arena_.RestSpace() > 100;
      }

      void CheckValid() {
          nvOffset prev, key;
          for (byte level = 0; level < max_height_; ++level) {
              prev = 0;
              for (nvOffset x = GetNext(head_,level); x != nulloffset; x = GetNext(x, level)) {
                  key = GetNum(x);
                  assert(key > prev || prev == 0);
                  prev = key;
              }
          }
      }
      FileBlockIndexSkiplist(const FileBlockIndexSkiplist&) = delete;
      void operator=(const FileBlockIndexSkiplist&) = delete;
    };
    FileBlockIndexSkiplist blocks_;
    //ull total_size_;
    std::string file_name_;

    nvOffset last_page_, last_used_;
    nvAddr last_block_;

    nvOffset read_page_, read_cursor_;
    nvAddr read_block_;

    OpenType openType_;
    bool lock_;
    pid_t locker_;

    static const uint32_t SizePerBlock = 1 * MB;
    static const uint32_t BlockListSize = 20 * 4 * KB;   // Max File Size = 4000 * 1MB = 4GB

private:

    nvAddr NewPage(nvOffset num);

    void locate(ull pos, ul *page, ul *offset);
    void setSize(ul size);
    void open(nvAddr main_address);

public:
    // NVM : [BytePerBlock = 4][TotalLength = 4][FirstBlockAddr = 8]
    //List_File() {}
    ~SkiplistFile();
    SkiplistFile(NVM_Manager *mng);
    SkiplistFile(NVM_Manager *mng, const std::string& filename);
    SkiplistFile(NVM_Manager *mng, nvAddr main_address); // recover.

    nvAddr location();
    string name();                  // get file name
    void setName(string s);

    ul readCursor();
    void setReadCursor(ull offset);
    bool setLock(bool lock);
    bool isLocked();
    bool isReadOnly();
    bool isReadable();
    bool isWritable();
    bool isOpen();
    ull totalSize();
    ull totalSpace();
    ull rest();
    void edit(byte* data, ull bytes, ull offset);
    // it depends on mng_->atom_write() operation.
    // if we use this operation for redo_log, it will loop forever.
    // it's not effective, because atom_write() takes double time.

    ull append(const char* data, ull bytes);
    ull read(byte* dest, ull bytes, ull from);
    ull read(char* dest, ull bytes);
    void clear();
    void print();
private:
    void resize(ull size);
};
typedef SkiplistFile NVM_File;

#endif // List_File_H
