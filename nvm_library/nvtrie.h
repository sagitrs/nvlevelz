#ifndef NVTRIE_H
#define NVTRIE_H

#include <string>
#include "nvm_manager.h"
#include <unistd.h>
using std::string;
struct NVM_Manager;

struct nvTrie {
    //[Hash = 16 * 8 = 128][DataAddr][ChildNum]
    enum { HashOffset = 0, DataOffset = 128, ChildNumOffset = 136, TotalLength = 144 };
    nvAddr NewHash16() {
        static const byte blank[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,};
        nvAddr hash = mng_->Allocate(DataOffset);
        mng_->write_zero(hash, DataOffset);
        return hash;
    }

    ~nvTrie() {

    }

    nvTrie(NVM_Manager *mng) { // create nvTrie from nvm
        mng_ = mng;
        main_ = mng_->Allocate(TotalLength);
        mng_->write_zero(main_, TotalLength);
    }
    nvTrie(NVM_Manager *mng, nvAddr main_address) {  // read nvTrie from nvm
        mng_ = mng;
        main_ = main_address;
    }

    nvAddr Next(byte k) {
        ull hash1 = mng_->read_addr(main_ + HashOffset + ((k & 0xF0) >> 1));
        if (hash1 == nvnullptr) return nvnullptr;
        return mng_->read_addr(hash1 + (k & 0x0F) * 8);
    }
    nvAddr Data() {
        return mng_->read_addr(main_ + DataOffset);
    }
    byte ChildNum() {
        return static_cast<byte>(mng_->read_ull(main_ + ChildNumOffset));
    }
    void SetData(nvAddr dataAddr) {
        mng_->write_addr(main_ + DataOffset, dataAddr);
    }

    void SetChild(byte k, nvAddr childAddr) {
        ull hash1 = mng_->read_addr(main_ + HashOffset + ((k & 0xF0) >> 1));
        if (hash1 == nvnullptr) {
            hash1 = NewHash16();
            mng_->write_addr(main_ + HashOffset + ((k & 0xF0) >> 1), hash1);
        }
        // assert( hash1(k&0x0F) == nvnullptr );
        mng_->write_addr(hash1 + (k & 0x0F) * 8, childAddr);
        byte childNum = mng_->read_addr(main_ + ChildNumOffset);
        // assert( childNum < 255 );
        mng_->write_addr(main_ + ChildNumOffset, ++childNum);
    }

    nvAddr Find_(const string& key, size_t *pos = nullptr) {
        const size_t l = key.size();
        const char* s = key.data();
        ull hash = nvnullptr;
        byte k = 0;
        nvAddr t = main_;
        for (size_t i = 0; i < l; i++) {
            k = static_cast<byte>(s[i]);
            hash = mng_->read_addr(t + HashOffset + ((k & 0xF0) >> 1));
            if (hash == nvnullptr) {
                if (pos == nullptr)
                    return nvnullptr;
                *pos = i;
                return t;
            }
            hash = mng_->read_addr(hash + (k & 0x0F) * 8);
            if (hash == nvnullptr) {
                if (pos == nullptr)
                    return nvnullptr;
                *pos = i;
                return t;
            }
            t = hash;
        }
        return t;
    }
    nvAddr Find(const string& key) {
        nvAddr lastNode = Find_(key);
        if (lastNode == nvnullptr)
            return nvnullptr;
        return mng_->read_addr(lastNode + DataOffset);
    }

    void Insert(const string& key, nvAddr data) {
        size_t pos = 0;
        const char* s = key.data();
        nvAddr lastNode = Find_(key, &pos);
        const size_t l = key.size();
        if (pos == l) {
            mng_->write_addr(lastNode + DataOffset, data);
            return;
        }
        nvTrie* st = new nvTrie(mng_, lastNode);
        for (size_t i = pos; i < l; ++i) {
            nvTrie* child = new nvTrie(mng_);
            st->SetChild(s[i], child->main_);
            delete st;
            st = child;
        }
        st->SetData(data);
        delete st;
    }

    bool Delete_(const char* key) {
        byte childNum = ChildNum();
        if (key[0] == '\0') {
            if (Data() == nvnullptr)
                return false;
            SetData(nvnullptr);
            return (childNum == 0);
        }
        nvAddr next = Next(key[0]);
        if (next == nvnullptr)
            return false;
        nvTrie child_(mng_, next);
        if (child_.Delete_(key + 1)) {
            mng_->Dispose(child_.main_, TotalLength);
            SetChild(key[0],nvnullptr);
            mng_->write_addr(main_ + ChildNumOffset, --childNum);
        }
        return childNum == 0;
    }
    void Delete(const string& key) {
        if (key.size() == 0)
            return;
        nvAddr next = Next(key[0]);
        if (next == nvnullptr)
            return;
        nvTrie child_(mng_, next);
        child_.Delete_(key.data() + 1);
    }

    void Print(const string& key) {
        nvAddr d = Data();
        if (d != nvnullptr) {
            printf("%s => %llu\n", key.data(), d);
        }
        for (byte i = 0; i < 128; ++i) {
            nvAddr n = Next(i);
            if (n != nvnullptr) {
                nvTrie child(mng_, n);
                child.Print( key + (char)i );
            }
        }
    }
    NVM_Manager* mng_;
    nvAddr main_;

private:
};


#endif // NVTRIE_H
