#ifndef BPTREE_H
#define BPTREE_H

#include "leveldb/slice.h"
#include "global.h"
#include "util/arena.h"
#include <unordered_map>
#include <deque>

namespace leveldb {
/*
struct BpTree {
    const byte kMinSize, kMaxSize;
    const ul kKeySize;
    const ul kTotSize;
    char* head_;
    byte height_;

    char* NewNode() {
        return new char[1 + kMaxSize * kKeySize];
    }
    char* NewValue(const Slice& value) {
        char* p = new char[sizeof(ul) + value.size()];
        *reinterpret_cast<ul*>(p) = value.size();
        memcpy(p + sizeof(ul), value.data(), value.size());
        return p;
    }

    BpTree(ul keySize, byte size) :
        kMinSize(size), kMaxSize(size * 2),
        kKeySize(keySize), kTotSize(kKeySize + sizeof(char*)) {
    }

    byte Query__(char* node, const char* key) {
        byte l = static_cast<byte>(node[0]);
        int cmp = 0;
        char* p = node + 1;
        for (byte i = 0; i < l; ++i) {
            cmp = cmp_(key, p);
            if (cmp >= 0) {
                if (cmp == 0)
                    return i;
                assert(i > 0);
                return i - 1;
            }
            p += kTotSize;
        }
        return l - 1;
    }
    char* Query_(char* node, const char* key) {
        byte l = static_cast<byte>(node[0]);
        int cmp = 0;
        char* p = node + 1;
        for (int i = 0; i < l; ++i) {
            cmp = cmp_(key, p);
            if (cmp >= 0) {
                if (cmp == 0)
                    return *reinterpret_cast<char**>(p + kKeySize);
                assert(i > 0);
                return *reinterpret_cast<char**>(p - sizeof(char*));
            }
            p += kTotSize;
        }
        return *reinterpret_cast<char**>(p - sizeof (char*));
    }

    void Query(const char* key, std::string *value) {
        char* p = head_;
        for (int i = height_ - 1; i >= 0; --i) {
            p = Query_(p, key);
        }
        ul size = *reinterpret_cast<ul*>(p);
        value->assign(p + 4, size);
    }

    char* large(char* node) {
        char* s = NewNode();
        s[0] = static_cast<char>(kMinSize);
        ul l = kMinSize * kTotSize;
        memcpy(s + 1, node + 1 + l, l);
        return s;
    }

    void normalization(char* node) {
        assert(node[0] == kMaxSize);
        node[0] = kMinSize;
    }

    char* Insert_(char* node, const char* key, char* next) {
        byte l = static_cast<byte>(node[0]);
        byte pos = Query__(node, key);
        char *p = node + 1 + l * kTotSize;
        for (int i = l; i >= pos; --i)
            memcpy(p, p - kTotSize, kTotSize);
        memcpy(p, key, kKeySize);
        *reinterpret_cast<char**>(p + kKeySize) = next;
        node[0] = static_cast<char>(l+1);
        if (l+1 == kMaxSize) {
            return large(node);
        }
        return nullptr;
    }

    void Insert(const char* key, const Slice& value) {
        char* history[height_];
        history[height_ - 1] = head_;
        byte i;
        for (i = height_ - 1; i > 0; --i)
            history[i-1] = Query_(history[i], key);

        char* p = NewValue(value);
        const char* k = key;
        int j = height_;
        for (i = 0; i < height_; ++i) {
            p = Insert_(history[i], k, p);
            if (!p)
                break;
            j = i;
            k = GetHead(p);
        }
        if (i == height_) { // head split, build new head instead.
            char* new_head = NewNode();
            Insert_(new_head, GetHead(head_), head_);
            Insert_(new_head, GetHead(p), p);
            head_ = new_head;
        }
        for (int i = j; i < height_; ++i) {
            normalization(history[i]);
        }
    }
};*/

}

#endif // BPTREE_H
