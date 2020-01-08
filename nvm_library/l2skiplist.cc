#include "l2skiplist.h"

namespace leveldb {
UpperSkiplist::UpperSkiplist(MixedSkipList* parent, Arena* arena, byte min_height, nvOffset lower_head) :
    parent_(static_cast<L2SkipList*>(parent)),
    arena_(arena), head_(NewNode(nullptr,kMaxHeight)), lower_head_(lower_head),
    max_height_(1), min_height_(min_height){
    for (byte i = 0; i < kMaxHeight; ++i)
        head_->SetNext(i, nullptr);
}
bool UpperSkiplist::Locate(const Slice& key, uint32_t *prev, uint32_t *pnext, Node* *dram_prev) {
     Node* x = head_;
     if (prev)
         for (byte i = 0; i < kMaxHeight; ++i)
             prev[i] = parent_->table_.Head();
     if (pnext)
         for (int i = GetMaxHeight(); i < kMaxHeight; ++i)
             pnext[i] = nulloffset;
     if (dram_prev) for (byte i = 0; i < kMaxHeight; ++i)
         dram_prev[i] = head_;
     int level = GetMaxHeight() - 1;
     while (true) {
         Node* next = x->Next(level);
         if (INFO_COLLECT) parent_->ic_.load_cache_line_in_mem_ ++;
         //if (next != nullptr && key.compare(next->GetKey()) > 0) {
         if (next != nullptr && next->GetKey().compare(key) < 0) {
            // Keep searching in this list
             x = next;
         } else {
             if (dram_prev != nullptr)
                 dram_prev[level] = x;
             if (prev != nullptr) {
                 if (x == head_)
                     prev[level] = parent_->table_.Head();
                 else {
                    x->GetValue(prev + level);
                    //assert(prev[level] == nulloffset || prev[level] < 64 * MB);
                 }
             }
             if (pnext != nullptr) {
                 if (next == nullptr)
                     pnext[level] = nulloffset;
                 else
                    next->GetValue(pnext + level);
             }
             if (level == 0)
                  return next;
             else
                  // Switch to next list
                  level--;
         }
     }
}
void UpperSkiplist::Print() {
    printf("Print Upper (cache):\n");
    printf("Height = %d ~ %d\n", min_height_, max_height_);
    for (Node* x = head_->NoBarrier_Next(0); x != nullptr; x = x->NoBarrier_Next(0)) {
        nvOffset o;
        x->GetValue(&o);
        printf("%s : %u\n", x->GetKey().ToString().c_str(), o);
    }
}
nvOffset LowerSkiplist::Locate(const Slice& key, nvOffset *prev, nvOffset *pnext) {
     byte level = max_height_ - 1;
     if (start_height_ < level)
         level = start_height_;
     nvOffset x = prev[level];

     nvOffset length = 0;
     nvOffset next = nulloffset, old_next = nulloffset;
     int cmp;
     char *data = nullptr;
     while (true) {
         next = GetNext(x, level);
         //if (INFO_COLLECT) parent_->ic_.load_cache_line_in_nvm_by_insert_ ++;
         if (next == nulloffset || next == old_next)
             cmp = -1;
         else {
             GetKey(next, &data, &length);
             cmp = key.compare(Slice(data, length));
             delete[] data;
         }
         if (cmp > 0) {
             //assert(next <= 64 * MB);
             x = next;
         } else {
             prev[level] = x;
             pnext[level] = next;
             if (cmp == 0 || level == 0)
                  return cmp == 0 ? next : nulloffset;
             else {
                 old_next = next;
                  // Switch to next list
                  level--;
             }
         }
     }
}

bool LowerSkiplist::Get(const Slice& key, std::string* value, Status* s, nvOffset x) {
    byte level = max_height_ - 1;
    if (start_height_ < level)
        level = start_height_;
    nvOffset length = 0;
    char *data = nullptr;
    int cmp;
    while (true) {
        nvOffset next = GetNext(x, level);
        //parent_->ic_.load_cache_line_in_nvm_by_query_++;
        if (next == nulloffset)
            cmp = -1;
        else {
            GetKey(next, &data, &length);
            cmp = key.compare(Slice(data, length));
            delete[] data;
        }
        if (cmp > 0)
            x = next;
        else {
            if (cmp == 0) {
                GetValue(next, &data, &length);
                if (length == 0) {
                    *s = Status::NotFound(Slice());
                    return true;
                }
                value->assign(data, length);
                delete[] data;
                return true;
            }
            if (level == 0) // cmp < 0
                return false;
            else
                 // Switch to next list
                 level--;
        }
    }
}

LowerSkiplist::LowerSkiplist(MixedSkipList* parent, NVM_Manager* mng, NVM_Linear_Allocator* narena, byte height) :
    parent_(static_cast<L2SkipList*>(parent)),
    rnd_(0xdeadbeef), mng_(mng),
    narena_(narena), main_(narena_->Main()),
    head_(NewNode("", "", 0, 0, kMaxHeight, nullptr)),
    max_height_(1), start_height_(height), garbage_size_(0)
{
    for (byte i = 0; i < kMaxHeight; ++i)
        SetNext(head_, i, nulloffset);
}
void LowerSkiplist::GetKey(nvOffset x, char* *data, nvOffset *size) {
    byte h = GetHeight(x);
    *size = GetKeySize(x);
    *data = new char[*size];
    mng_->read(reinterpret_cast<byte*>(*data), NVADDR(x + NextOffset + h * 4), *size);
    /*
    if (buffer_.size() >= MAX_BUFFER_SIZE){
        char* tmp = buffer_.front();
        buffer_.pop_front();
        delete[] tmp;
    }
    buffer_.push_back(*data);*/
}
Slice LowerSkiplist::GetKey(nvOffset x, DRAM_Buffer* buffer) {
    byte h = GetHeight(x);
    char* data; size_t size;
    size = GetKeySize(x);
    data = static_cast<char*>(buffer->Allocate(size));
    mng_->read(reinterpret_cast<byte*>(data), NVADDR(x + NextOffset + h * 4), size);
    return Slice(data, size);
}
Slice LowerSkiplist::GetOfficialKey(nvOffset x, DRAM_Buffer* buffer, ull seq) {
    byte h = GetHeight(x);
    char* data; size_t size;
    size = GetKeySize(x);
    data = static_cast<char*>(buffer->Allocate(size + 8));
    mng_->read(reinterpret_cast<byte*>(data), NVADDR(x + NextOffset + h * 4), size);

    nvOffset v = 0;
    GetValuePtr(x, &v);
    if (v == nulloffset) {
        EncodeFixed64(data + size, (seq << 8) | kTypeDeletion);
        //reinterpret_cast<ull*>(data + size) =
    } else {
        EncodeFixed64(data + size, (seq << 8) | kTypeValue);
    }
    return Slice(data, size + 8);
}
void LowerSkiplist::GetValue(nvOffset x, char* *data, nvOffset *size) {
    nvOffset v = GetValuePtr(x);
    if (v == nulloffset) { *data = nullptr; *size = 0; return; }
    *size = mng_->read_ul(NVADDR(v));
    *data = new char[*size];
    mng_->read(reinterpret_cast<byte*>(*data), NVADDR(v + 4), *size);
}
Slice LowerSkiplist::GetValue(nvOffset x, DRAM_Buffer* buffer) {
    nvOffset v = GetValuePtr(x);
    if (v == nulloffset) { return Slice(); }
    size_t size = mng_->read_ul(NVADDR(v));
    char *data = static_cast<char*>(buffer->Allocate(size));
    mng_->read(reinterpret_cast<byte*>(data), NVADDR(v + 4), size);
    return Slice(data, size);
}
nvOffset LowerSkiplist::NewValue(const Slice& value) {
    if (value.size() == 0) return nulloffset;
    nvOffset y = narena_->Allocate(value.size() + 4);
    assert(y != nulloffset);
    mng_->write_ul(NVADDR(y), value.size());
    mng_->write(NVADDR(y+4), reinterpret_cast<const byte*>(value.data()) , value.size());
    return y;
}

nvOffset LowerSkiplist::NewNode(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, byte height, nvOffset *pnext) {
    size_t total_size = 1 + 4 + 4 + 4 * height + key.size();
    nvOffset v = isDeletion ? nulloffset : NewValue(value);
    nvOffset x = narena_->Allocate(total_size);
    assert( x != nulloffset );
    byte buf[4096];

    //parent_->ic_.write_cache_line_in_nvm_ += isDeletion ? 1 : 2;
    //byte buf = new byte[total_size];
    byte *p = buf;
    *p = height;
    p ++;
    *reinterpret_cast<uint32_t*>(p) = key.size();
    p += 4;
    *reinterpret_cast<uint32_t*>(p) = v;
    p += 4;
    if (pnext)
        for (byte i = 0; i < height; ++i) {
            *reinterpret_cast<uint32_t*>(p) = pnext[i];
            p += 4;
        }
    else {
        memset(p, 0, height * 4);
        p += 4 * height;
    }
    memcpy(p, key.data(), key.size());

    mng_->write(NVADDR(x), buf, total_size);
    //delete[] buf;
    return x;
}

L2SkipList::L2SkipList(NVM_Manager *mng, const CachePolicy& cp)
    : mng_(mng), arena_(), narena_(mng_, cp.nvskiplist_size_),
      save_height_(cp.height_), save_rate_(cp.p_), cp_(cp),
      rnd_(0xdeadbeef),
      table_(reinterpret_cast<MixedSkipList*>(this), mng_, &narena_, (cp.height_ > 0 ? cp.height_ : 0)),
      cache_(reinterpret_cast<MixedSkipList*>(this), &arena_, (cp.height_ > 0 ? cp.height_ - 1 : 0), table_.Head())
{}

L2SkipList::L2SkipList(const L2SkipList& b)
    : mng_(b.mng_), arena_(), narena_(mng_, b.cp_.nvskiplist_size_),
      save_height_(b.cp_.height_), save_rate_(b.cp_.p_), cp_(b.cp_),
      rnd_(0xdeadbeef),
      table_(reinterpret_cast<MixedSkipList*>(this), mng_, &narena_, (cp_.height_ > 0 ? cp_.height_ : 0)),
      cache_(reinterpret_cast<MixedSkipList*>(this), &arena_, (cp_.height_ > 0 ? cp_.height_ - 1 : 0), table_.Head())
{}
Iterator* L2SkipList::NewIterator() {
    return new L2SkipListIterator(this);
}
Iterator* L2SkipList::NewOfficialIterator(ull seq) {
    return  new L2SkipListBoostOfficialIterator(this, seq);
    //return new L2SkipListOfficialIterator(this, seq);
}
void L2SkipList::Print() {
    printf("Mixed Skiplist :\n");/*
    cache_.Print();
    table_.Print();
    Iterator* iter = NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        Slice key = iter->key(), value = iter->value();
        printf("  %s:[%s]\n", key.ToString().c_str(), value.ToString().c_str());
        //delete[] key.data();
        //delete[] value.data();
    }*/
    printf("Info Collection : \n");
    printf("Insert = %d, Query = %d\n",ic_.insert_, ic_.query_);
    ull total_load = ic_.load_cache_line_in_mem_ + ic_.load_cache_line_in_nvm_;
    printf("Load Operation = %llu = %llu (%.2f%%) in mem + %llu (%.2f%%)\n",
           total_load,
           ic_.load_cache_line_in_mem_, 100.0 * ic_.load_cache_line_in_mem_ / total_load,
           ic_.load_cache_line_in_nvm_, 100.0 * ic_.load_cache_line_in_nvm_ / total_load
           );
    printf("Load by Insert = %.2f (%llu / %llu),  Load by Query = %.2f (%llu / %llu)\n",
           1.0 * ic_.load_cache_line_in_nvm_by_insert_ / ic_.insert_,
           ic_.load_cache_line_in_nvm_by_insert_,
           ic_.insert_,
           1.0 * ic_.load_cache_line_in_nvm_by_query_ / ic_.query_,
           ic_.load_cache_line_in_nvm_by_query_,
           ic_.query_);
}

void L2SkipList::Inserts(MemTable* mem) {
    auto GetUserKey = [](const Slice& key) { return Slice(key.data(), key.size() - 8); };
    Iterator* iter = mem->NewIterator();
     byte level = 0;
     nvOffset prev[kMaxHeight];
     nvOffset next[kMaxHeight];
     UpperSkiplist::Node* dram_prev[kMaxHeight];
     for (size_t i = 0; i < kMaxHeight; ++i) {
        prev[i] = table_.Head();
        next[i] = table_.GetNext(prev[i], i);
        dram_prev[i] = cache_.head_;
     }
     byte maxEditHeight = 0;
     Slice key, value;

     for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        while (iter->Valid() && (key.compare(GetUserKey(iter->key())) == 0))
            iter->Next();
        if (!iter->Valid()) break;
        assert(key.compare(GetUserKey(iter->key())) < 0);
        key = GetUserKey(iter->key());
        value = iter->value();

        //assert(dram_prev[0]->GetKey().compare(key) <= 0);
        for (UpperSkiplist::Node* next = dram_prev[0]->Next(0); next != nullptr; next = next->Next(0)) {
            if (next->GetKey().compare(key) < 0) {
                byte height = next->GetHeight();
                if (height > maxEditHeight)
                    maxEditHeight = height;
                for (byte h = 0; h < height; ++h)
                    dram_prev[h]  = next;
            } else
                break;
        }
        for (byte h = 0; h < maxEditHeight; ++h) {
            dram_prev[h]->GetValue(prev + h, nullptr);
            UpperSkiplist::Node* n = dram_prev[h]->Next(h);
            if (n)
                n->GetValue(next + h, nullptr);
            else
                next[h] = nulloffset;
        }
        maxEditHeight = 0;
        nvOffset l = table_.Locate(key, prev, next);
        if (l == nulloffset) {
            byte level = 0;
            nvOffset x = table_.Insert(key, value, 0, value.size() == 0, prev, next, &level);
             if (CacheSave(level) && cache_.HasRoomFor(key, x, cp_.node_cache_size_))
                cache_.Add(key, x, level, dram_prev);
         } else
            table_.Update(l, key, value, 0, iter->value().size() == 0);
     }
    delete iter;
    CheckValid();
}


void L2SkipList::ConnectUpper(UpperSkiplist* a, UpperSkiplist* b, bool reverse, nvOffset offset) {
    a->arena_->AppendBlocksOf(*b->arena_);
    b->arena_->StopDispose();

    for (UpperSkiplist::Node* x = b->head_->Next(0); x != nullptr; x = x->Next(0))
        x->MoveValue(offset);

    UpperSkiplist::Node* tails[UpperSkiplist::kMaxHeight];
    UpperSkiplist::Node* heads[UpperSkiplist::kMaxHeight];
    if (reverse) {
        b->GetTail(tails);
        for (int i = 0; i < UpperSkiplist::kMaxHeight; ++i)
            heads[i] = b->head_->Next(i);
        for (int i = 0; i < UpperSkiplist::kMaxHeight; ++i) {
            Slice key1 = tails[i] == b->head_ ? "" : tails[i]->GetKey();
            Slice key2 = a->head_->Next(i) == nullptr ? "" : a->head_->Next(i)->GetKey();
            assert(key1 == "" || key2 == "" || key1.compare(key2) < 0);
            tails[i]->SetNext(i, a->head_->Next(i));
            a->head_->SetNext(i, heads[i]);
        }
    } else {
        a->GetTail(tails);
        for (int i = 0; i < UpperSkiplist::kMaxHeight; ++i)
            heads[i] = b->head_->Next(i);
        for (int i = 0; i < UpperSkiplist::kMaxHeight; ++i) {
            tails[i]->SetNext(i, heads[i]);
        }
    }
    if (b->max_height_ > a->max_height_)
        a->max_height_ = b->max_height_;
}

void L2SkipList::ConnectLower(LowerSkiplist* a, LowerSkiplist *b, bool reverse,  nvOffset offset) {
    byte* mem = new byte[b->narena_->StorageUsage()];
    b->mng_->read(mem, b->narena_->Main(), b->narena_->StorageUsage());

    nvOffset x = b->head_;
    nvOffset* nexts = nullptr;
    nvOffset* valueptr = nullptr;
    while (x != nulloffset) {
        byte height = mem[x];
        valueptr = reinterpret_cast<nvOffset*>(mem + x + LowerSkiplist::ValueOffset);
        if (*valueptr != nulloffset)
            *valueptr += offset;
        nexts = reinterpret_cast<nvOffset*>(mem + x + LowerSkiplist::NextOffset);
        x = nexts[0];
        for (int i = 0; i < height; ++i)
            if (nexts[i] != nulloffset)
                nexts[i] += offset;
    }
    a->narena_->Append(mem, b->narena_->StorageUsage());

    nvOffset heads[LowerSkiplist::kMaxHeight];
    nvOffset tails[LowerSkiplist::kMaxHeight];
    if (reverse) {
        b->GetTail(tails);
        for (byte i = 0; i < LowerSkiplist::kMaxHeight; ++i) {
            if (tails[i] != nulloffset)
                tails[i] += offset;
            heads[i] = b->GetNext(b->head_, i);
            if (heads[i] != nulloffset)
                heads[i] += offset;
        }

        for (byte i = 0; i < LowerSkiplist::kMaxHeight; ++i) {
            if (tails[i] != nulloffset) {
                a->SetNext(tails[i], i, a->GetNext(a->head_, i));
                a->SetNext(a->head_, i, heads[i]);
            }
       }
    }
    else {
        a->GetTail(tails);
        for (byte i = 0; i < LowerSkiplist::kMaxHeight; ++i) {
            heads[i] = b->GetNext(b->head_, i);
            if (heads[i] != nulloffset)
                heads[i] += offset;

        }
        for (byte i = 0; i < LowerSkiplist::kMaxHeight; ++i) {
            if (tails[i] != nulloffset)
                a->SetNext(tails[i], i, heads[i]);
            else
                a->SetNext(a->head_, i, heads[i]);
        }
    }
    delete[] mem;
    if (b->max_height_ > a->max_height_)
            a->max_height_ = b->max_height_;
}


void L2SkipList::Connect(MixedSkipList* b, bool reverse) {
    nvOffset offset = 0;
    offset = this->narena_.StorageUsage();
    ConnectLower(&this->table_, &static_cast<L2SkipList*>(b)->table_, reverse, offset);
    ConnectUpper(&this->cache_, &static_cast<L2SkipList*>(b)->cache_, reverse, offset);
    //this->CheckValid();
}

void L2SkipList::CheckValid() {
    //return;
    for (byte level = 0; level < kMaxHeight; ++level) {
        for (nvOffset x = table_.GetNext(table_.Head(), level); x != nulloffset; x = table_.GetNext(x, level)) {
            nvOffset n = table_.GetNext(x, level);
            byte h = table_.GetHeight(x);
            byte* x_ = (byte*)mng_->main_block_->Decode(table_.NVADDR(x));
            byte* n_ = (byte*)mng_->main_block_->Decode(table_.NVADDR(n));
            assert(h > level && h <= kMaxHeight);
            //assert(table_.GetHeight(n) >= level);
            assert(n == nulloffset || table_.GetHeight(n) > level && table_.GetHeight(n) <= kMaxHeight);

            char* data; nvOffset size;
            table_.GetKey(x, &data, &size);
            assert(size > 0);
            if (n != nulloffset) {
                char* next_data; nvOffset next_size;
                table_.GetKey(n, &next_data, &next_size);
                Slice a(data,size), b(next_data, next_size);
                assert(a.compare(b) < 0);
                delete[] next_data;
            }
            delete[] data;
        }
        //Slice key;
        for (UpperSkiplist::Node* x = cache_.GetHead()->Next(level); x != nullptr; x = x->Next(level)) {
            byte height = x->GetHeight();
            Slice key(x->GetKey());
            UpperSkiplist::Node* n = x->Next(level);
            nvOffset v; x->GetValue(&v);
            byte* x_ = (byte*)table_.mng_->main_block_->Decode(table_.main_ + v);

            byte height_x = table_.GetHeight(v);

            assert(level < height && height <= UpperSkiplist::kMaxHeight);
            assert(key.size() > 0);
            assert(n == nullptr || key.compare(n->GetKey()) < 0);
            //assert(v >= 0 && v < 128 * MB);

            assert(height == height_x);
            char* data; nvOffset size;
            table_.GetKey(v, &data, &size);
            Slice key_x(data, size);
            assert( key.compare(key_x) == 0 );
            delete[] data;
        }
    }

}

void L2SkipList::GarbageCollection() {
   nvOffset l = narena_.StorageUsage();
   byte* mem[2];
   mem[0] = new byte[l];
   mem[1] = new byte[l];
   std::map<nvOffset, nvOffset> dict;
   mng_->read(mem[0], narena_.Main(), l);

   nvOffset info_ptr = 0, value_ptr = 0;
   for (nvOffset x = table_.Head();
        x != nulloffset;
        x = *reinterpret_cast<nvOffset*>(mem[0] + x + LowerSkiplist::NextOffset)) {
        byte h = mem[0][x];
        assert(h <= LowerSkiplist::kMaxHeight);
        nvOffset *nexts = reinterpret_cast<nvOffset*>(mem[0] + x + LowerSkiplist::NextOffset);
        assert(nexts[-2] <= 100);
        //assert(nexts[-1] == nulloffset || nexts[-1] <= 17 * MB);
        size_t length = 1 + 4 + 4 + 4 * h + 1 * nexts[-2];
        dict[x] = info_ptr;
        info_ptr += length;
        if (nexts[-1] != nulloffset) {
            nvOffset *value_size = reinterpret_cast<nvOffset*>(mem[0] + nexts[-1]);
            assert(*value_size < 1024);
            memcpy(mem[1] + value_ptr, mem[0] + nexts[-1], *value_size + 4);
            nexts[-1] = value_ptr;
            value_ptr += *value_size + 4;
        }
   }

   nvOffset p = value_ptr, next;
   for (nvOffset x = table_.Head();
        x != nulloffset;
        x = next) {
       byte h = mem[0][x];
       nvOffset *nexts = reinterpret_cast<nvOffset*>(mem[0] + x + LowerSkiplist::NextOffset);
       size_t length = 1 + 4 + 4 + 4 * h + 1 * nexts[-2];
       Slice key(reinterpret_cast<char*>(mem[0] + x + LowerSkiplist::NextOffset + 4 * h), nexts[-2]);
       next = nexts[0];
       for (byte i = 0; i < h; ++i)
           if (nexts[i] != nulloffset)
            nexts[i] = dict[nexts[i]] + value_ptr;
       assert(p == value_ptr + dict[x]);
       memcpy(mem[1] + p, mem[0] + x, length);
       p += length;
   }
    assert(p == value_ptr + info_ptr);

   for (UpperSkiplist::Node* x = cache_.head_->Next(0); x != nullptr; x = x->Next(0)) {
       nvOffset v;
       x->GetValue(&v);
       x->SetValue(dict[v] + value_ptr);
   }
   /*
   while (x0 != nulloffset) {
       byte h = mem[0][x0];
       nvOffset *nexts = reinterpret_cast<nvOffset*>(mem[0] + x0 + LowerSkiplist::NextOffset);
       lower_key = Slice(reinterpret_cast<char*>(mem[0] + x0 + LowerSkiplist::NextOffset + 4 * h), nexts[-2]);
       int cmp = upper_key.compare(lower_key);
       while (u && cmp < 0) {
           u = u->Next(0);
           if (u == nullptr) {
               break;
           }
           upper_key = u->GetKey();
           cmp = upper_key.compare(lower_key);
       }
       if (u && cmp == 0)
           u->SetValue(x);

       size_t length = 1 + 4 + 4 + 4 * h + 1 * nexts[-2];
       memcpy(mem[1]+x, mem[0]+x0, length);
       x += length;
       if (nexts[-1] != nulloffset) {
           nvOffset *value_size = reinterpret_cast<nvOffset*>(mem[0] + nexts[-1]);
           if (*value_size > 0) {
               memcpy(mem[1]+x, mem[0]+nexts[-1], *value_size);
               x += *value_size;
           }
       }
       x0 = nexts[0];
   }
   */
   narena_.Reset(mem[1], p);
   table_.head_ = value_ptr;
   table_.garbage_size_ = 0;
   delete[] mem[0];
   delete[] mem[1];
   CheckValid();
}

};
