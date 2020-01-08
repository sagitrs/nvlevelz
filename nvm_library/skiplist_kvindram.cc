#include "skiplist_kvindram.h"

namespace leveldb {

DramKV_Skiplist::DramKV_Skiplist()
    : memory_usage_(0),
      rnd_(0xdeadbeef),
      max_height_(1),
      head_(NewNode(BLANK_PTR, BLANK_PTR,0, kMaxHeight)),
      empty_(true) {
  for (int i = 0; i < kMaxHeight; i++) {
    SetNext(head_, i, NULL);
  }
}

DramKV_Skiplist::DramKV_Skiplist(NodePtr head)
    : memory_usage_(0),
      rnd_(0xdeadbeef),
      max_height_(1),
      head_(head),
      empty_(true) {
  for (int i = 0; i < kMaxHeight; i++) {
    if (GetNext(head_,i) != BLANK_PTR) empty_ = false;
  }
}

DramKV_Skiplist::~DramKV_Skiplist() {
    NodePtr x = head_;
    if (x == BLANK_PTR) return;
    NodePtr x_next;
    while (x != BLANK_PTR){
        x_next = GetNext(x,0);
        DisposeNode(x);
        x = x_next;
    }
}

void DramKV_Skiplist::Insert_(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion, NodePtr* prev){
    int height = RandomHeight();
    if (height > GetMaxHeight()) {
        for (int i = GetMaxHeight(); i < height; ++i)
            prev[i] = head_;
        max_height_ = height;
    }

    StaticSlice *k = new StaticSlice(key);
    StaticSlice *v = (isDeletion ? NULL : new StaticSlice(value));
    NodePtr x = NewNode(k, v, seq, height);

    for (int i = 0; i < height; i++) {
        SetNext(x, i, GetNext(prev[i],i));
        SetNext(prev[i], i, x);
    }

    empty_ = false;
}

void DramKV_Skiplist::Insert(const Slice& key, const Slice& value, uint64_t seq, bool isDeletion) {
    NodePtr prev[kMaxHeight];
    NodePtr x = FindGreaterOrEqual(key,prev);
    //assert(x == nvBLANK_PTR || !Equal(key, GetKey(x)));
    // you just can't insert the same key.
    if (x != BLANK_PTR && Equal(key, GetKey(x))){
        Update(x, value, seq, isDeletion);
        return;
    }
    Insert_(key, value, seq, isDeletion, prev);
}

bool DramKV_Skiplist::Contains(const Slice& key) const {
  NodePtr x = FindGreaterOrEqual(key, NULL);
  return x != BLANK_PTR && Equal(key, GetKey(x));
}
bool DramKV_Skiplist::TryGet(const Slice& key, std::string* &value) const {
    NodePtr x = FindGreaterOrEqual(key, NULL);
    if (x == BLANK_PTR || !Equal(key, GetKey(x))) {
        value = NULL;
        return false;
    }
    StaticSlice *v = GetValuePtr(x);
    if (v == NULL)
        value = NULL;
    else
        value = new std::string(v->data(),v->size());
    return true;
}

void DramKV_Skiplist::Insert(DramKV_Skiplist* a){
    NodePtr prev[kMaxHeight];
    for (byte i = 0; i < kMaxHeight; ++i)
        prev[i] = BLANK_PTR;
    Insert_(a, head_, max_height_+1,prev);
}

void DramKV_Skiplist::Insert_(DramKV_Skiplist* a, NodePtr x, byte height, NodePtr* prev){
    if (height == 0){
        Iterator* iter = a->NewIterator();
        NodePtr p = prev[0];
        for (iter->SeekToFirst();iter->Valid(); iter->Next()){
            StaticSlice key(iter->key());
            StaticSlice* vp = iter->valuePtr();
            StaticSlice value(vp ? *vp : StaticSlice());
            uint64_t seq(iter->seq());
            bool isDeletion(iter->isDeletion());
            if (Equal(key,GetKey(x))){
                Update(x,value.ToSlice(),seq,isDeletion);
            } else {
                prev[0] = x;
                x = GetNext(x,0);
                Insert_(key.ToSlice(), value.ToSlice(), seq, isDeletion, prev);
            }
        }
        prev[0] = p;
        return;
    }
    Node * end = GetNext(x,height);
    height --;
    Node *l, *r;
    for (l = x; l != end; l = r){
        r = GetNext(l,height);
        DramKV_Skiplist *rest;
        if (r == BLANK_PTR)
            rest = NULL;
        else
            rest = a->Cut(GetKey(r).ToSlice());
        if (!a->isEmpty()){
            if (l != x)
                FindTails(prev,x,l);
            Insert_(a,l,height,prev);
        }
        a = rest;
        if (a == NULL) break;
    }
    assert( a == NULL );
}

DramKV_Skiplist* DramKV_Skiplist::Cut(const Slice& key){
    NodePtr prev[kMaxHeight];
    NodePtr x = FindGreaterOrEqual(key,prev);
    if (x == BLANK_PTR)
        return NULL;
    NodePtr head2 = new Node(BLANK_PTR, BLANK_PTR, 0, kMaxHeight);
    for (byte i = 0; i < kMaxHeight; ++i) if (prev[i] != BLANK_PTR){
        SetNext(head2,i,GetNext(prev[i],i));
        SetNext(prev[i],i,BLANK_PTR);
    }
    return new DramKV_Skiplist(head2);
}

std::string DramKV_Skiplist::GetMiddle() {
    int top = GetMaxHeight();
    std::vector<NodePtr> a;

    for (int level = top-1; level >= 0; --level){
        for (NodePtr x = GetNext(head_,level); x; x = GetNext(x,level))
            a.push_back(x);
        if (level == 0 || a.size() >= 16) {
            assert(a.size() > 0);
            NodePtr result = a[a.size()/2];
            return GetKey(result).ToString();
        } else
            a.clear();
    }
    assert(false);
}
DramKV_Skiplist* DramKV_Skiplist::BuildFromMemtable(MemTable* mem) {
    auto DecodeFromIter = [] (leveldb::Iterator* iter, std::string &key, std::string &value, ull seq) {
        Slice key_info = iter->key();
        Slice value_info = iter->value();

        const size_t key_total_len = key_info.size();
        int key_len_len = VarintLength(key_total_len);
        size_t key_len = key_total_len - 8/* - key_len_len*/;
        const char* key_start = key_info.data()/* + key_len_len*/;
        Slice user_key(key_start,key_len);
        key = user_key.ToString();

        ull num = DecodeFixed64(key_start + key_total_len - 8);
        bool isValue = num & 0xff;
        seq = num | (~(0xffULL));

        if (isValue){
            const size_t value_total_len = value_info.size();
            int value_len_len = VarintLength(value_total_len);
            size_t value_len = value_total_len/* - value_len_len*/;
            const char* value_start = value_info.data()/* + value_len_len*/;
            Slice user_value(value_start,value_len);
            value = user_value.ToString();
        } else {
            value = "";
        }
    };
    DramKV_Skiplist* list = new DramKV_Skiplist;
    leveldb::Iterator* iter = mem->NewIterator();
    list->Append_Start();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string key, value;
        ull seq;
        DecodeFromIter(iter, key, value, seq);
        list->Append(key,value,seq);
    }
    list->Append_Stop();
    return list;
}

void DramKV_Skiplist::Append_Start() {
    for (int i = 0; i < kMaxHeight; ++i)
        assert(head_->next_[i] == nullptr);
    tailinfo_.tails_ = new NodePtr[kMaxHeight];
    for (int i = 0; i < kMaxHeight; ++i) tailinfo_.tails_[i] = nullptr;
    tailinfo_.tail_ = nullptr;
}
void DramKV_Skiplist::Append_Stop() {
    for (byte i = 0; i < kMaxHeight; ++i)
        if (tailinfo_.tails_[i] != nullptr)
            SetNext(tailinfo_.tails_[i],i,BLANK_PTR);
    if (head_->next_[0] != nullptr) {
        empty_ = false;
        for (NodePtr i = head_->next_[0]; i->next_[0] != BLANK_PTR; i = i->next_[0]) {
            assert(i->key_->compare(*(i->next_[0]->key_)) < 0);
        }
    }
}
void DramKV_Skiplist::Append(const std::string &key, const std::string &value, ull seq) {
    int height = RandomHeight();
    if (height > GetMaxHeight())
        max_height_ = height;
    NodePtr tail = tailinfo_.tails_[0];
    if (tail && tail->key_->ToSlice().compare(key) == 0) {
        StaticSlice *v = (value.empty() ? NULL : new StaticSlice(value));
        SetValue(tail,v,seq);
    } else {
        StaticSlice *k = new StaticSlice(key);
        StaticSlice *v = (value.empty() ? NULL : new StaticSlice(value));
        NodePtr x = NewNode(k, v, seq, height);
        for (int i = 0; i < height; i++) {
            if (tailinfo_.tails_[i] != nullptr)
                SetNext(tailinfo_.tails_[i], i, x);
            tailinfo_.tails_[i] = x;
            if (head_->next_[i] == nullptr)
                head_->next_[i] = x;
        }
    }
}

void DramKV_Skiplist::DramKV_Skiplist::Print() {
    static const bool fully_print = 0;
    //static const bool fully_print = 1;

    printf("Print Skiplist [Height = %d]:\n",max_height_);
    for (int level = 0; level < GetMaxHeight(); ++level){
        NodePtr l = head_;
        int count = 0;
        printf("  level %2d:\n    ",level);
        for (NodePtr r = GetNext(l,level); r != BLANK_PTR; r = GetNext(l,level)) {
            l = r;
            const StaticSlice& key = GetKey(l);
            StaticSlice* valuePtr = GetValuePtr(l);
            uint64_t seq = GetSeq(l);
            if (++count < 12 || fully_print)
                printf("[%s(%llu):%s], ",
                       key.ToString().c_str(),
                       seq,
                       (valuePtr == BLANK_PTR ? "" : valuePtr->ToString().c_str())
                );
            else if (count == 12 && !fully_print) printf("...");
        }
        printf("\n  level %2d print finished, %d ele(s) in total.\n",level,count);
    }
    printf("All Print Finished.\n");
}

};
