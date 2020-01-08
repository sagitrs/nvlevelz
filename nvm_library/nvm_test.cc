#include "global.h"
#include "sysnvm.h"
#include "nvm_manager.h"
#include "randomizer.h"
#include "nvm_filesystem.h"
#include "nvm_library.h"
#include <algorithm>
#define MAX_MEMORY (1<<20)
#include <stdlib.h>
#include <stdio.h>
#include <fstream>
#include <iostream>
//#include "nvskiplist.h"
#include "table/format.h"
//#include "nvmemtable.h"
#include "leveldb/comparator.h"
#include <string.h>
#include "arena.h"
#include "db/dbformat.h"
#include "leveldb/options.h"
#include "multitable.h"
#include "leveldb/iterator.h"
#include <map>
#include "skiplist_kvindram.h"
#include "trie.h"
#include "trie.h"
#include "nvskiplist.h"
#include "nvtrie.h"
#include "sys/time.h"
#include "l2skiplist.h"
#include "trie_compressed.h"
#include "nvm_file.h"
#include "bptree.h"
#include "buffer_and_log.h"
#include "leveldb/write_batch.h"
#include "util/testutil.h"
#include "d1skiplist.h"
#include "d2skiplist.h"
#include "d3skiplist.h"
#include "d4skiplist.h"
#include "benchmark.h"
#include "nvhash.h"
#include "nvhashtable.h"

//using leveldb::nvSkipList;
//using leveldb::L2MemTable;
//using leveldb::InternalKeyComparator;

void cleaner(byte* blackbox){
    return;
}

Randomizer R;
//NVM_Library* lib = new NVM_Library(1ULL<<32);

struct IntComparator {
  int operator()(const int a, const int b) const {
      if (a < b) return -1;
      if (a == b) return 0;
      return 1;
  }
};
struct StringComparator {
  int operator()(const char* a, const char* b) const {
      if (a == nullptr || b == nullptr){
          if (a == nullptr) return -1;
          return 1;
      }
      size_t la = strlen(a), lb = strlen(b);
      if (la != lb)
          return (la < lb ? -1 : 1);
      return memcmp(a,b,la);
  }
};

void Trie_Test(){
    Randomizer R;
    Trie<int>* trie = new Trie<int>;
    int p[256];
    char buf[256];
    int* result;
    for (int i = 0; i < 10000; ++i){
        int x = rand() % 256;
        p[x] = x;
        size_t l = R.randomInt() % 16 + 1;
        R.setStringMode(l);
        string key = R.randomString();
        const char* buf = key.c_str();
        switch (rand() % 3){
        case 0: //Insert
            printf("Insert [%s] -> %d\n",buf,p[x]);
            trie->Insert(buf,p+x);
            break;
        case 1: //Delete
            printf("Delete [%s]",buf);
            printf(" -> %s\n", (trie->Delete(buf) ? "Succeed" : "Failed"));
            break;
        case 2: //FuzzyFind
            printf("FuzzyFind [%s] -> ",buf);
            result = trie->FuzzyFind(buf);
            if (result) printf("%d\n",*result);
            else printf("x\n");
            break;
        }
        trie->Print();

    }
    trie->Print();
}

void NVTrie_Test() {

    NVM_Manager* mng = new NVM_Manager(1 << 28);
    nvTrie main(mng);
    main.Insert("Hello", 1);
    main.Insert("Holy", 2);
    main.Insert("Helon", 3);
    main.Insert("World", 4);
    main.Print("");
    printf("Hello = %llu\n", main.Find("Hello"));
    printf("Holy = %llu\n", main.Find("Holy"));
    printf("Helon = %llu\n", main.Find("Helon"));
    printf("World = %llu\n", main.Find("World"));

    main.Delete("Holy");
    main.Delete("Hello");
    main.Delete("Helon");
    main.Delete("World");
    printf("Hello = %llu\n", main.Find("Hello"));
    printf("Holy = %llu\n", main.Find("Holy"));
    printf("Helon = %llu\n", main.Find("Helon"));
    printf("World = %llu\n", main.Find("World"));
    main.Print("");
}

struct NVM_Delay_Simulator {
    const ull w_delay;
    const ull r_delay;
    ull w_rest, r_rest;
    std::mutex mutex_;
    const ull cache_line;
    const ull flush_limit;

    void nanodelay2(ull nanosecond) {
         struct timespec req, rem;
         static const int least_delayer = 2;
         if (nanosecond < least_delayer) return;
         req.tv_nsec = nanosecond - least_delayer;
         req.tv_sec = 0;
         //nanosleep(&req,&rem);
    }
    NVM_Delay_Simulator(const NVM_Options& options):
        w_delay(options.write_delay_per_cache_line),
        r_delay(options.read_delay_per_cache_line),
        w_rest(0), r_rest(0),
        cache_line(options.cache_line_size),
        flush_limit(cache_line * 1000)
    {
    }
    inline void readDelay(ull operation){
        r_rest += operation;
        if (r_rest >= flush_limit) {
            //mutex_.lock();
            nanodelay2(r_rest * r_delay / cache_line);
            r_rest -= flush_limit;
            //mutex_.unlock();
        }
    }

    inline void writeDelay(ull operation){
        w_rest += operation;
        if (w_rest >= flush_limit) {
            //mutex_.lock();
            nanodelay2(w_rest * w_delay / cache_line);
            w_rest -= flush_limit;
            //mutex_.unlock();
        }
    }

};

void RWSpeed_Test() {
    NVM_Manager * mng_ = new NVM_Manager(1 << 20);
    auto GetMicros = [] () {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    };
    ull time1 = GetMicros();
    nvAddr tmp = mng_->Allocate(4096);
    byte buf[128];
    Randomizer R; R.setRandLimit(0, 4000);
    for (ull i = 0; i < 1000000; ++i) {
        mng_->read(buf, tmp + R.randomInt(), 64);
    }
    ull time2 = GetMicros();
    printf("Read Time = %.2f ms / 1GB\n", (double)(time2 - time1) *16 / 1000 );
    for (ull i = 0; i < 1000000; ++i) {
        mng_->write(tmp + R.randomInt(), buf, 64);
    }
    time1 = GetMicros();
    printf("Write Time = %.2f ms / 1GB\n", (double)(time1 - time2) *16 / 1000 );
}

void Delay(ull total, ull each) {
    auto GetMicros = [] () {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    };
    ull times = total / each;
    ull time1 = GetMicros();

    for (int i = 0; i < times; ++i)
        nanodelay(each);
    ull time2 = GetMicros();
    ull time0 = (time2 - time1) * 1000;
    ull s = total / 1000000000;
    ll more = time0 - total;
    printf("Test : %llu (ns) * %llu (op) = %llu (s)\n", times, each, total / 1000000000);
    printf("Total Time : %llu - %lld = %lld\n", time0, total, more);
    printf("%lld / %llu = %.6f (ns / op)\n", more, times, (double)more / times);

}
void MSL_Test() {
    NVM_Manager * mng_ = new NVM_Manager(4 * MB);
    leveldb::CachePolicy cp(2 * MB, 2 * MB, 2 * MB, 16 * MB, 10, 10);
    leveldb::L2SkipList msl(mng_, cp);

    Randomizer R;
    Randomizer valueGenerator;
    valueGenerator.setStringMode(64);
    R.setStringMode(2);
    for (int i = 0; i < 2000; ++i){

        char* key = new char[R.stringLength+1];
        memcpy(key,R.randomString().c_str(),R.stringLength);
//        memcpy(key,R.randomString().c_str(),R.stringLength);
        key[R.stringLength] = 0;
//        printf("Inserting key : [%s]\n",key);
        msl.Add(leveldb::Slice(key,R.stringLength),
                leveldb::Slice(valueGenerator.randomString().c_str(),
                               valueGenerator.stringLength),
                0, 0);
        delete[] key;
    }
    msl.Print();
}
const int DB_BASE_SIZE = 100000, ADD_BASE_SIZE = 800000,
          DEL_BASE_SIZE = 800000, FIND_BASE_SIZE = 800000;

void Trie_SpeedTest(){
    printf("Trie Speed Test:\n");
    char s[] = "Hello,world!";
    Trie<int> * main = new Trie<int>;
    auto GetMicros = [] () {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    };
    Randomizer keyGenerator(0xdeadbeef);
    Randomizer valueGenerator(0xdeadbeef);
    Randomizer R(0xdeadbeef);
    keyGenerator.setStringMode(16);
    valueGenerator.setStringMode(64);
    Trie<int>::Iterator *iter = main->NewIterator(), *next = main->NewIterator();

    std::vector<string> DB;
    int DB_SIZE = 2 * DB_BASE_SIZE / 10, ADD_SIZE = ADD_BASE_SIZE / 10,
            DEL_SIZE = DEL_BASE_SIZE / 10, FIND_SIZE = FIND_BASE_SIZE / 10;
    for (int i = 0; i < DB_SIZE; ++i)
        DB.push_back(keyGenerator.randomString());

    ull time1 = GetMicros();
    R.setRandLimit(0, DB_SIZE);
    for (int i = 0; i < ADD_SIZE; ++i) {
        main->Insert(DB[R.randomInt()], &i);
        main->Delete(DB[R.randomInt()]);
    }

    ull time2 = GetMicros();
    ull time = time2 - time1;
    ull data = 2 * ADD_SIZE * (16 + 16);
    printf("Insert / Delete : %.2f MB/s (%llu op /s)\n", (double)data / time,
           2ULL * ADD_SIZE * 1000000 / time );


    time1 = GetMicros();
    R.setRandLimit(0, DB_SIZE);
    int x; bool equal;
    for (int i = 0; i < FIND_SIZE; ++i)
        main->FuzzyFind(DB[R.randomInt()]);

    time2 = GetMicros();
    time = time2 - time1;
    data = FIND_SIZE * (16 + 16);
    printf("Query : %.2f MB/s (%llu op /s)\n", (double)data / time,
           1ULL * FIND_SIZE * 1000000 / time );
/*
    next->SeekToFirst(); next->Next();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        string key = iter->Key(), key2 = next->Valid() ? next->Key() : "";
        int value = iter->Data();
        assert(!next->Valid()|| key < next->Key());
        printf("%s : %d\n", key.c_str(), value);
        if (next->Valid()) next->Next();
    }*/
}
void nvSkiplist_SpeedTest(NVM_Manager* mng, const leveldb::CachePolicy& cp){
    printf("nvSkiplist Speed Test:\n");
    char s[] = "Hello,world!";

    leveldb::nvSkiplist* main = new leveldb::nvSkiplist(mng, cp);
    //Trie<int> * main = new Trie<int>;
    auto GetMicros = [] () {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    };
    Randomizer keyGenerator(0xdeadbeef);
    Randomizer valueGenerator(0xdeadbeef);
    Randomizer R(0xdeadbeef);
    keyGenerator.setStringMode(16);
    valueGenerator.setStringMode(64);

    std::vector<string> DB;
    int DB_SIZE = 2 * DB_BASE_SIZE, ADD_SIZE = ADD_BASE_SIZE,
            DEL_SIZE = DEL_BASE_SIZE, FIND_SIZE = FIND_BASE_SIZE;
    for (int i = 0; i < DB_SIZE; ++i)
        DB.push_back(keyGenerator.randomString());

    ull time1 = GetMicros();
    R.setRandLimit(0, DB_SIZE);
    for (int i = 0; i < ADD_SIZE; ++i) {
        main->Insert(DB[R.randomInt()], DB[R.randomInt()], 0, 0);
        main->Insert(DB[R.randomInt()], DB[R.randomInt()], 0, 1);
    }
    ull time2 = GetMicros();
    ull time = time2 - time1;
    ull data = 2 * ADD_SIZE * (16 + 16);
    printf("Insert / Delete : %.2f MB/s (%llu op /s)\n", (double)data / time,
           2ULL * ADD_SIZE * 1000000 / time );


    time1 = GetMicros();
    R.setRandLimit(0, DB_SIZE);
    int x; bool equal;
    string value; leveldb::Status status;
    int total = 0;
    for (int i = 0; i < FIND_SIZE; ++i)
        total += main->Get(DB[R.randomInt()], &value, &status);

    time2 = GetMicros();
    time = time2 - time1;
    data = FIND_SIZE * (16 + 16);
    printf("Query : %.2f MB/s (%llu op /s)\n", (double)data / time,
           1ULL * FIND_SIZE * 1000000 / time );
    delete main;
/*
    next->SeekToFirst(); next->Next();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        string key = iter->Key(), key2 = next->Valid() ? next->Key() : "";
        int value = iter->Data();
        assert(!next->Valid()|| key < next->Key());
        printf("%s : %d\n", key.c_str(), value);
        if (next->Valid()) next->Next();
    }*/
}
void MixedSkiplist_SpeedTest(NVM_Manager* mng, const leveldb::CachePolicy& cp){
    printf("MixedSkiplist Speed Test:\n");
    char s[] = "Hello,world!";

    leveldb::L2SkipList* main = new leveldb::L2SkipList(mng, cp);
    //Trie<int> * main = new Trie<int>;
    auto GetMicros = [] () {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    };
    Randomizer keyGenerator(0xdeadbeef);
    Randomizer valueGenerator(0xdeadbeef);
    Randomizer R(0xdeadbeef);
    keyGenerator.setStringMode(16);
    valueGenerator.setStringMode(64);
    leveldb::Iterator* next = main->NewIterator(), *iter = main->NewIterator();


    std::vector<string> DB;
    int DB_SIZE = 2 * DB_BASE_SIZE, ADD_SIZE = ADD_BASE_SIZE,
            DEL_SIZE = DEL_BASE_SIZE, FIND_SIZE = FIND_BASE_SIZE;
    for (int i = 0; i < DB_SIZE; ++i)
        DB.push_back(keyGenerator.randomString());

    ull time1 = GetMicros();
    R.setRandLimit(0, DB_SIZE);
    for (int i = 0; i < ADD_SIZE; ++i) {
        main->Insert(DB[R.randomInt()], DB[R.randomInt()], 0, 0);
        main->Insert(DB[R.randomInt()], DB[R.randomInt()], 0, 1);
    }

    ull time2 = GetMicros();
    ull time = time2 - time1;
    ull data = 2 * ADD_SIZE * (16 + 16);
    printf("Insert / Delete : %.2f MB/s (%llu op /s)\n", (double)data / time,
           2ULL * ADD_SIZE * 1000000 / time );


    time1 = GetMicros();
    R.setRandLimit(0, DB_SIZE);
    int x; bool equal;
    string value; leveldb::Status status;
    int total = 0;
    for (int i = 0; i < FIND_SIZE; ++i)
        total += main->Get(DB[R.randomInt()], &value, &status);

    time2 = GetMicros();
    time = time2 - time1;
    data = FIND_SIZE * (16 + 16);
    printf("Query : %.2f MB/s (%llu op /s)\n", (double)data / time,
           1ULL * FIND_SIZE * 1000000 / time );
    delete main;
}
void MemTable_SpeedTest(){
    printf("SkipList Speed Test:\n");
    char s[] = "Hello,world!";

    leveldb::Options options_;
    const leveldb::InternalKeyComparator cmp(options_.comparator);
    leveldb::MemTable * main = new leveldb::MemTable(cmp);
    main->Ref();
    //Trie<int> * main = new Trie<int>;
    auto GetMicros = [] () {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    };
    Randomizer keyGenerator(0xdeadbeef);
    Randomizer valueGenerator(0xdeadbeef);
    Randomizer R(0xdeadbeef);
    keyGenerator.setStringMode(16);
    valueGenerator.setStringMode(64);
    leveldb::Iterator* next = main->NewIterator(), *iter = main->NewIterator();


    std::vector<string> DB;
    int DB_SIZE = 2 * DB_BASE_SIZE, ADD_SIZE = ADD_BASE_SIZE,
            DEL_SIZE = DEL_BASE_SIZE, FIND_SIZE = FIND_BASE_SIZE;
    for (int i = 0; i < DB_SIZE; ++i)
        DB.push_back(keyGenerator.randomString());

    ull time1 = GetMicros();
    R.setRandLimit(0, DB_SIZE);
    for (int i = 0; i < ADD_SIZE; ++i) {
        main->Add(2*i, leveldb::ValueType::kTypeValue,
                  DB[R.randomInt()], DB[R.randomInt()]);
        main->Add(2*i+1, leveldb::ValueType::kTypeDeletion,
                  DB[R.randomInt()], DB[R.randomInt()]);
    }

    ull time2 = GetMicros();
    ull time = time2 - time1;
    ull data = 2 * ADD_SIZE * (16 + 16);
    printf("Insert / Delete : %.2f MB/s (%llu op /s)\n", (double)data / time,
           2ULL * ADD_SIZE * 1000000 / time );


    time1 = GetMicros();
    R.setRandLimit(0, DB_SIZE);
    int x; bool equal;
    string value; leveldb::Status status;
    int total = 0;
    for (int i = 0; i < FIND_SIZE; ++i)
        total += main->Get(leveldb::LookupKey(DB[R.randomInt()],0), &value, &status);

    time2 = GetMicros();
    time = time2 - time1;
    data = FIND_SIZE * (16 + 16);
    printf("Query : %.2f MB/s (%llu op /s)\n", (double)data / time,
           1ULL * FIND_SIZE * 1000000 / time );
    //delete main;
    main->Unref();
}
void BandWidth_Test(NVM_Manager* mng) {
    auto GetMicros = [] () {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    };
    nvAddr x = mng->Allocate(1 * MB);
    byte* buf = new byte[4096];
    ull total = 0;
    ull time1 = GetMicros();
    for (int i = 0; i < 1000000; ++i) {
        mng->read(buf, x, 4096);
        total += 4096;
    }
    ull time2 = GetMicros();
    printf("Bandwidth = %llu MB / s\n", total / (time2 - time1));
}

void NVM_FILE_TEST() {
    NVM_Manager* mng = new NVM_Manager(1500 * MB);
    FakeFS* fs = new FakeFS(mng);
    nvFileHandle file = fs->fopen("alice_temp.txt", "w");
    //NVM_File *file = new NVM_File(mng);
    FILE* alice = fopen("alice.txt", "r");
    const size_t BUF_SIZE = 4096;
    char buf[BUF_SIZE];
    size_t size;
    //file->openType_ = NVM_File::WRITE_ONLY;
    while (1) {
        size = fread(buf, 1, BUF_SIZE, alice);
        if (size == 0)
            break;
        fs->fwrite(buf, 1, size, file);
        //file->append(buf, size);
        if (size < BUF_SIZE)
            break;
    }
    fclose(alice);
    fs->fclose(file);
    FILE* blice = fopen("blice.txt", "w");
    nvFileHandle file2 = fs->fopen("alice_temp.txt", "r");
    //NVM_File * file2 = new NVM_File(mng, file->info_block_);
    //file2->openType_ = NVM_File::READ_ONLY;
    while (1) {
        size_t size = fs->fread(buf, 1, BUF_SIZE, file2);
        //size_t size = file2->read(buf, 4096);
        if (size == 0)
            break;
        fwrite(buf, 1, size, blice);
        if (size < BUF_SIZE)
            break;
    }
    fclose(blice);
    fs->fclose(file2);
    //delete file;
    //delete file2;
    delete fs;
    delete mng;
}

leveldb::Random randomizer(10000);
namespace leveldb {
class MyRandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  MyRandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, 0.5, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};
}

uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void RandomTest(leveldb::AbstractMemTable * table, ull oprs, ull datasize, double write_percentage, ull vsize, bool print = true) {
    static ull seq = 0;
    std::string value;
    leveldb::Status s;
    uint32_t write_line = 2147483647L * write_percentage;

    int found = 0, read = 0;
    leveldb::WriteBatch batch;
    leveldb::MyRandomGenerator gen;

    printf("Random Test (%llu/%llu, %.2f/%.2f, Value = %llu)...\n",
           oprs, datasize,
           write_percentage, 1 - write_percentage,
           vsize);
    fflush(stdout);

    ull micros = NowMicros();

    for (ull t = 0; t < oprs; ++t) {
          char key_[100];
          const int k = randomizer.Next() % datasize;
          //const int k = thread->rand.Next() % DB_Base;
          snprintf(key_, sizeof(key_), "%016d", k);
          Slice key(key_, 16);
          if (randomizer.Next() < write_line) {
              leveldb::Slice value = gen.Generate(
                          vsize == 0 ? 8 * (1 + randomizer.Next() % 128) : vsize);
              table->Add(seq++, leveldb::kTypeValue, key, value);
          } else {
              read++;
              if (table->Get(leveldb::LookupKey(key, t), &value, &s)) {
                found++;
              }
          }
    }

    micros = NowMicros() - micros;

    //printf("ok.\n");
    if (print) {
        printf("Speed : %.4f * 10^3 ops/s (%d of %d found)\n", 1000. * oprs / micros, found, read);
    }
    fflush(stdout);
}

void RandomTest1(NVM_Manager* mng, const leveldb::CachePolicy& cp, leveldb::D2MemTable * table,
                 ull oprs, ull datasize, double write_percentage, ull vsize) {
    std::string dbname = "/home/sagitrs/test_leveldb";
    ull seq(0);
    table = new leveldb::D2MemTable(mng, cp, dbname, seq);
    //leveldb::L2SkipList *list = new leveldb::L2SkipList(mng, cp);
    //table = new leveldb::L2MemTable(list, "", mng, "/nvm/test/table1.nvskiplist", 0);
    table->Ref();
    RandomTest(table, oprs, datasize, 1., vsize, false);
    RandomTest(table, oprs, datasize, 0.05, vsize, true);
    RandomTest(table, oprs, datasize, 0.95, vsize, true);
    RandomTest(table, oprs, datasize, write_percentage, vsize, true);

    table->Unref();
    table = nullptr;
}

void TestMixedSkipList() {
    ull oprs = 10000;
    ull datasize = 1000;
    ull value_size = 100;
    double wp = 0.5;

    NVM_Manager* mng = new NVM_Manager(1000 * MB);
    leveldb::Options options;
    leveldb::CachePolicy cp_1   (256 * MB, 256 * MB, 1000 * MB, 1000 * MB, options.TEST_cover_range, 10);
    leveldb::CachePolicy cp_4   (256 * MB, 256 * MB, 250  * MB, 1000 * MB, options.TEST_cover_range, 10);
    leveldb::CachePolicy cp_10  (256 * MB, 256 * MB, 100  * MB, 1000 * MB, options.TEST_cover_range, 10);
    leveldb::CachePolicy cp_100 (256 * MB, 256 * MB, 10   * MB, 1000 * MB, options.TEST_cover_range, 10);
    leveldb::CachePolicy cp_1000(256 * MB, 256 * MB, 1    * MB, 1000 * MB, options.TEST_cover_range, 10);
    leveldb::CachePolicy cp_0   (256 * MB, 256 * MB, 1    * KB, 1000 * MB, options.TEST_cover_range, 10);

    leveldb::L2MemTable_BufferLog *table0 = nullptr;
    leveldb::L2MemTable_D1SkipList *table2 = nullptr;
    leveldb::D2MemTable *table1 = nullptr;
/*
    printf("Test of Buffer(MemTable) & Log:\n");
    RandomTest0(mng, table0, oprs, datasize, wp, 100);
    RandomTest0(mng, table0, oprs, datasize, wp, 1000);
    RandomTest0(mng, table0, oprs, datasize, wp, 10000);
    RandomTest0(mng, table0, oprs, datasize, wp, 100000);
    printf("Finished.\n\n");

    printf("Test of Buffer(D1SkipList) & Log:\n");
    RandomTest2(mng, table2, oprs, datasize, wp, 100);
    RandomTest2(mng, table2, oprs, datasize, wp, 1000);
    RandomTest2(mng, table2, oprs, datasize, wp, 10000);
    RandomTest2(mng, table2, oprs, datasize, wp, 100000);
    printf("Finished.\n\n");
*/
    printf("Test of Mixed-Skiplist (NVM/DRAM = 1x):\n");
    RandomTest1(mng, cp_1, table1, oprs, datasize, wp, 100);
    RandomTest1(mng, cp_1, table1, oprs, datasize, wp, 1000);
    RandomTest1(mng, cp_1, table1, oprs, datasize, wp, 10000);
    RandomTest1(mng, cp_1, table1, oprs, datasize, wp, 100000);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 4x):\n");
    RandomTest1(mng, cp_4, table1, oprs, datasize, wp, 100);
    RandomTest1(mng, cp_4, table1, oprs, datasize, wp, 1000);
    RandomTest1(mng, cp_4, table1, oprs, datasize, wp, 10000);
    RandomTest1(mng, cp_4, table1, oprs, datasize, wp, 100000);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 10x):\n");
    RandomTest1(mng, cp_10, table1, oprs, datasize, wp, 100);
    RandomTest1(mng, cp_10, table1, oprs, datasize, wp, 1000);
    RandomTest1(mng, cp_10, table1, oprs, datasize, wp, 10000);
    RandomTest1(mng, cp_10, table1, oprs, datasize, wp, 100000);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 100x):\n");
    RandomTest1(mng, cp_100, table1, oprs, datasize, wp, 100);
    RandomTest1(mng, cp_100, table1, oprs, datasize, wp, 1000);
    RandomTest1(mng, cp_100, table1, oprs, datasize, wp, 10000);
    RandomTest1(mng, cp_100, table1, oprs, datasize, wp, 100000);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 1000x):\n");
    RandomTest1(mng, cp_1000, table1, oprs, datasize, wp, 100);
    RandomTest1(mng, cp_1000, table1, oprs, datasize, wp, 1000);
    RandomTest1(mng, cp_1000, table1, oprs, datasize, wp, 10000);
    RandomTest1(mng, cp_1000, table1, oprs, datasize, wp, 100000);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 0x):\n");
    RandomTest1(mng, cp_0, table1, oprs, datasize, wp, 100);
    RandomTest1(mng, cp_0, table1, oprs, datasize, wp, 1000);
    RandomTest1(mng, cp_0, table1, oprs, datasize, wp, 10000);
    RandomTest1(mng, cp_0, table1, oprs, datasize, wp, 100000);
    printf("Finished.\n\n");

    oprs = 1000000;
    value_size = 100;

    printf("Test of Mixed-Skiplist (NVM/DRAM = 1x):\n");
    RandomTest1(mng, cp_1, table1, oprs, 100, wp, value_size);
    RandomTest1(mng, cp_1, table1, oprs, 1000, wp, value_size);
    RandomTest1(mng, cp_1, table1, oprs, 10000, wp, value_size);
    RandomTest1(mng, cp_1, table1, oprs, 100000, wp, value_size);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 4x):\n");
    RandomTest1(mng, cp_4, table1, oprs, 100, wp, value_size);
    RandomTest1(mng, cp_4, table1, oprs, 1000, wp, value_size);
    RandomTest1(mng, cp_4, table1, oprs, 10000, wp, value_size);
    RandomTest1(mng, cp_4, table1, oprs, 100000, wp, value_size);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 10x):\n");
    RandomTest1(mng, cp_10, table1, oprs, 100, wp, value_size);
    RandomTest1(mng, cp_10, table1, oprs, 1000, wp, value_size);
    RandomTest1(mng, cp_10, table1, oprs, 10000, wp, value_size);
    RandomTest1(mng, cp_10, table1, oprs, 100000, wp, value_size);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 100x):\n");
    RandomTest1(mng, cp_100, table1, oprs, 100, wp, value_size);
    RandomTest1(mng, cp_100, table1, oprs, 1000, wp, value_size);
    RandomTest1(mng, cp_100, table1, oprs, 10000, wp, value_size);
    RandomTest1(mng, cp_100, table1, oprs, 100000, wp, value_size);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 1000x):\n");
    RandomTest1(mng, cp_1000, table1, oprs, 100, wp, value_size);
    RandomTest1(mng, cp_1000, table1, oprs, 1000, wp, value_size);
    RandomTest1(mng, cp_1000, table1, oprs, 10000, wp, value_size);
    RandomTest1(mng, cp_1000, table1, oprs, 100000, wp, value_size);
    printf("Finished.\n\n");

    printf("Test of Mixed-Skiplist (NVM/DRAM = 0x):\n");
    RandomTest1(mng, cp_0, table1, oprs, 100, wp, value_size);
    RandomTest1(mng, cp_0, table1, oprs, 1000, wp, value_size);
    RandomTest1(mng, cp_0, table1, oprs, 10000, wp, value_size);
    RandomTest1(mng, cp_0, table1, oprs, 100000, wp, value_size);
    printf("Finished.\n\n");
}

void D2MemTableTest() {

    NVM_Manager* mng = new NVM_Manager(1000 * MB);
    leveldb::Options options;
    leveldb::CachePolicy cp_10(256 * MB, 256 * MB, 100 * MB, 1000 * MB, options.TEST_cover_range, 10);
    //D1SkipList table(64 * MB);
    leveldb::D2MemTable table0(mng, cp_10, "/nvm/memtable/", 0);


    double write_percentage = 0.2;
    ull oprs = 100000;
    ull datasize = 10000;
    ull value_size = 100;

    RandomTest(&table0, oprs, datasize, 1., value_size, false);
    RandomTest(&table0, oprs, datasize, 0.05, value_size, true);
    RandomTest(&table0, oprs, datasize, 0.95, value_size, true);
    RandomTest(&table0, oprs, datasize, write_percentage, value_size, true);

    leveldb::L2SkipList *list = new leveldb::L2SkipList(mng, cp_10);
    //leveldb::L2MemTable *table2 = new leveldb::L2MemTable(list, "", mng, "/nvm/test/table1.nvskiplist", 0);
    //table2->Ref();
    //RandomTest(table2, oprs, datasize, 1., value_size, false);
    //RandomTest(table2, oprs, datasize, 0.05, value_size, true);
    //RandomTest(table2, oprs, datasize, 0.95, value_size, true);
    //RandomTest(table2, oprs, datasize, write_percentage, value_size, true);

    //table2->Unref();
    //table2 = nullptr;

}
ull FillTest(leveldb::nvMemTable* table, ull datasize, ull vsize, ull memsize) {
    static ull seq = 0;
    std::string value;
    leveldb::Status s;
    uint32_t write_line = 2147483647L * 0.95;

    ull found = 0, read = 0, write = 0;
    leveldb::WriteBatch batch;
    leveldb::MyRandomGenerator gen;

    printf("Fill Test (%llu, Value = %llu)...\n",
           datasize,
           vsize);
    fflush(stdout);

    ull micros = NowMicros();
    for (ull t = 0; t < 10000000; ++t) {
         char key[100];
         const int k = randomizer.Next() % datasize;
         //const int k = thread->rand.Next() % DB_Base;
         snprintf(key, sizeof(key), "%016d", k);
         if (randomizer.Next() < write_line) {
             write ++;
             leveldb::Slice value = gen.Generate(
                 vsize == 0 ? 8 * (1 + randomizer.Next() % 32) : vsize);
             if (table->StorageUsage() + 16 + value.size() + 100 > memsize) {
                 printf("Write = %llu, Garbage = %.2f\n", write, table->Garbage());
                 printf("%llu of %llu found\n", found, read);
                 fflush(stdout);
                 if (table->Garbage() > 0.5)
                     table->GarbageCollection();
                 else
                     break;
             }
             table->Add(seq++, leveldb::kTypeValue, key, value);
         } else {
             read++;
             if (table->Get(leveldb::LookupKey(key, t), &value, &s)) {
               found++;
             }
         }
    }

    micros = NowMicros() - micros;

    //printf("ok.\n");
    printf("Write : %llu\n", write);
    printf("Speed : %.4f * 10^3 ops/s\n", 1000. * write / micros);
    fflush(stdout);
    return write;
}
ull FillTest2(leveldb::D3MemTable* table, ull datasize, ull vsize, ull memsize, double write_p) {
    static ull seq = 0;
    std::string value;
    leveldb::Status s;
    uint32_t write_line = 2147483647L * write_p;

    ull found = 0, read = 0, write = 0, total = 0;
    leveldb::WriteBatch batch;
    leveldb::MyRandomGenerator gen;

    printf("Fill Test (%llu, Value = %llu)...\n",
           datasize,
           vsize);
    fflush(stdout);


    static const ul max_buffer = 100000;
    ul rands[max_buffer], rand_now = 0;
    for (ull t = 0; t < max_buffer; ++t)
        rands[t] = randomizer.Next();

    ull micros = NowMicros();
    for (ull t = 0; t < 10000000; ++t) {
        total++;
        if (t % 100000 == 0) { printf("%llu op finished...\n", t); fflush(stdout); }
         char key[100];
         const int k = rands[++rand_now % max_buffer] % datasize;
         //const int k = thread->rand.Next() % DB_Base;
         snprintf(key, sizeof(key), "%016d", k);
         if (rands[++rand_now % max_buffer] < write_line) {
             write ++;
             leveldb::Slice value = gen.Generate(vsize);
             if (table->StorageUsage() + 16 + value.size() + 100 > memsize) {
                 printf("Write = %llu, Garbage = %.2f\n", write, table->Garbage());
                 printf("%llu of %llu found\n", found, read);
                 fflush(stdout);
                 if (table->Garbage() > 0.5)
                     table->GarbageCollection();
                 else
                     break;
             }
             table->Add(seq++, leveldb::kTypeValue, key, value);
         } else {
             read++;
             if (table->Get(leveldb::LookupKey(key, t), &value, &s)) {
               found++;
             }
         }
    }

    micros = NowMicros() - micros;

    //printf("ok.\n");
    printf("Write : %.2f%%\n", write_p * 100);
    printf("Speed : %.4f * 10^3 ops/s\n", 1000. * total / micros);
    fflush(stdout);
    return write;
}
using namespace leveldb;

void GarbageCollection_Test() {
    NVM_Manager* mng = new NVM_Manager(1000 * MB);
    leveldb::Options options;
    leveldb::CachePolicy cp_10(64 * MB, 64 * MB, 100 * MB, 1000 * MB, options.TEST_cover_range, 10);
    cp_10.garbage_cache_size_ = 0;
    //D1SkipList table(64 * MB);
    D2MemTable table0(mng, cp_10, "/nvm/memtable/", 0);

    double write_percentage = 0.2;
    ull oprs = 1000000;
    ull datasize = 100000;
    ull value_size = 100;

    ull write = 0;
    double g = 1;
    write += FillTest(&table0, datasize, value_size, 64 * MB);

    printf("Write Total : %llu\n", write);
}


void HashedSkiplistTest(NVM_Manager* mng, ul data_size, ul value_size, ul hash_div) {
    CachePolicy cp(256 * MB, 12 * MB, 100 * MB, 1000 * MB, 10, hash_div);
    leveldb::nvMemTable *mem = new leveldb::D4MemTable(mng, cp, "/nvm/test/", 1);
    mem->Ref();
    printf("DataSize = %u, ValueSize = %u, HashDiv = %u\n", data_size, value_size, hash_div);
    FillTest(mem, data_size, value_size, 256 * MB);
    mem->Unref();
}
int main() {
    NVM_Manager* mng = new NVM_Manager(1000 * MB);
    std::vector<ul> div, vs;
    div.push_back(4); div.push_back(16); div.push_back(64); div.push_back(256); div.push_back(1024);
    vs.push_back(16); vs.push_back(64); vs.push_back(256);
    for (size_t i = 0; i < vs.size(); ++i) {
        printf("----------------\n");
        for (size_t j = 0; j < div.size(); ++j)
            HashedSkiplistTest(mng, 256 * MB / (16 + 32 + vs[i]), vs[i], div[j]);
    }
    return 0;
}
