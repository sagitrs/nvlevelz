// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include "nvm_library/randomizer.h"
#include "nvm_library/sysnvm.h"
#include "nvm_library/benchmark.h"

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
static const char* FLAGS_benchmarks =
        "fillrandomfresh,"
        //"readrandom,readrandomnowarmup,readseq,"
        //"YCSB-workloada,YCSB-workloadb,YCSB-workloadc,YCSB-workloadd,YCSB-workloadf,"
        //"fillseqfresh,fillrandomfresh,fillrandom,"
        //"YCSB-workloade,"
        //"readrandom,readseq,"
        //"fillseqfresh,"
            // DB_Bench_Throughput_4G

        //"YCSB-workloadc,"
        //"YCSB-workloadd,"
        //"YCSB-workloadf,"
        //    "fillrandom,"       // 2. Update Random
        //"readrandom,"                    // 3. Read Random
        //    "readrandom,"                    // 3. Read Random

        //"ReadMoreRandom,"   // 5. Test : Random Read&Write = 9 : 1
        //"BanlanceRandom,"   // 6. Test : Random Read&Write = 1 : 1
        //"WriteMoreRandom,"  // 7. Test : Random Read&Write = 1 : 9
        //"WriteMoreZipfian," // PRE
        //"ReadMoreZipfian,"  // 8. Test : Zipfian Read&Write = 9 : 1
        //"BanlanceZipfian,"  // 9. Test : Zipfian Read&Write = 1 : 1
        //"WriteMoreZipfian," //10. Test : Zipfian Read&Write = 1 : 9
        ;


// Number of key/values to place in database
//static int FLAGS_num = 1000000;
static ull FLAGS_record = 16 * MB;
static ull FLAGS_operation = 8 * MB;
static bool FLAGS_random_warmup = true;

static double FLAGS_pre = 0.0;
static int FLAGS_num =  FLAGS_record;            //      2.00*10^6 * 128Byte = 2.56 * 10^ 8 Byte = 0.256G,

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 10;

// Size of each value
static int FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes written to each file.
// (initialized to default value by "main")
static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
static int FLAGS_block_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// Use the db with the following name.
#ifdef NVDIMM_ENABLED
static const char* FLAGS_db = "/home/user/test_leveldb";
#else
static const char* FLAGS_db = "/home/sagitrs/test_leveldb";
#endif
namespace leveldb {

namespace {
leveldb::Env* g_env = NULL;

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
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

#if defined(__linux)
static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}
#endif

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  ull done_;
  ull next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 0;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = g_env->NowMicros();
    finish_ = start_;
    message_.clear();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Pause() {
      finish_ = g_env->NowMicros();
      seconds_ += (finish_ - start_) * 1e-6;
  }
  void Researt() {
      start_ = g_env->NowMicros();
  }
  double Now() {
      return g_env->NowMicros();
  }
  double PassedMicros() { return Now() - start_; }

  void Stop() {
    finish_ = g_env->NowMicros();
    seconds_ += (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = g_env->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      //if      (next_report_ < 1000)   next_report_ += 100;
      //else if (next_report_ < 10000)  next_report_ += 1000;
      //else if (next_report_ < 100000) next_report_ += 10000;
      //else
          if (next_report_ < 1000000)next_report_ += 100000;
      else if (next_report_ <10000000)next_report_ += 1000000;
      else if (next_report_<100000000)next_report_ += 10000000;
      else                            next_report_ += 100000000;
      fprintf(stderr, "... finished %llu ops%30s\r", done_, "");
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      //double elapsed = (finish_ - start_) * 1e-6;
      double elapsed = this->seconds_;
        char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
            name.ToString().c_str(),
            seconds_ * 1e6 / done_,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState() : cv(&mu) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  ThreadState(int index)
      : tid(index),
        rand(1000 + index) {
  }
};

}  // namespace

class Benchmark {
 private:
  Cache* cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_;
  int value_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int heap_counter_;

  void PrintHeader() {
    const int kKeySize = 16;
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %d\n", num_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_)
             / 1048576.0));
    PrintWarnings();
    fprintf(stdout, "------------------------------------------------\n");
    fflush(stdout);
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

  void PrintEnvironment() {
    fprintf(stderr, "LevelDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
  : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : NULL),
    filter_policy_(FLAGS_bloom_bits >= 0
                   ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                   : NULL),
    db_(NULL),
    num_(FLAGS_num),
    value_size_(FLAGS_value_size),
    entries_per_batch_(1),
    reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
    heap_counter_(0) {
    std::vector<std::string> files;
    g_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        g_env->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }

  void Run() {
    PrintHeader();
    Open();

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      // Reset parameters that may be overridden below
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      value_size_ = FLAGS_value_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();

      void (Benchmark::*method)(ThreadState*) = NULL;
      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      if (name == Slice("open")) {
        method = &Benchmark::OpenBench;
        num_ /= 10000;
        if (num_ < 1) num_ = 1;
      } else if (name == Slice("fillseqfresh")) {
          fresh_db = true;
          method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillseq")) {
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        //fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        //fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillrandomfresh")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("overwrite")) {
        method = &Benchmark::OverWrite;
      } else if (name == Slice("fillsync")) {
        //fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        //fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == Slice("readrandom") || name == Slice("readrandomnowarmup")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("ReadMoreRandom")) {
        method = &Benchmark::ReadMoreRandom;
      }  else if (name == Slice("WriteMoreRandom")) {
          fresh_db = false;
        method = &Benchmark::WriteMoreRandom;
      }  else if (name == Slice("BanlanceRandom")) {
          fresh_db = false;
        method = &Benchmark::BanlanceRandom;
      } else if (name == Slice("ReadMoreLatest")) {
          fresh_db = false;
        method = &Benchmark::ReadMoreLatest;
      } else if (name == Slice("WriteMoreZipfian")) {
          fresh_db = false;
        method = &Benchmark::WriteMoreZipfian;
      } else if (name == Slice("ReadMoreZipfian")) {
          fresh_db = false;
        method = &Benchmark::ReadMoreZipfian;
      } else if (name == Slice("BanlanceZipfian")) {
          fresh_db = false;
        method = &Benchmark::BanlanceZipfian;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("acquireload")) {
        method = &Benchmark::AcquireLoad;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("leveldb.stats");
      } else if (name == Slice("sstables")) {
        PrintStats("leveldb.sstables");
      } else if (name == Slice("YCSB-workloada")) {
        fresh_db = true;
        method = &Benchmark::YCSB_workloada;
      } else if (name == Slice("YCSB-workloadb")) {
        fresh_db = true;
        method = &Benchmark::YCSB_workloadb;
      } else if (name == Slice("YCSB-workloadc")) {
        fresh_db = true;
        method = &Benchmark::YCSB_workloadc;
      } else if (name == Slice("YCSB-workloadd")) {
        fresh_db = true;
        method = &Benchmark::YCSB_workloadd;
      } else if (name == Slice("YCSB-workloade")) {
        fresh_db = true;
        method = &Benchmark::YCSB_workloade;
      } else if (name == Slice("YCSB-workloadf")) {
        fresh_db = true;
        method = &Benchmark::YCSB_workloadf;
      } else if (name == Slice("SAGI-workloadc")) {
        fresh_db = true;
        method = &Benchmark::SAGI_workloadc;
      } else if (name == Slice("SAGI-workloadd")) {
        fresh_db = true;
        method = &Benchmark::SAGI_workloadd;
      } else {
        if (name != Slice()) {  // No error message for empty name
            fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.ToString().c_str());
          method = NULL;
        } else {
          delete db_;
          db_ = NULL;
          DestroyDB(FLAGS_db, Options());
          //char* test = new char[256];delete [] test;
          Open();
          if (name.starts_with("YCSB-")
           || name.starts_with("SAGI-")) {
              Warmup(name);
          }
        }
      }
      if (name == Slice("readrandom"))
          Warmup("readrandom");

      if (method != NULL) {
        RunBenchmark(num_threads, name, method);
        {
            std::string* ss = reinterpret_cast<std::string*>(db_->InfoCollection(-1));
            fprintf(stderr, "Global Info : \n%s\n", ss->c_str());
            fflush(stderr);
            delete ss;
        }
      }
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      g_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    //{
    //    std::string* s = reinterpret_cast<std::string*>(db_->InfoCollection(-1));
    //    printf("Global Info : \n%s\n", s->c_str());
    //    fflush(stdout);
    //    delete s;
    //}

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedSingleOp();
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    port::AtomicPointer ap(&dummy);
    int count = 0;
    void *ptr = NULL;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.Acquire_Load();
      }
      count++;
      thread->stats.FinishedSingleOp();
    }
    if (ptr == NULL) exit(1); // Disable unused variable warning.
  }

  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok =  port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                    uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  void Open() {
    assert(db_ == NULL);
    Options options;
    options.env = g_env;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.block_cache = cache_;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_file_size = FLAGS_max_file_size;
    options.block_size = FLAGS_block_size;
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
    options.reuse_logs = FLAGS_reuse_logs;
    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void OpenBench(ThreadState* thread) {
    for (int i = 0; i < num_; i++) {
      delete db_;
      Open();
      thread->stats.FinishedSingleOp();
    }
  }

  void WriteSeq(ThreadState* thread) {
      DoComplexV2(thread, SagBenchmark::Distribution::Uniform, false, 0., 1., 0., 0., 0.);
      //DoComplex(thread, OperationGenerator::Uniform, 0, false, 0., 1., FLAGS_pre);
    //DoWrite(thread, true);
  }

  void WriteRandom(ThreadState* thread) {
    //DoWrite(thread, false);
      DoComplexV2(thread, SagBenchmark::Distribution::Uniform, true, 0., 1., 0., 0., 0.);
  }

  void OverWrite(ThreadState* thread) {
      DoComplex(thread, OperationGenerator::Uniform, 0, false, 0, 1., FLAGS_pre);
      DoComplex(thread, OperationGenerator::Uniform, 0, true, 0, 1., FLAGS_pre);
  }

  void ReadMoreRandom(ThreadState* thread) {
      DoComplex(thread, OperationGenerator::Uniform, 0, true, 0.9, 0.1, FLAGS_pre);
  }
  void WriteMoreRandom(ThreadState* thread) {
      DoComplex(thread, OperationGenerator::Uniform, 0, true, 0.1, 0.9, FLAGS_pre);
  }
  void BanlanceRandom(ThreadState* thread) {
      DoComplex(thread, OperationGenerator::Uniform, 0, true, 0.5, 0.5, FLAGS_pre);
  }
  void WriteMoreZipfian(ThreadState* thread) {
      DoComplex(thread, OperationGenerator::Zipfian, 1.07, true, 0.1, 0.9 , FLAGS_pre);
  }
  void ReadMoreZipfian(ThreadState* thread) {
      DoComplex(thread, OperationGenerator::Zipfian, 1.07, true, 0.9, 0.1 , FLAGS_pre);
  }
  void BanlanceZipfian(ThreadState* thread) {
      DoComplex(thread, OperationGenerator::Zipfian, 1.07, true, 0.5, 0.5 , FLAGS_pre);
  }
  void ReadMoreLatest(ThreadState* thread) {
      DoComplex(thread, OperationGenerator::Latest, 0, true, 0.8, 0.2, FLAGS_pre);
  }

  void DoWrite(ThreadState* thread, bool seq) {
      if (num_ != FLAGS_num) {
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d ops)", num_);
        thread->stats.AddMessage(msg);
      }

      RandomGenerator gen;
      WriteBatch batch;
      Status s;
      int64_t bytes = 0;
          for (int i = 0; i < num_; i += entries_per_batch_) {
            batch.Clear();
            for (int j = 0; j < entries_per_batch_; j++) {
              const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
              //const int k = seq ? i+j : (thread->rand.Next() % DB_Base);
              char key[100];
              snprintf(key, sizeof(key), "%016d", k);
              batch.Put(key, gen.Generate(value_size_));
              bytes += value_size_ + strlen(key);
              thread->stats.FinishedSingleOp();
            }
            s = db_->Write(write_options_, &batch);
            if (!s.ok()) {
              fprintf(stderr, "put error: %s\n", s.ToString().c_str());
              exit(1);
            }
          }

      thread->stats.AddBytes(bytes);
    }
  void Warmup(const Slice& name) {
      bool random_warmup = FLAGS_random_warmup;
      char key[16 + 1];
      std::string get_value_;
      WriteBatch batch;
      RandomGenerator gen;
      Status s;
      ull warmup_counter = 0, warmup_reporter = 0;
      ShuffleRandomGenerator * shuffle_rand = nullptr;
      if (random_warmup)
          shuffle_rand = new ShuffleRandomGenerator(FLAGS_record);

      fprintf(stderr, "Warm Up... %llu in total.\n", FLAGS_record);
      fflush(stderr);
      for (ull i = 0; i < FLAGS_record; ++i) {
          //const SagBenchmark::Operation& op = bm[i];
          ull key_ull = (random_warmup ? shuffle_rand->Next() : i);
          sprintf(key, "%016llu", key_ull);
          {
              batch.Clear();
              batch.Put(key, gen.Generate(FLAGS_value_size));
              s = db_->Write(write_options_, &batch);
              if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
              }
          }
          {
              warmup_counter++;
              if (warmup_counter >= warmup_reporter) {
                if      (warmup_reporter < 1000000)warmup_reporter += 100000;
                else if (warmup_reporter <10000000)warmup_reporter += 1000000;
                else if (warmup_reporter<100000000)warmup_reporter += 10000000;
                else                               warmup_reporter += 100000000;
                fprintf(stderr, "... finished %llu ops%30s\r", warmup_counter, "");
                fflush(stderr);
              }
          }
      }
      if (random_warmup)
          delete shuffle_rand;
      //db_->InfoCollection(0);{
      std::string* ss = reinterpret_cast<std::string*>(db_->InfoCollection(-1));
      fprintf(stderr, "Global Info : \n%s\n", ss->c_str());
      //fflush(stdout);
      delete ss;
      fprintf(stderr, "Warm-Up finished, Sleep for 5 seconds.\n");
      fflush(stderr);
      sleep(5);
  }
  void YCSB_workloada(ThreadState* thread) {
      DoComplexV2(thread,
                  SagBenchmark::Distribution::Zipfian,
                  SagBenchmark::InsertOrder::Hashed,
                  0.50, 0.00, 0.50, 0.00, 0.00);
  }
  void YCSB_workloadb(ThreadState* thread) {
      DoComplexV2(thread,
                  SagBenchmark::Distribution::Zipfian,
                  SagBenchmark::InsertOrder::Hashed,
                  0.95, 0.00, 0.05, 0.00, 0.00);
  }

  void YCSB_workloadc(ThreadState* thread) {
      DoComplexV2(thread,
                  SagBenchmark::Distribution::Zipfian,
                  SagBenchmark::InsertOrder::Hashed,
                  1., 0.00, 0.00, 0.00, 0.00);
  }

  void YCSB_workloadd(ThreadState* thread) {
      DoComplexV2(thread,
                  SagBenchmark::Distribution::Zipfian,
                  SagBenchmark::InsertOrder::Hashed,
                  0.95, 0.05, 0.00, 0.00, 0.00);
  }

  void YCSB_workloade(ThreadState* thread) {
      DoComplexV2(thread,
                  SagBenchmark::Distribution::Zipfian,
                  SagBenchmark::InsertOrder::Hashed,
                  0.00, 0.05, 0.00, 0.95, 0.00);
  }

  void YCSB_workloadf(ThreadState* thread) {
      DoComplexV2(thread,
                  SagBenchmark::Distribution::Zipfian,
                  SagBenchmark::InsertOrder::Hashed,
                  0.50, 0.00, 0.00, 0.00, 0.50);
  }
  void SAGI_workloadc(ThreadState* thread) {
      DoComplexV2(thread,
                  SagBenchmark::Distribution::Zipfian,
                  SagBenchmark::InsertOrder::Hashed,
                  0.20, 0.00, 0.80, 0.00, 0.00);
  }
  void SAGI_workloadd(ThreadState* thread) {
      DoComplexV2(thread,
                  SagBenchmark::Distribution::Zipfian,
                  SagBenchmark::InsertOrder::Hashed,
                  0.05, 0.00, 0.95, 0.00, 0.00);
  }
  void DoComplexV2(ThreadState* thread, SagBenchmark::Distribution dist, bool hashed, double read, double insert, double update, double scan, double readmodifywrite) {
      thread->stats.Pause();
      if (num_ != FLAGS_num) {
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d ops)", num_);
        thread->stats.AddMessage(msg);
      }

      ul sec = 0;
      ul new_sec = trunc(thread->stats.PassedMicros());
      ul sec_op = 0;

      SagBenchmark bm;
      ull record_count = FLAGS_record, operation_count = FLAGS_operation;
      bm.SetKeyDistribution(dist,
          (hashed ? SagBenchmark::InsertOrder::Hashed : SagBenchmark::InsertOrder::Ordered ));
      //bm.SetRandomFullRange(false);
      bm.SetKeyValueSize(16, FLAGS_value_size, SagBenchmark::Distribution::Static);
      bm.SetOperationCount(operation_count, record_count);
      bm.SetTypeDistribution(read, insert, update, scan, readmodifywrite);
      bm.SetPreGenerate(false);
      bm.Flush();
      bm.Generate();
      //bm.SeekToWarmupFirst();

      //ull total_count = record_count + operation_count;

      char key[16 + 1];
      std::string get_value_;
      WriteBatch batch;
      Status s;
      int64_t bytes = 0;

      ull found = 0, read_count = 0, write_count = 0;
      ReadOptions options;

      thread->stats.Researt();
      for (ull i = 0; i < operation_count; ++i) {
          const SagBenchmark::Operation& op = bm[i];
          sprintf(key, "%016llu", op.key_);
          switch (op.type_) {
          case SagBenchmark::OperationType::Read: {
              read_count ++;
              s = db_->Get(options, key, &get_value_);
              found += s.ok();
          } break;
          case SagBenchmark::OperationType::Insert:
          case SagBenchmark::OperationType::Update:{
              batch.Clear();
              batch.Put(key, op.value_);
              bytes += value_size_ + 16;
              write_count ++;
              s = db_->Write(write_options_, &batch);
              if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
              }
          } break;
          case SagBenchmark::OperationType::Scan: {
              Iterator* iter = db_->NewIterator(options);
              int i = 0;
              int64_t bytes = 0;
              for (iter->SeekToFirst(); i < 100 && iter->Valid(); iter->Next()) {
                bytes += iter->key().size() + iter->value().size();
                ++i;
              }
              //thread->stats.FinishedSingleOp();
              delete iter;
              //thread->stats.AddBytes(bytes);
          } break;
          case SagBenchmark::OperationType::ReadModifyWrite: {
              read_count ++;
              s = db_->Get(options, key, &get_value_);
              found += s.ok();

              batch.Clear();
              batch.Put(key, op.value_);
              bytes += value_size_ + 16;
              write_count ++;
              s = db_->Write(write_options_, &batch);
              if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
              }
          } break;
          default:
              assert(false);
          }
          thread->stats.FinishedSingleOp();

          new_sec = trunc(thread->stats.PassedMicros() / 1000000);
          if (new_sec > sec) {
              for (ul i = sec+1; i < new_sec; ++i)
                printf("0\t\n");
              printf("%u\t\n", sec_op);
              fflush(stdout);
              sec_op = 0;
              sec = new_sec;
          } else
              sec_op ++;

      }
      thread->stats.AddBytes(bytes);
      char msg[100];
      snprintf(msg, sizeof(msg), "(%llu of %llu found)", found, read_count);
  //    snprintf(msg, sizeof(msg), "(%d of %llu found)", found, FLAGS_cycle);
      thread->stats.AddMessage(msg);
      {
          //std::string *s = reinterpret_cast<std::string*>(db_->InfoCollection(1));
          //printf("Info Collection:\n%s\n", s->c_str());
          //delete s;
      }
  }
  void DoComplex(ThreadState* thread, int distribution, double c, bool hash, double read, double write, double pre) {
      //this->db_->InfoCollection(false);
      thread->stats.Pause();
      if (num_ != FLAGS_num) {
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d ops)", num_);
        thread->stats.AddMessage(msg);
      }

      ull warmup = static_cast<ull>(num_ * pre);
      ull total = static_cast<ull>(num_);
      ull read_total = 0;
      RandomGenerator gen;
      OperationGenerator opgen;
      opgen.SetDistribution(distribution, c);
      opgen.SetOperationCount(total);
      opgen.SetOrder(hash);
      opgen.SetRWProportion(read, write, 1.);

      WriteBatch batch;
      Status s;
      int64_t bytes = 0;

      ReadOptions options;
      std::string value;
      int found = 0;

      double max_delay = 0, prev = 0;
      for (ull i = 0; i < warmup; ++i) {
          OperationGenerator::Operation operation;
          OperationGenerator::Operation *op = &operation;
          opgen.Generate(op);
          if (op->type_ == OperationGenerator::Read) {
              s = db_->Get(options, op->key_, &value);
          } else {
              batch.Clear();
              batch.Put(op->key_, gen.Generate(value_size_));
              bytes += value_size_ + op->key_.size();
              s = db_->Write(write_options_, &batch);
              if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
              }
          }
      }
      std::vector<OperationGenerator::Operation> op_list;
      static const ull MaxListSize = 100000;
      ull rest = num_;
      ull block = 0;
      //OperationGenerator::Operation *op_list = new OperationGenerator::Operation[MaxListSize];
      thread->stats.Researt();
      for (ull t = 0; t < total; t += block) {
          block = min(rest, MaxListSize);
          rest -= block;
          op_list.clear();
          for (ull i = 0; i < block; ++i) {
              OperationGenerator::Operation op;
              op_list.push_back(op);
          }
          for (auto i = op_list.begin(); i != op_list.end(); ++i) {
              opgen.Generate(&(*i));
          }
          //thread->stats.Researt();
          //prev = thread->stats.Now();
          for (auto i = op_list.begin(); i != op_list.end(); ++i) {
              OperationGenerator::Operation *op = &(*i);
              if (op->type_ == OperationGenerator::Read) {
                  read_total ++;
                  s = db_->Get(options, op->key_, &value);
                  if (s.ok())
                      found++;
                  thread->stats.FinishedSingleOp();
              } else {
                  batch.Clear();
                  batch.Put(op->key_, gen.Generate(value_size_));
                  bytes += value_size_ + op->key_.size();
                  thread->stats.FinishedSingleOp();
                  s = db_->Write(write_options_, &batch);
                  if (!s.ok()) {
                    fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                    exit(1);
                  }
              }
          }
          //thread->stats.Pause();
      }
      thread->stats.AddBytes(bytes);
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d of %llu found) (< %.2f ms/op)",
               found,
               read_total,
               max_delay * 1e-3);
  //    snprintf(msg, sizeof(msg), "(%d of %llu found)", found, FLAGS_cycle);
      thread->stats.AddMessage(msg);
  }

  void ReadSequential(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
      DoComplexV2(thread, SagBenchmark::Distribution::Uniform, true, 1., 0., 0., 0., 0.);
    }

  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d.", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % range;
      snprintf(key, sizeof(key), "%016d", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);
      iter->Seek(key);
      if (iter->Valid() && iter->key() == key) found++;
      delete iter;
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        batch.Delete(key);
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) {
    DoDelete(thread, true);
  }

  void DeleteRandom(ThreadState* thread) {
    DoDelete(thread, false);
  }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      RandomGenerator gen;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        const int k = thread->rand.Next() % FLAGS_num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
      thread->stats.Start();
    }
  }

  void Compact(ThreadState* thread) {
    db_->CompactRange(NULL, NULL);
  }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
    fflush(stdout);
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }

  void HeapProfile() {
    char fname[100];
    snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
    WritableFile* file;
    Status s = g_env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      fprintf(stderr, "heap profiling not supported\n");
      g_env->DeleteFile(fname);
    }
  }
};

}  // namespace leveldb

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_max_file_size = leveldb::Options().max_file_size;
  FLAGS_block_size = leveldb::Options().block_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_reuse_logs = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1) {
      FLAGS_max_file_size = n;
    } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
      FLAGS_block_size = n;
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  leveldb::g_env = leveldb::Env::Default();

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      leveldb::g_env->GetTestDirectory(&default_db_path);
      default_db_path += "/dbbench";
      FLAGS_db = default_db_path.c_str();
  }

  leveldb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}

