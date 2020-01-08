// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "nvm_library/nvm_library.h"
#include <iostream>
#include <map>
#include "nvm_library/multitable.h"
#include "nvm_library/global.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      //mems_(new MultiTable(internal_comparator_,1)),
      //mem_(NULL),
      imm_(NULL),
      nvmems_(nullptr),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL) {
  has_imm_.Release_Store(NULL);
  nvmems_ = new nvMultiTable(this, raw_options, dbname_);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  //if (mem_ != NULL) mem_->Unref();

  delete nvmems_;
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s;
  if (options_.TEST_nvm_accelerate_method == NVM_DISABLED)
      s = env_->NewWritableFile(manifest, &file);
  else {
      s = env_->NewWritableNVMFile(manifest, &file);
  }

  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kNVMLogFile:
          keep = (nvmems_->FileInUse(number));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
      mem->Unref();
      mem = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == NULL);
    assert(log_ == NULL);
    assert(mem_ == NULL);
    uint64_t lfile_size;

//    if (env_->GetFileSize(fname, &lfile_size).ok() &&
//        env_->NewAppendableFile(fname, &logfile_).ok()) {
    if (env_->GetFileSize(fname, &lfile_size).ok()) {
      bool logfileCreated = env_->NewAppendableNVMFile(fname, &logfile_).ok();
      //if (options_.TEST_nvm_accelerate_method != NO_BUFFER_NO_LOG) {
      if (logfileCreated){
          Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
          log_ = new log::Writer(logfile_, lfile_size);
          logfile_number_ = log_number;
          if (mem != NULL) {
            //mems_->PushMemTable(mem);
            nvmems_->Init(mem);
            mem = NULL;
          } else {
            // mem can be NULL if lognum exists but was empty.
            //MemTable* m = new MemTable(internal_comparator_);
            //m->Ref();
            //mems_->PushMemTable(m);
            nvmems_->Init(nullptr);

          }
      }

    }
  }

  if (mem != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  //if (mem->ApproximateMemoryUsage() <= 10000) {
  //    printf("MemTable Size : %lu\n", mem->ApproximateMemoryUsage());
  //}
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    //printf("Compact Memtable : [%s] - [%s]\n", min_user_key.ToString().c_str(), max_user_key.ToString().c_str());
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
      dc_.AddSiniorCompaction(level,1);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  dc_.AddSiniorCompactionTime(stats.micros);
  auto Chaos = [] (const Slice& a, const Slice& b) {
      ull x,y;
      sscanf(a.data(), "%llu", &x);
      sscanf(b.data(), "%llu", &y);
      return (double)(y-x) / 100000000;
  };
  dc_.AddChaos(Chaos(meta.smallest.user_key(), meta.largest.user_key()));
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::JuniorCompaction(void* nvmem) {
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);
  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  uint64_t temp_timer = env_->NowMicros();
  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
  dc_.AddSiniorCompactionTime(env_->NowMicros() - temp_timer);
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (nvmems_->level0_.Size() == 0 &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  if (reinterpret_cast<DBImpl*>(db)->options_.TEST_background_infinate)
    reinterpret_cast<DBImpl*>(db)->BackgroundCallInfinite();
  else
    reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}
void DBImpl::BackgroundCallInfinite() {
  while (true) {
    MutexLock l(&mutex_);
    assert(bg_compaction_scheduled_);
    if (shutting_down_.Acquire_Load()) {
      break;// No more background work when shutting down.
    } else if (!bg_error_.ok()) {
      break;// No more background work after a background error.
    } else if (nvmems_->level0_.Size() == 0 && !versions_->NeedsCompaction()) {
        mutex_.Unlock();
        env_->SleepForMicroseconds(1000);   // Mo Yu
        mutex_.Lock();
    } else {
      BackgroundCompaction();
    }
    // Previous compaction may have produced too many files in a level,
    // so reschedule another compaction if needed.
    //MaybeScheduleCompaction();
  }
  bg_compaction_scheduled_ = false;
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  uint64_t temp_timer1 = 0;
  if (nvmems_->level0_.Size() > 0) {
      if (options_.TEST_no_double_level0) {
          nvMemTable* mem = nullptr;
          nvmems_->level0_.Front(&mem);
          //nvmems_->level0_.front();
          Compaction* c = versions_->PickLevel0Compaction(mem);
          CompactionState* compact = new CompactionState(c);
          Status status = DoCompactionWork(compact);

          if (!status.ok()) {
            RecordBackgroundError(status);
          }
          CleanupCompaction(compact);
          c->ReleaseInputs();
          DeleteObsoleteFiles();

          delete c;

          if (status.ok()) {
            // Done
          } else if (shutting_down_.Acquire_Load()) {
            // Ignore compaction errors found during shutting down
          } else {
            Log(options_.info_log,
                "Compaction error: %s", status.ToString().c_str());
          }
      } else {
          nvMemTable* mem = nullptr;
          nvmems_->level0_.Front(&mem);
          imm_ = mem->Immutable(mem->Seq());
          CompactMemTable();
          nvmems_->level0_.PopFront(&mem);
          //nvmems_->level0_.pop_front();
          mem->Unref();
      }
    return;
  }
  temp_timer1 = env_->NowMicros();

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    //printf("%d: [%s] , [%s]\n", m->level,m->begin->user_key().ToString().c_str(), m->end->user_key().ToString().c_str());
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }
  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
  //else if (!is_manual && c->IsTrivialMove() && c->level()+1 <= options_.TEST_max_level_in_nvm) {
     // Move file to next level
      dc_.AddMajorCompaction(0, c->level(), 1);
      //trivial_compaction++;
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);

    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }

  dc_.AddMajorCompactionTime(env_->NowMicros() - temp_timer1);
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s;
  s = env_->NewWritableFile(fname, &compact->outfile);

  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  if (compact->compaction->level() == 0 && options_.TEST_no_double_level0)
      ;
  else
      assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work


  Iterator* input = nullptr;
  nvMemTable* mem = nullptr;
  if (compact->compaction->level() == 0 && options_.TEST_no_double_level0) {
      nvmems_->level0_.lock_.ReadLock();
      nvmems_->level0_.Front(&mem);
      nvmems_->level0_.lock_.Unlock();
      input = versions_->MakeInputIteratorWithoutLevel0(compact->compaction, mem, compact->smallest_snapshot);
      nvmems_->isCompactingLevel0_ = true;
      mutex_.Unlock();
  } else {
      mutex_.Unlock();
      input = versions_->MakeInputIterator(compact->compaction);
  }
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
      /*
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    } */
      if (nvmems_->level0_.Size() > 0 && !options_.TEST_no_double_level0) {
        fprintf(stderr, "Compacting nvMemTable in level 0..\n");
        fflush(stderr);
        const uint64_t imm_start = env_->NowMicros();

        nvMemTable* mem = nullptr; nvmems_->level0_.Front(&mem);
        imm_ = mem->Immutable(mem->Seq());
        //imm_->Ref();

        mutex_.Lock();
        CompactMemTable();
        //nvmems_->level0_.pop_front();
        nvmems_->level0_.PopFront(&mem);
        mem->Unref();
        mutex_.Unlock();

        imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  if (mem != nullptr) {
      assert(compact->compaction->level() == 0);
      stats.bytes_read += mem->StorageUsage();
      for (int i = 0; i < compact->compaction->num_input_files(1); i++) {
        stats.bytes_read += compact->compaction->input(1, i)->file_size;
      }
  } else {
      for (int which = 0; which < 2; which++) {
        for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
          stats.bytes_read += compact->compaction->input(which, i)->file_size;
        }
      }
  }

  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }
  //nvmems_->rwlock_.WriteLock();
  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
    if (mem != nullptr) {
        nvMemTable* tmp = nullptr;
        nvmems_->level0_.lock_.WriteLock();
        nvmems_->level0_.PopFront(&tmp);
        nvmems_->level0_.lock_.Unlock();
        assert( mem == tmp );
        //assert( mem == nvmems_->level0_.front() );
        //nvmems_->level0_.pop_front();
        mem->Unref();   // Delete level 0 file.
        nvmems_->isCompactingLevel0_ = false;
    }
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  //nvmems_->rwlock_.Unlock();
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};
struct IterState_MultiVersion {
  port::Mutex* mu;
  Version* version;
  nvMultiTable* mem;
  MemTable* imm;
};
static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
static void CleanupIteratorState_MultiVersion(void* arg1, void* arg2) {
  IterState_MultiVersion* state = reinterpret_cast<IterState_MultiVersion*>(arg1);
  state->mu->Lock();
  //state->mem->ProtectRelease();
  //state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::NewInternalIterator_MultiVersion(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState_MultiVersion* cleanup = new IterState_MultiVersion;
  nvmems_->rwlock_.WriteLock();
  mutex_.Lock();
  if (options_.TEST_nvm_accelerate_method == BUFFER_WITH_LOG) {
      nvmems_->ClearWriteBuffer();
  }
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(nvmems_->NewIterator());
  //nvmems_->ProtectAllMemtable();

  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = nvmems_;
  //cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState_MultiVersion, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  nvmems_->rwlock_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::GetOld(const ReadOptions& options,
                      const Slice& key,
                      std::string* value) {

    Status s = Status::OK();
    //MutexLock l(&mutex_);
    SequenceNumber snapshot;
    if (options.snapshot != NULL) {
      snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
    } else {
      snapshot = versions_->LastSequence();
    }

    //MultiTable* mem = mems_;
    string key_hashed;
    if (options_.TEST_key_hash)
        KeyHash(key.ToString(), &key_hashed);
    LookupKey lkey(options_.TEST_key_hash ? key_hashed : key, snapshot);

    if (options_.TEST_nvm_accelerate_method == BUFFER_WITH_LOG) {
        nvmems_->rwlock_.ReadLock();
        if (nvmems_->cache_->Get_(key, value, &s)) {
            nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::HashLog);
            nvmems_->rwlock_.Unlock();
            return s;
        }
        if (nvmems_->WhereIs(key)->Get(lkey, value, &s)) {
            nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::nvMemTable);
            nvmems_->rwlock_.Unlock();
            //dc_.AddLocateType(1);
            return s;
          // Done
        }

        if (nvmems_->GetInLevel0(lkey, value, &s)) {
            nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::Level0);
            nvmems_->rwlock_.Unlock();
          // Done
            //dc_.AddLocateType(2);
            return s;
        }
        nvmems_->rwlock_.Unlock();

        mutex_.Lock();
        Version* current = versions_->current();
        Version::GetStats stats;
        current->Ref();
        {
            mutex_.Unlock();
            s = current->Get(options, lkey, value, &stats);
            if (s.ok())
              nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::LevelK);
            else
              nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::NotFound);
            mutex_.Lock();
        }
        if (current->UpdateStats(stats)) {
          MaybeScheduleCompaction();
        }
        current->Unref();

        mutex_.Unlock();
            //if (s.ok() || s.IsNotFound()) {
                //nvmems_->rwlock_.WriteLock();
                //nvmems_->cache_.Add(key, (s.ok() ? *value : Slice()));
                //nvmems_->rwlock_.Unlock();
            //}
        return s;
    } else if (options_.TEST_nvm_accelerate_method == NO_BUFFER_NO_LOG) {
        nvmems_->rwlock_.ReadLock();
        nvMemTable* mem = nvmems_->WhereIs(key);
        //nvmems_->ic_.Get(lkey, value, &s);

        if (mem->Get(lkey, value, &s)) {
            nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::nvMemTable);
            //dc_.AddLocateType(1);
            //mem->Unlock();
            nvmems_->rwlock_.Unlock();
            // Done
        } else if (nvmems_->GetInLevel0(lkey, value, &s)) {
            nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::Level0);
            // Done
            //dc_.AddLocateType(2);
            //mem->Unlock();
            nvmems_->rwlock_.Unlock();
        } else {
            //mem->Unlock();
            nvmems_->rwlock_.Unlock();

            mutex_.Lock();
            Version* current = versions_->current();
            Version::GetStats stats;
            current->Ref();
            mutex_.Unlock();

            s = current->Get(options, lkey, value, &stats);

            mutex_.Lock();
            if (s.ok())
                nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::LevelK);
            else
                nvmems_->global_ic_.AddLocation(InfoCollector::KeyValueInfo::NotFound);

            if (current->UpdateStats(stats)) {
              MaybeScheduleCompaction();
            }

            current->Unref();
            mutex_.Unlock();
        }
        //mutex_.Lock();
    } else {
        assert(false);
    }
    return s;
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
    if (options_.TEST_nvm_accelerate_method != MULTI_MEMTABLE)
        GetOld(options, key, value);
    Status s = Status::OK();
    SequenceNumber snapshot = options.snapshot != NULL ?
                reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_ :
                versions_->LastSequence();

    LookupKey lkey(key, snapshot);
    nvmems_->rwlock_.ReadLock();
    nvMemTable* mem = nvmems_->WhereIs(key);
    if (mem->Get(lkey, value, &s)) {
        nvmems_->rwlock_.Unlock();
    } else if (nvmems_->GetInLevel0(lkey, value, &s)) {
        nvmems_->rwlock_.Unlock();
    } else {
        nvmems_->rwlock_.Unlock();

        mutex_.Lock();
        Version* current = versions_->current();
        Version::GetStats stats;
        current->Ref();
        mutex_.Unlock();

        s = current->Get(options, lkey, value, &stats);

        mutex_.Lock();
        if (current->UpdateStats(stats))
          MaybeScheduleCompaction();

        current->Unref();
        mutex_.Unlock();
    }
    return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter;
  iter = NewInternalIterator_MultiVersion(options, &latest_snapshot, &seed);

  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::WriteLogFree(const WriteOptions& options, WriteBatch* my_batch) {
    size_t total = WriteBatchInternal::Count(my_batch);
    //uint64_t last_sequence = versions_->LastSequence();
    //WriteBatchInternal::SetSequence(my_batch, last_sequence + 1);
    //last_sequence += total;
    Slice input(my_batch->rep_);

    static const int kHeader = 12;
    input.remove_prefix(kHeader); // kHeader = 12;
    Slice key, value;
    int found = 0;
    //nvMemTable * mem = nullptr;
    string key_hashed;

    while (!input.empty()) {
        found++;
        char tag = input[0];
        input.remove_prefix(1);
        if (!GetLengthPrefixedSlice(&input, &key))
          return Status::Corruption("bad WriteBatch Put");

        if (options_.TEST_key_hash) {
          KeyHash(key.ToString(), &key_hashed);
          key = key_hashed;
        }
        if (tag == kTypeValue) {
            if (!GetLengthPrefixedSlice(&input, &value))
                return Status::Corruption("bad WriteBatch Put");
        } else {
            value = Slice();
        }
        nvMemTable* mem = nullptr;;
        BackgroundWriter* writer = nullptr;
        //MutexLock l(&mutex_);
/*
        if (nvmems_->level0_.size() >= config::kL0_SlowdownWritesTrigger) {
            if (use_rw_lock) nvmems_->rwlock_.Unlock();
            env_->SleepForMicroseconds(50 * (1 + nvmems_->level0_.size() - config::kL0_SlowdownWritesTrigger));
            if (use_rw_lock) nvmems_->rwlock_.ReadLock();
        }
*/

        //nvmems_->ic_.Add(0, tag == kTypeValue ? kTypeValue : kTypeDeletion, key, value);
        //write_mutex_.Lock();
        while (true) {
            nvmems_->rwlock_.ReadLock();
            mem = nvmems_->WhereIs(key);

            mem->Lock();
            if (mem->Immutable()) {
                mem->Unlock();
                nvmems_->rwlock_.Unlock();
                env_->SleepForMicroseconds(1);
                continue;
            }

            mem->Ref();
            if (mem->HasRoomForWrite(key, value, nvmems_->level0_.Size() >= config::kL0_SlowdownWritesTrigger))
                break;
            ll freeze_start = GetNano();
            mem->SetImmutable(true);
            mem->Unref();

            mem->Unlock();

            nvmems_->rwlock_.Unlock();
            {
                nvmems_->rwlock_.WriteLock();
                mutex_.Lock();
                mem = nvmems_->WhereIs(key);
                mem->Lock();

                MakeRoomForWrite(mem->LeftBound());
                nvmems_->Pop(versions_->current(), mem->LeftBound());
                MaybeScheduleCompaction();

                ll freeze_end = GetNano();
                nvmems_->global_ic_.CompactionTime(freeze_end - freeze_start);

                mutex_.Unlock();
                nvmems_->rwlock_.Unlock();
            }
        }

        nvmems_->ByteCount(key.size() + (tag == kTypeDeletion ? 0 : value.size()) + 32);
        nvmems_->rwlock_.Unlock();

        //write_mutex_.Unlock();

        mem->Add(0, tag == kTypeValue ? kTypeValue : kTypeDeletion,
                 key, value);
        mem->Unref();
        mem->Unlock();

    }

    assert(found == total);
    return Status::OK();
}
Status DBImpl::WriteMultiCache(const WriteOptions& options, WriteBatch* my_batch) {
    Writer w(&write_mutex_);
    w.batch = my_batch;
    w.sync = options.sync;
    w.done = false;

    MutexLock l(&write_mutex_);
    //write_mutex_.Lock();
    writers_.push_back(&w);
    while (!w.done && &w != writers_.front()) {
      w.cv.Wait();
    }
    if (w.done) {
      return w.status;
    }

    // May temporarily unlock and wait.
    Status status = Status::OK();
    //Status status = MakeRoomForWrite(my_batch == NULL);
    uint64_t last_sequence = versions_->LastSequence();
    Writer* last_writer = &w;
    if (my_batch != NULL) {  // NULL batch is for compactions
        WriteBatch* updates = BuildBatchGroup(&last_writer);
        WriteBatchInternal::SetSequence(updates, last_sequence + 1);
        int counts = WriteBatchInternal::Count(updates);
        last_sequence += counts;

      // Add to log and apply to memtable.  We can release the lock
      // during this phase since &w is currently responsible for logging
      // and protects against concurrent loggers and concurrent writes
      // into mem_.

    //MemTableInserter inserter;
        SequenceNumber seq = DecodeFixed64(updates->rep_.data());
        //inserter.mem_ = memtable;
        Slice input(updates->rep_);
        if (input.size() < 12) {
            return Status::Corruption("malformed WriteBatch (too small)");
        }

        input.remove_prefix(12);
        Slice key, value;
        int found = 0;
        while (!input.empty()) {
          found++;
          char tag = input[0];
          input.remove_prefix(1);
          switch (tag) {
            case kTypeValue:
              if (GetLengthPrefixedSlice(&input, &key) &&
                  GetLengthPrefixedSlice(&input, &value)) {
                //handler->Put(key, value);
                  //mem->Add(seq, kTypeValue, key, value);
              } else {
                return Status::Corruption("bad WriteBatch Put");
              }
              break;
            case kTypeDeletion:
              if (GetLengthPrefixedSlice(&input, &key)) {
                  value = Slice();
                //handler->Delete(key);
                  //mem->Add(seq, kTypeDeletion, key, value);
              } else {
                    return Status::Corruption("bad WriteBatch Delete");
              }
              break;
            default:
              return Status::Corruption("unknown WriteBatch tag");
          }
          while (true) {
              nvmems_->rwlock_.ReadLock();
              nvMemTable* mem = nvmems_->WhereIs(key);
              //nvmems_->cache_.Add(key, value);
              if (mem->PreWrite(key, value, true)) {
                  BackgroundWriter_LockFree* writer = nvmems_->WhoIs(mem->LeftBound());
                  nvAddr block_addr = nvmems_->cache_->Add(key, value);

                  writer->PushWorkToQueue(mem, block_addr);
                  if (nvmems_->cache_->Full()) {
                      nvmems_->rwlock_.Unlock();
                      ll freeze_start = GetNano();
                      nvmems_->rwlock_.WriteLock();
                      nvmems_->ClearWriteBuffer();
                      ll freeze_end = GetNano();
                      nvmems_->global_ic_.CompactionTime(freeze_end - freeze_start);
                  }
                  nvmems_->rwlock_.Unlock();
                  break;
              }
              //nvmems_->oprs_++;
              nvmems_->rwlock_.Unlock();

              nvmems_->rwlock_.WriteLock();
              mutex_.Lock();
              ll freeze_start = GetNano();
              Version* current = versions_->current();
              //current->Ref();
              //mutex_.Unlock();
              nvmems_->ClearWriteBuffer();
              MakeRoomForWrite(mem->LeftBound());
              nvmems_->Pop(current, mem->LeftBound());
              MaybeScheduleCompaction();
              //MakeRoomForWrite(false);
              //mutex_.Lock();
              //current->Unref();
              ll freeze_end = GetNano();
              nvmems_->global_ic_.CompactionTime(freeze_end - freeze_start);
              mutex_.Unlock();
              nvmems_->rwlock_.Unlock();
          }
        }
        if (found != counts) {
          status = Status::Corruption("WriteBatch has wrong count");
        } else {
          status = Status::OK();
        }
        if (updates == tmp_batch_)
            tmp_batch_->Clear();
        versions_->SetLastSequence(last_sequence);
    }

    while (true) {
      Writer* ready = writers_.front();
      writers_.pop_front();
      if (ready != &w) {
        ready->status = status;
        ready->done = true;
        ready->cv.Signal();
      }
      if (ready == last_writer) break;
    }

    // Notify new head of write queue
    if (!writers_.empty()) {
      writers_.front()->cv.Signal();
    }

    return status;
}
Status DBImpl::WriteMultiMemTableSequentially(const WriteOptions& options, WriteBatch* my_batch, ul level0_size) {
    size_t total = WriteBatchInternal::Count(my_batch);
    Slice input(my_batch->rep_);
    static const int kHeader = 12;
    input.remove_prefix(kHeader); // kHeader = 12;
    Slice key, value;
    int found = 0;
    string key_hashed;

    while (!input.empty()) {
        found++;
        char tag = input[0];
        input.remove_prefix(1);
        if (!GetLengthPrefixedSlice(&input, &key))
          return Status::Corruption("bad WriteBatch Put");

        if (options_.TEST_key_hash) {
          KeyHash(key.ToString(), &key_hashed);
          key = key_hashed;
        }
        if (tag == kTypeValue) {
            if (!GetLengthPrefixedSlice(&input, &value))
                return Status::Corruption("bad WriteBatch Put");
        } else {
            value = Slice();
        }
        nvMemTable* mem = nullptr;
        while (true) {
            ul standard = nvmems_->cache_policy_.standard_immutablequeue_size_ / 2;
            nvmems_->rwlock_.WriteLock();
            if (level0_size > standard) {
                ul base = 200;  // 200ns * 1024 = 204.8us
                double slow_rate = 20. * (level0_size - standard) / standard;
                ull delay_time = pow2(base, slow_rate);
                nanodelay_clock_gettime(delay_time);
            }
            mem = nvmems_->WhereIs(key);
            if (mem->HasRoomForWrite(key, value, level0_size == 0))
                break;
            ll freeze_start = GetNano();
            {
                mutex_.Lock();
                MakeRoomForWrite(mem->LeftBound());
                nvmems_->Pop(versions_->current(), mem->LeftBound());
                MaybeScheduleCompaction();
                mutex_.Unlock();
            }
            ll freeze_end = GetNano();
            nvmems_->global_ic_.CompactionTime(freeze_end - freeze_start);

            nvmems_->rwlock_.Unlock();
        }

        nvmems_->ByteCount(key.size() + (tag == kTypeDeletion ? 0 : value.size()) + 32);
        mem->Add(0, tag == kTypeValue ? kTypeValue : kTypeDeletion, key, value);
        nvmems_->rwlock_.Unlock();
        //mem->Unref();
    }

    assert(found == total);
    return Status::OK();
}
Status DBImpl::WriteMultiMemTable(const WriteOptions& options, WriteBatch* my_batch) {
    size_t total = WriteBatchInternal::Count(my_batch);
    Slice input(my_batch->rep_);
    static const int kHeader = 12;
    input.remove_prefix(kHeader); // kHeader = 12;
    Slice key, value;
    int found = 0;
    string key_hashed;

    while (!input.empty()) {
        found++;
        char tag = input[0];
        input.remove_prefix(1);
        if (!GetLengthPrefixedSlice(&input, &key))
          return Status::Corruption("bad WriteBatch Put");

        if (options_.TEST_key_hash) {
          KeyHash(key.ToString(), &key_hashed);
          key = key_hashed;
        }
        if (tag == kTypeValue) {
            if (!GetLengthPrefixedSlice(&input, &value))
                return Status::Corruption("bad WriteBatch Put");
        } else {
            value = Slice();
        }
        nvMemTable* mem = nullptr;
        while (true) {
            nvmems_->rwlock_.ReadLock();
            mem = nvmems_->WhereIs(key);

            mem->Lock();
            if (mem->Immutable()) {
                mem->Unlock();
                nvmems_->rwlock_.Unlock();
                env_->SleepForMicroseconds(1);
                continue;
            }

            mem->Ref();
            if (mem->HasRoomForWrite(key, value, nvmems_->level0_.Size() == 0))
                break;
            ll freeze_start = GetNano();
            mem->SetImmutable(true);
            mem->Unref();

            mem->Unlock();

            nvmems_->rwlock_.Unlock();
            {
                nvmems_->rwlock_.WriteLock();
                mutex_.Lock();
                mem = nvmems_->WhereIs(key);
                mem->Lock();

                MakeRoomForWrite(mem->LeftBound());
                nvmems_->Pop(versions_->current(), mem->LeftBound());
                MaybeScheduleCompaction();

                ll freeze_end = GetNano();
                nvmems_->global_ic_.CompactionTime(freeze_end - freeze_start);

                mutex_.Unlock();
                nvmems_->rwlock_.Unlock();
            }
        }

        nvmems_->ByteCount(key.size() + (tag == kTypeDeletion ? 0 : value.size()) + 32);
        nvmems_->rwlock_.Unlock();

        //write_mutex_.Unlock();

        mem->Add(0, tag == kTypeValue ? kTypeValue : kTypeDeletion,
                 key, value);
        mem->Unref();
        mem->Unlock();

    }

    assert(found == total);
    return Status::OK();
}
Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
    if (my_batch == nullptr)
        return Status::OK();
    if (options_.TEST_nvm_accelerate_method == TEST_NVM_Accelerate_Method::MULTI_MEMTABLE) {
        //return WriteMultiMemTableSequentially(options, my_batch);
        ul level0_size = nvmems_->level0_.Size();
        if (level0_size * 2 >= nvmems_->cache_policy_.standard_immutablequeue_size_)
            return WriteMultiMemTableSequentially(options, my_batch, level0_size);
        else
            return WriteMultiMemTable(options, my_batch);
    }
    if (options_.TEST_nvm_accelerate_method == NO_BUFFER_NO_LOG)
        return WriteLogFree(options, my_batch);
    else if (options_.TEST_nvm_accelerate_method == BUFFER_WITH_LOG)
        return WriteMultiCache(options, my_batch);
    assert(false);
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(const Slice& except) {
  mutex_.AssertHeld();
  ul standard = nvmems_->cache_policy_.standard_immutablequeue_size_;
  if (nvmems_->level0_.Size() * 2 >= standard) {
      if (nvmems_->isCompactingLevel0_ == false)
          MaybeScheduleCompaction();
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      mutex_.Lock();
  }
  while (nvmems_->level0_.Size() >= standard || !nvmems_->HasRoomForNewMem()) {
      if (nvmems_->level0_.Size() == 0 && !nvmems_->HasRoomForNewMem()) {
          Slice exc = except;
          nvmems_->ForcePop(nullptr, &exc);
      }
      if (nvmems_->isCompactingLevel0_ == false)
          MaybeScheduleCompaction();
      fprintf(stderr, "Level 0 Full. Triger delay = 100ms.\n");
      fflush(stderr);
      mutex_.Unlock();
      env_->SleepForMicroseconds(100000);
      mutex_.Lock();
  }

  return Status::OK();
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == Slice("stats")) {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == Slice("sstables")) {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == Slice("approximate-memory-usage")) {
    size_t total_usage = options_.block_cache->TotalCharge();
    //if (mem_) {
    //if (!mems_->isEmpty()) {
        total_usage += nvmems_->StorageUsage();
    //  total_usage += mem_->ApproximateMemoryUsage();
    //}
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  //if (s.ok() && impl->mem_ == NULL) {
  if (s.ok()) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    impl->logfile_number_ = new_log_number;
    impl->nvmems_->Init(nullptr);
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    //assert(!impl->mems_->isEmpty());
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}


void* DBImpl::InfoCollection(int get_info) {
    if (get_info == -1) {
        return nvmems_->global_ic_.Report();
    }
    if (get_info == 0) {
        //nvmems_->ic_.Clear();
        return nullptr;
    }
    if (get_info == 1) {
        return nullptr;
        //return nvmems_->ic_.Report();
    }
}

}  // namespace leveldb
