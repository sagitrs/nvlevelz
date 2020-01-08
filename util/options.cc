// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(2LL * MB),//64LL * MB),
      max_open_files(2000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      max_file_size(2<<20),
      compression(kSnappyCompression),
      reuse_logs(false),
      filter_policy(NULL),
      TEST_nvm_accelerate_method(TEST_NVM_Accelerate_Method::MULTI_MEMTABLE),
      TEST_max_level_in_nvm(0),
      TEST_max_dram_buffer_size(
          #ifdef NVDIMM_ENABLED
            1ULL * GB
          #else
            32ULL * MB
          #endif
          ),
      TEST_max_nvm_buffer_size(
          #ifdef NVDIMM_ENABLED
            32ULL * GB
          #else
            1500ULL * MB
          #endif
          ),
      TEST_nvm_buffer_reserved(
          #ifdef NVDIMM_ENABLED
          (4ULL + 2ULL) * GB
          #else
          (500) * MB
          #endif
          ),
      TEST_max_nvm_memtable_size(4 * MB),
      TEST_halfmax_nvm_memtable_size(0.8 * TEST_max_nvm_memtable_size),
      TEST_min_nvm_memtable_garbage_rate(0.2),
      TEST_pop_limit(0.9),
      TEST_key_hash(0), TEST_write_thread(8),
      TEST_no_double_level0(true),
      TEST_nvskiplist_type(kTypePureSkiplist),
      TEST_background_lock_free(true),
      TEST_hdd_cache_size(256 * MB),
      TEST_cover_range(50),
      TEST_background_infinate(false),
      TEST_hash_div(10),
      TEST_hash_size(
          #ifdef NVDIMM_ENABLED
          2ULL * GB
          #else
          100 * MB
          #endif
          ),
      TEST_hash_full_limit(0.5)
{ }

}  // namespace leveldb


