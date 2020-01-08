#include "nvm_options.h"
NVM_Options::NVM_Options(NVM_MemoryBlock* memblock) :
  block(memblock),
  max_level(40),
  page_size(4096),
  basic_offset(8),
  write_delay_per_cache_line(600),//500 - 30),
  read_delay_per_cache_line(0),//100 - 30),
  cache_line_size(64), bandwidth(5000ULL * MB) {
}
