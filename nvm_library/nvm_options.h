#ifndef NVM_OPTIONS_H
#define NVM_OPTIONS_H
#include "global.h"
#include "sysnvm.h"

struct NVM_MemoryBlock;

struct NVM_Options {
  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  //byte** mains;
  NVM_MemoryBlock* block;

  // If true, the database will be created if it is missing.
  // Default: false
  //ull main_size;

  // If true, an error is raised if the database already exists.
  // Default: false
  byte max_level;

  // If true, the implementation will do aggressive checking of the
  // data it is processing and will stop early if it detects any
  // errors.  This may have unforeseen ramifications: for example, a
  // corruption of one DB entry may cause a large number of entries to
  // become unreadable or for the entire DB to become unopenable.
  // Default: false
  ull page_size;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  ull basic_offset;

  ull write_delay_per_cache_line;
  ull read_delay_per_cache_line;
  ull cache_line_size;
  ull bandwidth;

  // Create an Options object with default values for all fields.
  NVM_Options(NVM_MemoryBlock* memblock);
};
#endif // NVM_OPTIONS_H
