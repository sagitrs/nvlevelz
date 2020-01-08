#pragma once

#include <cstdio>
#include <sstream>
#include <string>
#include <limits.h>
#include <assert.h>

#define MIN(a,b) ((a < b) ? a : b)
#define MAX(a,b) ((a > b) ? a : b)

#define debugf blankprintf
//#define debugf printf
#define KB 1024
#define MB 1048576
#define GB (1ULL << 30)
typedef unsigned long long ull;
typedef long long ll;
//typedef ll ssize_t;
typedef unsigned char byte;
typedef uint32_t ul;
typedef ull nvFileHandle;

typedef uint64_t nvAddr;
typedef uint32_t nvOffset;
const ull nvnullptr = 0ULL;
const uint32_t nulloffset = 0xFFFFFFFFUL;

//typedef ull size_t;
//typedef ull off_t;

byte log2_downfit(ull x);
byte log2_upfit(ull x);
double log2_exact(ull x);

ull pow2(ull x, ull l);
void printbyte(byte c);
void printbyte(byte c, int times);
void blankprintf(const char* format, ...);
void nanodelay(long nanosecond);

ll GetNano();
void nanodelay_clock_gettime(long nanosecond);
ll nanodelay_until(ll until_nano);

#define asm_clflush(addr)                   \
({                              \
    __asm__ __volatile__ ("clflush %0" : : "m"(*addr)); \
})

#define asm_mfence()                \
({                      \
    __asm__ __volatile__ ("mfence":::"memory");    \
})
void pflush(uint64_t *addr);
void init_pflush(int cpu_speed_mhz, int write_latency_ns);

std::string NumToString(ull k, size_t width);

#define NV_PAGE(x) static_cast<unsigned long>(x >> 32)
#define NV_BASE(x) static_cast<unsigned long>(x & 0xffffffffULL)
