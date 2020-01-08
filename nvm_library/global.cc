#include "global.h"
#include <cstdio>
#include <time.h>
#include <math.h>
#include <unistd.h>
byte log2_downfit(ull x){
    if (x == 0) return 0;
    for (byte t = 0; t < 64; ++t){
        if (x < ((ull)1<<t)) return t-1;
    }
    return 63;
}
byte log2_upfit(ull x){
    for (byte t = 0; t < 64; ++t){
        if (x <= ((ull)1<<t)) return t;
    }
    return 64;
}
double log2_exact(ull x) {
    return log2(x);
}

ull pow2(ull x, ull l){
    return x << l;
}

void printbyte(byte c){
    if (c == '\r') printf("\\r");
    else if (c == '\n') printf("\\n");
    else if (c == '\t') printf("\\t");
    else if (' ' <= c && c <= 126)
        putchar(c);
    else
        printf("[%d]",c);
}
void printbyte(byte c, int times){
    for (int i = 0; i < times; ++i) printbyte(c);
}

void nanodelay(long nanosecond){
     static struct timespec req, rem;
     static const long least_delayer = 1000000;
     static const long offset = 105000;
     if (nanosecond < offset) return;
     //assert( nanosecond >= least_delayer );
     req.tv_nsec = nanosecond - offset;
     req.tv_sec = 0;
    nanosleep(&req,&rem);
}
/*
ll GetNano() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return 1LL * ts.tv_sec * 1000000000 + ts.tv_nsec;
}
void nanodelay_clock_gettime(long nanosecond) {
    ll st = GetNano();
    ll se = st + nanosecond;
    while (GetNano() < se);
}*/
ll GetNano() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return 1LL * ts.tv_sec * 1000000000 + ts.tv_nsec;
}
void nanodelay_clock_gettime(long nanosecond) {
    ll st = GetNano();
    if (nanosecond < 35) return;
    ll se = st + nanosecond;
    while (GetNano() < se);
}
ll nanodelay_until(ll until_nano) {
    static const ll offset = 1000;
    ll now = GetNano();
    while (now + offset < until_nano)
        now = GetNano();
    return until_nano - now;
}
void blankprintf(const char* format, ...){

}

std::string NumToString(ull k, size_t width) {
    std::string s;
    s.append(width, '0');
    int tail = width - 1;
    while (k && tail >= 0) {
        s[tail] = '0' + k % 10;
        k = k / 10;
        tail --;
    }
    return s;
}

static inline unsigned long long asm_rdtsc(void) {
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

static inline unsigned long long asm_rdtscp(void) {
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtscp" : "=a"(lo), "=d"(hi)::"rcx");
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}
