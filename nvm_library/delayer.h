#pragma once
#include "global.h"
#include <mutex>
struct Delayer {
    ull w_delay, r_delay;
    ull rest;
    std::mutex mutex_;
    static const ull cache_line = 64;

    Delayer();
    Delayer(const ull write_delay_per_opr, const ull read_delay_per_opr);

    void set(const ull write_delay_per_opr, const ull read_delay_per_opr);

    void readDelay(ull operation);

    void writeDelay(ull operation);

};
