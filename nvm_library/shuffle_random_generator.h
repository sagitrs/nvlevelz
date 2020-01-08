#ifndef SHUFFLE_RANDOM_GENERATOR_H
#define SHUFFLE_RANDOM_GENERATOR_H

#include "global.h"
#include <vector>
#include <algorithm>
#include <random>
#include <chrono>

struct ShuffleRandomGenerator {
    uint32_t range_;
    std::vector<uint32_t> a;
    uint32_t current_;
    ShuffleRandomGenerator(uint32_t range): range_(range), a(), current_(0) {
        Reset(range);
    }
    void Reset(uint32_t range) {
        range_ = range;
        a.clear();
        for (uint32_t i = 0; i < range; ++i)
            a.push_back(i);

        unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();

        random_shuffle(a.begin(), a.end());
        current_ = range - 1;
    }
    uint32_t Next() {
        current_++;
        if ( current_ >= range_)
            current_ = 0;
        return a[current_];
    }
};

#endif // SHUFFLE_RANDOM_GENERATOR_H
