#pragma once
#include <cstdlib>
#include <ctime>

struct Timer {
    clock_t offset_;
    void start(){
        offset_ = clock();
    }
    double stop(){
        clock_t end = clock();
        double l = end-offset_;
        offset_ = end;
        return l / CLOCKS_PER_SEC;
    }
};
