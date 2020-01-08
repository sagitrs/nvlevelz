#include "delayer.h"

Delayer::Delayer() {
    w_delay = r_delay = rest = 0;
}

Delayer::Delayer(const ull write_delay_per_opr, const ull read_delay_per_opr){
    set(write_delay_per_opr,read_delay_per_opr);
    rest = 0;
}

void Delayer::set(const ull write_delay_per_opr, const ull read_delay_per_opr){
    w_delay = write_delay_per_opr;
    r_delay = read_delay_per_opr;
}

void Delayer::readDelay(ull operation){
    ull time = rest + operation * r_delay;
    rest = time % cache_line;
    if (time >= cache_line) {
        mutex_.lock();
        nanodelay(time/cache_line);
        mutex_.unlock();
    }
}

void Delayer::writeDelay(ull operation){
    ull time = rest + operation * w_delay;
    rest = time % cache_line;
    if (time >= cache_line) {
        mutex_.lock();
        nanodelay(time/cache_line);
        mutex_.unlock();
    }
}
