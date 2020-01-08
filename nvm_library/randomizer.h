#pragma once
#include <iostream>
#include <math.h>
#include <chrono>
#include <limits>
#include <random>
#include "leveldb/slice.h"
#include <array>
#include <algorithm>
#include <sstream>
#include "global.h"
using namespace std;
using std::string;

struct Randomizer{
    int offset, length, retryLimit;
    int passWall;
    int stringLength;

    Randomizer(unsigned int seed = 0){
        if (seed == 0)
            srand(time(0));
        else
            srand(seed);
        offset = 0;
        length = RAND_MAX;
        retryLimit = 0;
        passWall = RAND_MAX / 2;
    }

    void setRandLimit(int a, int b){
        if (b <= a){
            printf("ERROR : invalid limit for randomizer.\n");
            return;
        }
        offset = a;
        length = b - a;
        retryLimit = RAND_MAX % length;
    }

    int randomInt(){
        int t = rand();
        while (t < retryLimit) t = rand();
        return offset + t % length;
    }

    void setPassRate(double p){
        if (0<=p && p<=1){
            passWall = p * RAND_MAX;
        } else
            printf("ERROR : invalid pass rate for randomizer.\n");
    }

    bool randomBool(){
        return rand() < passWall;
    }

    double randomDouble(){
        double A = rand();
        A /= RAND_MAX;
        double a = rand() - RAND_MAX / 2;
        a /= RAND_MAX;
        a /= RAND_MAX;
        return A + a;
    }

    void setStringMode(int length){
        stringLength = length;
    }
    string randomString(){
        setRandLimit('A','Z');
        char s[stringLength+1];
        for (int i=0;i<stringLength;i++)
            s[i] = randomInt();
        s[stringLength] = '\0';
        return s;
    }
};

namespace leveldb {



struct OperationGenerator {
    struct Operation {
        bool type_;
        string key_;
        string value_;
    };
    struct RandomGenerator {
        unsigned seed = static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count());
        std::default_random_engine generator;

        RandomGenerator() : generator(seed) {}

        double Double() {
            return std::generate_canonical<double,std::numeric_limits<double>::digits>(generator);
        }

        long Zipf(double a) {
            double am1, b;

            am1 = a - 1.0;
            b = pow(2.0, am1);

            while (1) {
                double T, U, V, X;

                U = 1.0 - Double();
                V = Double();
                X = floor(pow(U, -1.0/am1));
                /*
                 * The real result may be above what can be represented in a signed
                 * long. Since this is a straightforward rejection algorithm, we can
                 * just reject this value. This function then models a Zipf
                 * distribution truncated to sys.maxint.
                 */
                if (X > LONG_MAX || X < 1.0) {
                    continue;
                }

                T = pow(1.0 + 1.0/X, am1);
                if (V*X*(T - 1.0)/(b - 1.0) <= T/b) {
                    return static_cast<long>(X);
                }
            }
        }
    } gen_;
    enum {Read = 0, Insert = 1, Update = 2};
    enum {Uniform = 0, Zipfian = 1, Latest = 2};
    enum {Orderd = 0, Hashed = 1};
    //Uniform + Orderd = Orderd
    //Uniform + Hashed = Random
    //Zipfian + Orderd = Orderd Zipfian
    //Zipfian + Hashed = Hashed Zipfian
    //Latest + Orderd = Orderd Latest
    //Latest + Hashed = Hashed Latest
    ull operation_count_;
    ull dataset_size_;

    int distribution_type_;
    double distribution_const_;
    ull latest_;

    bool hashed_;
    static const ull SHUFFLE_RANGE = 65536;
    static const ull MAX_UINT_32 = (static_cast<ull>(1) << 32) - 1;
    std::array<ull, SHUFFLE_RANGE> hasher_;

    double read_;
    double new_rate_;

    ull current_;

    OperationGenerator() {
        for (size_t i = 0; i < SHUFFLE_RANGE; ++i)
            hasher_[i] = i;
        unsigned seed = static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count());
        std::shuffle (hasher_.begin(), hasher_.begin() + SHUFFLE_RANGE, std::default_random_engine(seed));
    }

    ull ShuffleHash(ull k) {
        ull r = 0;
        r = (r << 16) | (hasher_[k & 0xFFFF]);
        k >>= 16;
        r = (r << 16) | (hasher_[k & 0xFFFF]);
        k >>= 16;
        r = (r << 16) | (hasher_[k & 0xFFFF]);
        k >>= 16;
        r = (r << 16) | (hasher_[k & 0xFFFF]);
        k >>= 16;
        return r;
    }
    void SetRWProportion(double read, double insert, double rate) {
        assert(read + insert == 1.);
        read_ = read;
        new_rate_ = 1. / rate;
        if (rate > 0)
            dataset_size_ = static_cast<ull>(1. * operation_count_ / rate);
        else
            dataset_size_ = operation_count_;
    }
    void SetOperationCount(ull operation_count) {
        operation_count_ = operation_count;
    }
    void SetDistribution(int t, double c) {
        distribution_type_ = t;
        distribution_const_ = c;
    }
    void SetOrder(bool order) {
        hashed_ = order;
    }
    void Generate(Operation* o) {
        //Random Uniform : 1, 9, 4, 3, 1, 2, 5, 8, ....
        //Orderd Uniform : 1, 2, 3, 4, ..., 1, 2, 3, 4, ....
        //Random Latest  : 1, 1, 1, 9, 9, 9, 2, 2, 2, 4, ....
        //Orderd Latest  : 1, 1, 1, 2, 2, 2, 3, 3, 3, ...
        //Random Zipfian : 5, 9, 5, 9, 5, 1, 9, 5, ...
        //Orderd Zipfian : 1, 2, 1, 3, 2, 3, 2, 1, 1, ...
        if (distribution_type_ == Zipfian) {
            ull k = gen_.Zipf(distribution_const_);
            if (hashed_)
                k = ShuffleHash(k);
        }

        ull k = current_++;
        switch (distribution_type_) {
        case Uniform:
            if (hashed_)
                k = static_cast<ull>(gen_.Double() * dataset_size_);
            else
                k = k % dataset_size_;
            break;
        case Zipfian:
            k = static_cast<ull>(gen_.Zipf(distribution_const_)) % dataset_size_;
            if (hashed_)
                k = ShuffleHash(k);
            break;
        case Latest:
            k = (gen_.Double() >= new_rate_ ? latest_ : (++latest_));
            if (hashed_)
                k = ShuffleHash(k);
            break;
        default:
            assert(false);
        }

        k = k % (dataset_size_ - 1);
        char key[100];
        snprintf(key, sizeof(key), "%016llu", k);
        o->key_.assign(key, 16);

        o->type_ = gen_.Double() > read_;

        //if (o->type_ != Read)
        //    o->value_ = "World";
    }
    std::string Name() {
        std::stringstream ss;
        if (hashed_)
            ss << "Random_";
        else
            ss << "Orderd_";
        switch (distribution_type_) {
        case Uniform:
            break;
        case Zipfian:
            ss << "Zipfian_";
            break;
        case Latest:
            ss << "Latest_";
            break;
        default:
            assert(false);
        }
        ss << "R" << read_ << "W" << 1 - read_ << "_DB=" << dataset_size_ << "_OP=" << operation_count_;
        return ss.str();
    }

};


}
