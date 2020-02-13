#ifndef BENCHMARK_H
#define BENCHMARK_H
#include "global.h"
#include "leveldb/slice.h"
#include <vector>
#include "util/random.h"
#include "util/testutil.h"
#include "zipfian_generator.h"
#include <unordered_set>
#include "murmurhash.h"
#include <stdlib.h>
#include <stdio.h>
#include "shuffle_random_generator.h"

using leveldb::Slice;
using leveldb::Random;
using leveldb::test::CompressibleString;
using std::unordered_set;
struct SagBenchmark {
    enum OperationType {Read = 0, Insert = 1, Update = 2, Scan = 3, ReadModifyWrite = 4};
    enum Distribution {Uniform = 0, Zipfian = 1, Latest = 2, Static = 3};
    enum InsertOrder {Hashed = 0, Ordered = 1};
    enum DatasetType {dTypeOperation,  dTypeRecord};

    bool pre_generate_;

    uint32_t key_length_;
    uint32_t value_length_;
    Distribution value_length_distribution_;

    double read_;
    double insert_;
    double update_;
    double scan_;
    double read_modify_write_;
    struct RandTypeHelper {
        double read_, insert_, update_, scan_, read_modify_write_;
        RandTypeHelper(double read, double insert, double update, double scan, double read_modify_write):
            read_(read), insert_(read_ + insert), update_(insert_ + update), scan_(update_ + scan), read_modify_write_(scan_ + read_modify_write) {}
        OperationType Check(double k) {
            if (k < read_)
                return Read;
            else if (k < insert_)
                return Insert;
            else if (k < update_)
                return Update;
            else if (k < scan_)
                return Scan;
            else
                return ReadModifyWrite;
        }
        void Set(double read, double insert, double update, double scan, double read_modify_write) {
            read_ = read;
            insert_ = read_ + insert;
            update_ = insert_ + update;
            scan_ = update_ + scan;
            read_modify_write_ = scan_ + read_modify_write;
        }
    } rand_type_helper_;

    Distribution global_request_distribution_;
    InsertOrder global_insert_order_;
    bool random_full_range_;

    //uint32_t thread_count_;
    ull operation_count_;
    ull old_record_count_, new_record_count_, dbsize_;
    ull last_insert_, last_update_;
    //double record_rate_;
    // Total Items = record_count + operation_count * insert_
    //ull dbsize_;

    uint32_t max_scan_length_;
    Distribution scan_length_distribution_;

    //char* db_path_;

    struct Allocator {
    public:
        std::vector<byte*> block_;
        static const size_t BlockSize = 1 * MB;

        byte* alloc_ptr_;
        ull alloc_remaining_;

        Allocator() :
            block_(),
            alloc_ptr_(nullptr), alloc_remaining_(0){}
        ~Allocator() {
            DisposeAll();
        }
        byte* Allocate(size_t size) {
            if (size > alloc_remaining_)
                return AllocateNewBlock(size);
            byte* ans = alloc_ptr_;
            alloc_remaining_ -= size;
            alloc_ptr_ += size;
            return ans;
        }
        byte* AllocateNewBlock(size_t size) {
            alloc_ptr_ = new byte[BlockSize];
            block_.push_back(alloc_ptr_);
            alloc_remaining_ = BlockSize;
            return Allocate(size);
        }
        void DisposeAll() {
            for (size_t i = 0; i < block_.size(); ++i) {
                delete[] block_[i];
            }
            block_.clear();
        }

        Allocator(const Allocator&) = delete;
        void operator=(const Allocator&) = delete;
    } arena_;

    class RandomGenerator {
     private:
      std::string data_;
      int pos_;

     public:
      RandomGenerator() {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < 1048576) {
          // Add a short fragment that is as compressible as specified
          // by FLAGS_compression_ratio.
          CompressibleString(&rnd, 0.5, 100, &piece);
          data_.append(piece);
        }
        pos_ = 0;
      }

      Slice Generate(size_t len) {
        if (pos_ + len > data_.size()) {
          pos_ = 0;
          assert(len < data_.size());
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
      }
    } value_generator_;
    ZipfianGenerator *key_generator_, *valuesize_generator_, *rand_;
    //ShuffleRandomGenerator shuffle_rand_;

    unordered_set<uint32_t> inserted_;

    struct Operation {
        OperationType type_;
        //Slice key_;
        ull key_;
        Slice value_;   // value_.size() = Scan Length
        Operation(OperationType type, ull key, const Slice& value) : type_(type), key_(key), value_(value) {}
        Operation(): type_(Read), key_(0), value_() {}
    };
    Operation* oprs_;
    Operation opr_;
    //ull current_opr_;
    Operation& operator[] (ull i) {
        //static __thread Operation temp_op;
        if (pre_generate_)
            return oprs_[i];
        else {
            opr_.type_ = rand_type_helper_.Check(rand_->nextDouble());
            opr_.key_ = RandKey(opr_.type_);
            opr_.value_ = RandValue();
            return opr_;
        }
    }
    ull OperationCount() const { return operation_count_; }
    //const Operation& Current() const { return oprs_[current_opr_]; }
    //const Operation& Next() { return oprs_[current_opr_++]; }
    //void SeekToWarmupFirst() { current_opr_ = 0; }
    //void SeekToFirst() { current_opr_ = record_count_; }
    //bool End() { return current_opr_ >= record_count_ + operation_count_; }

    SagBenchmark() :
        pre_generate_(true),
        key_length_(16), value_length_(100), value_length_distribution_(Static),
        read_(0), insert_(1), update_(0), scan_(0), read_modify_write_(0),
        rand_type_helper_(0, 1, 0, 0, 0),
        global_request_distribution_(Zipfian), global_insert_order_(Hashed), random_full_range_(false),
        operation_count_(200), old_record_count_(100), new_record_count_(10), dbsize_(0),
        max_scan_length_(100), scan_length_distribution_(Uniform),
        //db_path_("/home/jiyoung/nvlevelz/test_leveldb/")
        arena_(), value_generator_(), key_generator_(nullptr), valuesize_generator_(nullptr),
        rand_(nullptr),// shuffle_rand_(1),
        oprs_(nullptr)
    {
    }
    ~SagBenchmark() {
        if (oprs_ != nullptr)
            delete[] oprs_;
        if (key_generator_)
            delete key_generator_;
        if (valuesize_generator_)
            delete valuesize_generator_;
        if (rand_)
            delete rand_;
    }

    void Flush() {
        new_record_count_ = operation_count_ * insert_;
        dbsize_ = old_record_count_ + new_record_count_;

        if (key_generator_)
            delete key_generator_;
        key_generator_ = new ZipfianGenerator(old_record_count_);

        if (valuesize_generator_)
            delete valuesize_generator_;
        if (value_length_distribution_ != Distribution::Static)
            valuesize_generator_ = new ZipfianGenerator(value_length_);
        else
            valuesize_generator_ = nullptr;

        if (rand_)
            delete rand_;
        rand_ = new ZipfianGenerator(old_record_count_);

        //if (random_full_range_) {
        //    shuffle_rand_.Reset(new_record_count_);
        //}

        rand_type_helper_.Set(read_, insert_, update_, scan_, read_modify_write_);
        last_insert_ = old_record_count_;
        last_update_ = 0;

    }

    void SetOperationCount(ull operation_count, ull record_count) {
        operation_count_ = operation_count;
        old_record_count_ = record_count;
    }
    void SetKeyDistribution(Distribution dist, InsertOrder order) {
        global_request_distribution_ = dist;
        global_insert_order_ = order;
    }
    void SetTypeDistribution(double read, double insert, double update, double scan, double readmodifywrite) {
        read_ = read;
        insert_ = insert;
        update_ = update;
        scan_ = scan;
        read_modify_write_ = readmodifywrite;
    }
    //void SetRandomFullRange(bool fr) {
    //    random_full_range_ = fr;
    //}
    void SetKeyValueSize(uint32_t key_size, uint32_t value_size, Distribution value_dist) {
        this->key_length_ = key_size;
        this->value_length_ = value_size;
        this->value_length_distribution_ = value_dist;
    }
    void SetPreGenerate(bool pre_generate) {
        pre_generate_ = pre_generate;
    }

    uint32_t Hash(uint32_t key) {
        return MurmurHash3_x86_32(&key, 4);
    }
    ull RandKey(OperationType type) {
        ull key = 0;
        if (type == Insert) {
            if (global_insert_order_ == Hashed) {
                //if (random_full_range_)
                //    return old_record_count_ + shuffle_rand_.Next() % new_record_count_;
                return old_record_count_ + rand_->nextLong() % new_record_count_;
            }
            return last_insert_++;
        }
        //return a random number between [0..record_count_].
        switch (global_request_distribution_) {
        case Uniform:
            if (global_insert_order_ == Hashed) {
                //if (random_full_range_)
                //    return shuffle_rand_.Next();
                return rand_->nextLong() % old_record_count_;
            }
            else
                return last_update_++;
        case Zipfian:
            key = key_generator_->nextZipf(old_record_count_);
            if (global_insert_order_ == Hashed)
                return Hash(key) % old_record_count_;
            else
                return key;
        case Latest:
            if (type == Read)
                return last_update_;
            if (global_insert_order_ == Hashed)
                last_update_ = rand_->nextLong() % old_record_count_;
            else
                last_update_++;
            return last_update_;
        default:
            assert(false);
            return 0;
        }
        assert(false);
        return 0;
    }
    Slice RandValue() {
        size_t value_size = value_length_;
        switch (value_length_distribution_) {
        case Uniform:
            value_size = valuesize_generator_->nextLong() % (value_length_) + 1;
            break;
        case Zipfian:
            value_size = valuesize_generator_->nextZipf(value_length_) + 1;
            break;
        case Static:
            value_size = value_length_;
            break;
        default:
            assert(false);
        }
        return value_generator_.Generate(value_size);
    }
    void Generate() {
        assert(read_ + insert_ + update_ + scan_ + read_modify_write_ == 1);
        //arena_.DisposeAll();
        if (oprs_ != nullptr) {
            delete[] oprs_;
            oprs_ = nullptr;
        }
        if (pre_generate_) {
            oprs_ = static_cast<Operation*>(malloc(sizeof(Operation) * operation_count_));//new Operation[operation_count_ + record_count_];
            //RandTypeHelper rth(read_, insert_, update_, scan_, read_modify_write_);

            for (ull i = 0; i < operation_count_; ++i) {
                oprs_[i].type_ = rand_type_helper_.Check(rand_->nextDouble());
                oprs_[i].key_ = RandKey(oprs_[i].type_);
                oprs_[i].value_ = RandValue();
            }
        }
    }

};
#endif // BENCHMARK_H
