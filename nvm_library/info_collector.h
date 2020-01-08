#ifndef INFO_COLLECTOR_H
#define INFO_COLLECTOR_H

#include <vector>
#include <string>
#include <map>

#include "leveldb/slice.h"
#include "global.h"
#include "db/dbformat.h"
#include <unordered_map>
#include <thread>
#include <unistd.h>



namespace leveldb {

struct InfoCollector {
    struct KeyValueInfo {
        enum LocationType {HashLog, nvMemTable, Level0, LevelK, NotFound};
        std::map<std::string, ull> key_info_;
        ull key_total_;

        std::map<size_t, ull> value_info_;
        std::map<LocationType, ull> loc_info_;
        ull value_total_, deletion_total_;

        void SaveKey(const std::string& key) {
            auto p = key_info_.find(key);
            if (p == key_info_.end())
                key_info_[key] = 1;
            else
                p->second ++;
            key_total_ ++;
        }
        void SaveValue(size_t size) {
            auto p = value_info_.find(size);
            if (p == value_info_.end())
                value_info_[size] = 1;
            else
                value_info_[size] ++;
        }
        void GetFrom(LocationType type) {
            ull total = loc_info_[type];
            loc_info_[type] = total + 1;
        }
        void Clear() {
            key_info_.clear();
            value_info_.clear();
            loc_info_.clear();
            key_total_ = 0;
            value_total_ = 0;
            deletion_total_ = 0;
        }
        void Save(std::string *info) {
            /*
            for (auto p = key_info_.begin(); p != key_info_.end(); ++p) {
                const std::string& key = p->first;
                ull count = p->second;
                *info += key + "\t";
            }
            *info += "\n";
            for (auto p = key_info_.begin(); p != key_info_.end(); ++p) {
                const std::string& key = p->first;
                ull count = p->second;
                *info += std::to_string(count) + "\t";
            }*/
            *info += "HashLog : ";
            *info += std::to_string(loc_info_[HashLog]);
            *info += "\n";
            *info += "nvMemTable : ";
            *info += std::to_string(loc_info_[nvMemTable]);
            *info += "\n";
            *info += "Level0 : ";
            *info += std::to_string(loc_info_[Level0]);
            *info += "\n";
            *info += "LevelK : ";
            *info += std::to_string(loc_info_[LevelK]);
            *info += "\n";
        }

        KeyValueInfo() : key_total_(0), value_total_(0), deletion_total_(0) {}
    } kvinfo_;
    void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) {
        kvinfo_.SaveKey(key.ToString());
        if (type == kTypeDeletion)
            kvinfo_.deletion_total_ ++;
        else
            kvinfo_.SaveValue(value.size());
    }
    bool Get(const LookupKey& lkey, std::string* value, Status* s) {
        std::string key = lkey.user_key().ToString();
        kvinfo_.SaveKey(key);
    }
    void AddLocation(KeyValueInfo::LocationType type) {
        kvinfo_.GetFrom(type);
    }

    //-----------------------------------------------------------------------------------
    struct MultiTableInfo {
        typedef std::vector<std::string> Snapshot;
        std::vector<Snapshot*> history_;
        MultiTableInfo() : history_() {}
        ~MultiTableInfo() {
            Clear();
        }
        void SaveSnapshot(Snapshot* ss) {
            history_.push_back(ss);
        }
        void Clear() {
            for (size_t i = 0; i < history_.size(); ++i)
                delete history_[i];
            history_.clear();
        }
        void Save(std::string& info) {
            for (size_t i = 0; i < history_.size(); ++i) {
                Snapshot *s = history_[i];
                for (size_t j = 0; j < s->size(); ++j)
                    info += (*s)[j] + "\t";
                info += "\n";
            }
        }
    } bound_info_;
    void SaveSnapshot(MultiTableInfo::Snapshot* ss) {
        bound_info_.SaveSnapshot(ss);
    }
    //-----------------------------------------------------------------------------------
    struct WarhouseInfo {
        typedef ull* Snapshot;
        static const int MaxWarhouse = 8;
        std::vector<Snapshot> history_;
        std::vector<Snapshot> hit_history_;
        Snapshot current_, current_hit_;
        WarhouseInfo() : history_(), current_(nullptr), current_hit_(nullptr) {}
        ~WarhouseInfo() {
            Clear();
        }
        Snapshot NewSnapshot() {
            Snapshot p = new ull[MaxWarhouse];
            for (int i = 0; i < MaxWarhouse; ++i)
                p[i] = 0;
            return p;
        }
        void Clear() {
            for (size_t i = 0; i < history_.size(); ++i) {
                delete[] history_[i];
                delete[] hit_history_[i];
            }
            history_.clear();
            hit_history_.clear();
            if (current_) delete[] current_;
            if (current_hit_) delete[] current_hit_;

        }
        void Record() {
            if (current_)
                history_.push_back(current_);
            if (current_hit_)
                hit_history_.push_back(current_hit_);
            current_ = NewSnapshot();
            current_hit_ = NewSnapshot();
        }
        void SaveKey(int warhouse) {
            assert(0 <= warhouse && warhouse < MaxWarhouse);
            current_[warhouse]++;
        }
        void SaveHit(int warhouse) {
            assert(0 <= warhouse && warhouse < MaxWarhouse);
            current_hit_[warhouse]++;
        }
    } warhouse_info_;
    void SaveWarhouse(int warhouse) {
        warhouse_info_.SaveKey(warhouse);
    }
    void SaveWarhouseHit(int warhouse) {
        warhouse_info_.SaveHit(warhouse);
    }
    void TriggerWarhouseClear() {
        warhouse_info_.Record();
    }
    //-----------------------------------------------------------------------------------
    struct PopInfo {
        struct PopGCInfo {
            ull size_;
            double garbage_;
            ull lifetime_;
            PopGCInfo(ull size, double garbage, ull lifetime) : size_(size), garbage_(garbage), lifetime_(lifetime) {}
        };
        std::vector<PopGCInfo*> history_;
        ll compaction_time_;
        ll sinior_compaction_;

        void Pop(ull size, double garbage_rate, ull lifetime) {
            PopGCInfo* info = new PopGCInfo(size, garbage_rate, lifetime);
            history_.push_back(info);
        }
        void Clear() {
            for (size_t i = 0; i < history_.size(); ++i)
                delete history_[i];
            history_.clear();
            compaction_time_ = 0;

        }
        void Save(std::string *s) {
            *s += "Compcation Time   : " + std::to_string(1. * compaction_time_ / 1000000000)   + " s\n";
            *s += "Sinior Compaction : " + std::to_string(sinior_compaction_) + "\n";
        }
        PopInfo() : history_(), compaction_time_(0), sinior_compaction_(0) {}
    } garbage_collection_info_;
    void Pop(ull size, double garbage_rate, ull lifetime) {
        garbage_collection_info_.Pop(size, garbage_rate, lifetime);
    }
    void CompactionTime(ll time) {
        garbage_collection_info_.compaction_time_ += time;
    }
    void AddCompaction(int level) {
        if (level == -1)
            garbage_collection_info_.sinior_compaction_ ++;
    }
    //-----------------------------------------------------------------------------------
    void Clear() {
        this->bound_info_.Clear();
        this->kvinfo_.Clear();
        this->garbage_collection_info_.Clear();
        this->warhouse_info_.Clear();
    }
    std::string* Report() {
        return ToString();
    }
    std::string* ToString();
    //-----------------------------------------------------------------------------------

};
/*
struct MultiThreadInfoCollector {
    std::unordered_map<pid_t, InfoCollector*> a;
    MultiThreadInfoCollector() : a() {}
    ~MultiThreadInfoCollector() {
        for (auto p = a.begin(); p != a.end(); ++p) {
            delete p->second;
        }
    }
    pid_t ID() {
        return 0;
        pid_t id = getpid();
        if (a.find(id) == a.end()) a[id] = new InfoCollector;
        return id;
    }
    void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value) {
        return;
        a[ID()]->Add(seq, type, key, value);
    }
    bool Get(const LookupKey& lkey, std::string* value, Status* s) {
        return false;
        a[ID()]->Get(lkey, value, s);
    }
    void SaveSnapshot(InfoCollector::MultiTableInfo::Snapshot* ss) {
        return;
        a[ID()]->SaveSnapshot(ss);
    }
    void SaveWarhouse(int warhouse) {
        return;
        a[ID()]->SaveWarhouse(warhouse);
    }
    void SaveWarhouseHit(int warhouse) {
        return;
        a[ID()]->SaveWarhouseHit(warhouse);
    }
    void TriggerWarhouseClear() {
        return;
        a[ID()]->TriggerWarhouseClear();
    }
    void Pop(ull size, double garbage_rate, ull lifetime) {
        return;
        a[ID()]->Pop(size, garbage_rate, lifetime);
    }

    void Clear() {
        return;
        a[ID()]->Clear();
    }
    InfoCollector* Report() {
        return nullptr;
        a[ID()]->Report();
    }
    void Print(FILE* file) {
        return;
        a[ID()]->Print(file);
    }
};
*/
}

#endif // INFO_COLLECTOR_H
