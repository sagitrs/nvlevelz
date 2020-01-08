#include "info_collector.h"

namespace leveldb {
    std::string* InfoCollector::ToString() {
        std::string* info_ = new std::string();
        this->kvinfo_.Save(info_);
        this->garbage_collection_info_.Save(info_);
        //kvinfo_.Save(info_);

        //fprintf(file, "%s\n", info_->c_str());
        return info_;
        //printf("%s\n", info_.c_str());
    }
}
