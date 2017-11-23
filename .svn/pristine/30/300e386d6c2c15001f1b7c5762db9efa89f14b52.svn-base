#ifndef GLOBAL_UNORDERED_MAP_DB_INTERFACE_H
#define GLOBAL_UNORDERED_MAP_DB_INTERFACE_H

#include "global_db_interface.h"
#include <stdlib.h>
#include <unordered_map>
#include <string>
#include <fstream>

using namespace std;

class GlobalUnorderedMapDbInterface : public GlobalDbInterface {
public:
    GlobalUnorderedMapDbInterface(const GlobalDbInfoStruct &global_db_info_struct):
    GlobalDbInterface(global_db_info_struct) {

    }
    ~GlobalUnorderedMapDbInterface() {
        unordered_map_global_data_.clear();
    }

    int mget(const vector<string> keys, unordered_map<string, double> &key_value_map) {
        for (auto &key : keys) {
            double value = 0.0;
            auto found = unordered_map_global_data_.find(key);
            if (found == unordered_map_global_data_.end()) value = 0.0;
            else value = found->second;
            key_value_map[key] = value;
        }
        return 1;
    }

    bool is_exist(uint64_t id) {
        return true;
    }

    int load_db_config() {
        char* db_file_name = global_db_info_struct_.db_file_name_;
        if (NULL == db_file_name) return -1;

        ifstream file(db_file_name);
        string line;
        while (getline(file, line)) {
            size_t split_pos = line.find_first_of(global_db_info_struct_.kv_split_char_);
            string mid = line.substr(0, split_pos);
            string score_str = line.substr(split_pos + 1, string::npos);
            double score = strtod(score_str.c_str(), NULL);
            unordered_map_global_data_[mid] = score;
        }
        return 1;
    }

private:
    unordered_map<string, double> unordered_map_global_data_;
};

#endif /* GLOBAL_UNORDERED_MAP_DB_INTERFACE_H */
