#ifndef GLOBAL_DISCOVER_DB_INTERFACE_H
#define GLOBAL_DISCOVER_DB_INTERFACE_H

#include "global_db_interface.h"
#include <stdlib.h>
#include <unordered_map>
#include <vector>
#include <string>
#include <fstream>
using namespace std;

class GlobalDiscoverDbInterface : public GlobalDbInterface {
public:
    GlobalDiscoverDbInterface(const GlobalDbInfoStruct &global_db_info_struct):
    GlobalDbInterface(global_db_info_struct) {

    }
    ~GlobalDiscoverDbInterface() {
        discover_global_data_.clear();
    }

    bool is_exist(uint64_t id) {
        return true;
    }

    int get_mids(const string &category, vector<string> &mids_vec) {
        auto found = discover_global_data_.find(category);
        if (found == discover_global_data_.end()) return -1;
        mids_vec = found->second;
        return 1;
    }

    int load_db_config() {
        char* db_file_name = global_db_info_struct_.db_file_name_;
        if (NULL == db_file_name) return -1;

        ifstream file(db_file_name);
        string line;
        while (getline(file, line)) {
            size_t first_split_pos = line.find_first_of(global_db_info_struct_.kv_split_char_);
            string category = line.substr(0, first_split_pos);
            string mids = line.substr(first_split_pos + 1, string::npos);

            vector<string> mids_vec;
            size_t pos1 = 0;
            size_t pos2 = mids.find(',');
            while (string::npos != pos2) {
                string mid = mids.substr(pos1, pos2 - pos1);
                mids_vec.push_back(mid);
                pos1 = pos2 + 1;
                pos2 = mids.find(',', pos1);
            }

            if (pos1 != mids.length()) {
                string mid = mids.substr(pos1);
                mids_vec.push_back(mid);
            }

            /* srand(unsigned(time(NULL))); */
            /* random_shuffle(mids_vec.begin(), mids_vec.end()); */

            discover_global_data_[category] = mids_vec;
        }

        return 1;
    }

private:
    unordered_map<string, vector<string> > discover_global_data_;
};

#endif /* GLOBAL_DISCOVER_DB_INTERFACE_H */
