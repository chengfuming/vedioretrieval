#ifndef GLOBAL_VECTOR_DB_INTERFACE_H
#define GLOBAL_VECTOR_DB_INTERFACE_H

#include "global_db_interface.h"
#include <vector>
#include <string>
#include <fstream>

using namespace std;

class GlobalVectorDbInterface : public GlobalDbInterface {
public:
    GlobalVectorDbInterface(const GlobalDbInfoStruct &global_db_info_struct):
    GlobalDbInterface(global_db_info_struct) {

    }
    ~GlobalVectorDbInterface() {
        vector_global_data_.clear();
    }

    int get_values(vector<string> &strs_vec) {
        strs_vec = vector_global_data_;
        return 1;
    }

    bool is_exist(uint64_t id) {
        return true;
    }

    int load_db_config() {
        char* db_file_name = global_db_info_struct_.db_file_name_;
        if (NULL == db_file_name) return -1;

        ifstream file(db_file_name);
        string str;
        while (getline(file, str)) {
            vector_global_data_.push_back(str);
        }
        return 1;
    }

private:
    vector<string> vector_global_data_;
};

#endif /* GLOBAL_VECTOR_DB_INTERFACE_H */
