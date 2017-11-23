#ifndef LOAD_TEST_WORK_INTERFACE_H
#define LOAD_TEST_WORK_INTERFACE_H

#include "work_interface.h"
#include "ini_file.h"
#include "woo/log.h"

#include <map>
#include <set>
#include <vector>
#include <fstream>
#include <iostream>
#include <string>
#include <algorithm>
#include <utility>

#include "json.h"
#include <time.h>

#ifdef __GNUC__
#include <ext/hash_map>
#else
#include <hash_map>
#endif // __GNUC__

#include <unordered_map>
#include <unordered_set>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
using namespace rapidjson;

class LoadTestWorkInterface : public WorkInterface {
public:
    LoadTestWorkInterface(DbCompany* &p_db_company, int interface_id):
    WorkInterface(p_db_company, interface_id) {

    }
    ~LoadTestWorkInterface() {

    }
private:
    int work_core(json_object* req_json, char* &p_out_string, int &n_out_len, int64_t req_id);

    string get_rand_num_str(int mod_num);
    int split_strs_add_index(const string &strs, vector<string> &str_vec, int mod_num);
    int video_feat(const set<string> &mids);
    int score(const set<string> &mids);
    int keyword(const string &mid);
    int topic(const string&mid);
};

#endif /* LOAD_TEST_WORK_INTERFACE_H */
