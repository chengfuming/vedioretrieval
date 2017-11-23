#ifndef TEST_WORK_INTERFACE_H
#define TEST_WORK_INTERFACE_H

#include "work_interface.h"
#include "ini_file.h"
#include "woo/log.h"

#include <map>
#include <set>
#include <vector>
#include <fstream>
#include <iostream>
#include <string>


#include "json.h"
#include <time.h>

#ifdef __GNUC__
#include <ext/hash_map>
#else
#include <hash_map>
#endif // __GNUC__

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/prettywriter.h"

class TestWorkInterface : public WorkInterface {
public:
    TestWorkInterface(DbCompany* &p_db_company, int interface_id):
    WorkInterface(p_db_company, interface_id) {

    }
    ~TestWorkInterface() {

    }
private:
    int return_fail(char* &p_out_string, int &n_out_len);
    int redis_get(const string &key, string &value);
    int redis_mget(const set<string> &keys, MapStringString &key_value_map);
    int lushan_get(const string &key, string &value);
    int test_rapidjson(const char* input_json_cstr);
    int return_json(const char* req_cmd, int ret_code, const char* ret_msg, const char* req_id, int cand_id, rapidjson::Value &cand_list_val, char* &p_out_string, int &n_out_len);
    int test_openapi(const string &mid, string &value);
    int woo_get(const string &request,string &response);
    int mc_set(const string &key, const string &value);
    int global_data_get();
public:
    int work_core(json_object* req_json, char* &p_out_string, int &n_out_len, int64_t req_id);
};

#endif /* TEST_WORK_INTERFACE_H */
