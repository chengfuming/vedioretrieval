#ifndef DISCOVER_WORK_INTERFACE_H
#define DISCOVER_WORK_INTERFACE_H

#include "work_interface.h"
#include "ini_file.h"
#include "woo/log.h"

#include <vector>
#include <string>
#include <unordered_map>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
using namespace rapidjson;

typedef struct MidInfo {
    string mid;
    int is_mcn;
} MidInfo;

typedef pair<string, double> StringDoublePair;

bool compare_by_value(const StringDoublePair &lhs, const StringDoublePair &rhs) {
    return lhs.second > rhs.second;
}


class DiscoverWorkInterface : public WorkInterface {
public:
    DiscoverWorkInterface(DbCompany* &p_db_company, int interface_id):
    WorkInterface(p_db_company, interface_id) {

    }
    ~DiscoverWorkInterface() {

    }

private:
    int return_fail(int ret_code, char* ret_msg, char* category, char* &p_out_string, int &n_out_len);
    int return_json(int ret_code, const char* ret_msg, const string &category, const vector<MidInfo> &midinfo_vec, char* &p_out_string, int &n_out_len);
    int get_mid_score(const vector<string> &mids_vec, unordered_map<string, double> &mid_score_map);
    int get_mids_aux(const string &category, int is_mcn, vector<string> &mids_vec);
    int get_festival_mids_aux(const string &category, int is_weight, vector<string> &mids_vec);

    int get_mcn_mids(const string &category, int num, vector<MidInfo> &midinfo_vec);
    int get_weight_mids(const string &category, int num, vector<MidInfo> &midinfo_vec);
    int get_not_mcn_mids(const string &category, int num, vector<MidInfo> &midinfo_vec);
    int get_no_weight_mids(const string &category, int num, vector<MidInfo> &midinfo_vec);

    int get_festival_mids(const string &category, int num, vector<MidInfo> &midinfo_vec);
    int work_core(json_object* req_json, char* &p_out_string, int &n_out_len, int64_t req_id);
};

#endif /* DISCOVER_WORK_INTERFACE_H */
