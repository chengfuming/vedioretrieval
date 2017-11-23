#include "load_test_work_interface.h"

DYN_WORK(LoadTestWorkInterface);

string LoadTestWorkInterface::get_rand_num_str(int mod_num) {
    int rand_num = rand() % mod_num;
    char rand_num_cstr[8];
    sprintf(rand_num_cstr, "%d", rand_num);
    return string(rand_num_cstr);
}

int LoadTestWorkInterface::split_strs_add_index(const string &strs, vector<string> &str_vec, int mod_num) {
    string rand_num_str = get_rand_num_str(mod_num);
    size_t pos1 = 0;
    size_t pos2 = strs.find(',');
    while (string::npos != pos2) {
        string str = strs.substr(pos1, pos2 - pos1) + "_" + rand_num_str;
        str_vec.push_back(str);
        pos1 = pos2 + 1;
        pos2 = strs.find(',', pos1);
    }

    if (pos1 != strs.length()) {
        string str = strs.substr(pos1) + "_" + rand_num_str;
        str_vec.push_back(str);
    }

    return 1;
}

int LoadTestWorkInterface::video_feat(const set<string> &mids) {
    HiRedisDbInterface* p_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("LOAD_TEST_MID_FEATURE");
    if (p_db_interface == NULL) {
        LOG_ERROR("get LOAD_TEST_MID_FEATURE fail");
        return -1;
    }
    MapStringString values;
    int ret_code = p_db_interface->string_mget(mids, values);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget LOAD_TEST_MID_FEATURE fail");
    }
    return ret_code;
}

int LoadTestWorkInterface::score(const set<string> &mids) {
    HiRedisDbInterface* p_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("LOAD_TEST_MID_SCORE");
    if (p_db_interface == NULL) {
        LOG_ERROR("get LOAD_TEST_MID_SCORE fail");
        return -1;
    }
    MapStringString values;
    int ret_code = p_db_interface->string_mget(mids, values);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget LOAD_TEST_MID_FEATURE fail");
    }
    return ret_code;
}

int LoadTestWorkInterface::keyword(const string&mid) {
    HiRedisDbInterface* p_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("LOAD_TEST_MID_KEYWORDS");
    if (p_db_interface == NULL) {
        LOG_ERROR("get LOAD_TEST_MID_KEYWORDS");
        return -1;
    }
    string keywords_str;
    int ret_code = p_db_interface->string_get(mid, keywords_str);
    if (ret_code <= 0) {
        LOG_ERROR("string_get LOAD_TEST_MID_KEYWORDS fail");
    }
    vector<string> keywords_vec;
    split_strs_add_index(keywords_str, keywords_vec, 5);
    int keyword_num = std::min((int)keywords_vec.size(), 5);
    set<string> keywords_set(keywords_vec.begin(), keywords_vec.begin() + keyword_num);
    MapStringString values;
    ret_code = p_db_interface->string_mget(keywords_set, values);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget LOAD_TEST_MID_KEYWORDS fail");
    }
    return ret_code;
}

int LoadTestWorkInterface::topic(const string&mid) {
    HiRedisDbInterface* p_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("LOAD_TEST_MID_TOPIC");
    if (p_db_interface == NULL) {
        LOG_ERROR("get LOAD_TEST_MID_TOPIC");
        return -1;
    }
    string topics_str;
    int ret_code = p_db_interface->string_get(mid, topics_str);
    if (ret_code <= 0) {
        LOG_ERROR("string_get LOAD_TEST_MID_TOPIC fail");
    }
    vector<string> topics_vec;
    split_strs_add_index(topics_str, topics_vec, 10);
    set<string> topics_set(topics_vec.begin(), topics_vec.end());
    MapStringString values;
    ret_code = p_db_interface->string_mget(topics_set, values);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget LOAD_TEST_MID_TOPIC fail");
    }
    return ret_code;
}

int LoadTestWorkInterface::work_core(json_object* req_json, char* &p_out_string, int &n_out_len, int64_t req_id) {
    const char* json_cstr = json_object_to_json_string(req_json);
    Document input_doc;
    input_doc.Parse(json_cstr);

    string method = input_doc["method"].GetString();
    string mids_str = input_doc["mids"].GetString();
    string mid = input_doc["mid"].GetString();

    set<string> mids_set;
    size_t pos1 = 0;
    size_t pos2 = mids_str.find(',');
    while (string::npos != pos2) {
        string mid = mids_str.substr(pos1, pos2 - pos1);
        mids_set.insert(mid);
        pos1 = pos2 + 1;
        pos2 = mids_str.find(',', pos1);
    }

    if (pos1 != mids_str.length()) {
        string mid = mids_str.substr(pos1);
        mids_set.insert(mid);
    }

    int ret_code = 0;
    if (method == "video_feat") {
        ret_code = video_feat(mids_set);
    } else if (method == "score") {
        ret_code = score(mids_set);
    } else if (method == "keyword") {
        ret_code = keyword(mid);
    } else if (method == "topic") {
        ret_code = topic(mid);
    }

    n_out_len =sprintf(p_out_string, "{\"ret_code\": %d}", ret_code);
    return 0;
}
