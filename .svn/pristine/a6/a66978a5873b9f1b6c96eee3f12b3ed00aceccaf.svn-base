#include "discover_work_interface.h"

DYN_WORK(DiscoverWorkInterface);

uint64_t get_now_usec(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint64_t usec = tv.tv_sec*1000000+tv.tv_usec;
    return usec;
}

string print_cost(uint64_t * cost, uint32_t cost_num)
{
    uint64_t total = 0;
    uint32_t i;
    for(i=0; i<cost_num; i++)
    {
        total += cost[i];
    }
    char buf[128] = "";
    string res = "";
    sprintf(buf, "%lu", total);
    res = res + buf + "(";
    sprintf(buf, "%lu", cost[0]);
    res = res + buf;
    for(i=1; i<cost_num; i++)
    {
        sprintf(buf, "%lu", cost[i]);
        res = res + "-" + buf;
    }
    res = res + ")";
    return res;
}

int DiscoverWorkInterface::return_fail(int ret_code, char* ret_msg, char* category, char* &p_out_string, int &n_out_len) {
    n_out_len = sprintf(p_out_string, "{\"ret_code\": %d, \"ret_msg\": \"%s\", \"category\": \"%s\", \"mids\": []}", ret_code, ret_msg, category);
    return 1;
}

int DiscoverWorkInterface::return_json(int ret_code, const char* ret_msg, const string &category, const vector<MidInfo> &midinfo_vec, char* &p_out_string, int &n_out_len) {
    char buf[64];
    int len;

    Document out_doc(kObjectType);
    Document::AllocatorType &allocator = out_doc.GetAllocator();

    Value mid_list_val(kArrayType);
    for (auto &mid_info : midinfo_vec) {
        Value mid_val;
        len = sprintf(buf, "%s", mid_info.mid.c_str());
        mid_val.SetString(buf, len, allocator);

        Value is_mcn_val(mid_info.is_mcn);

        Value mid_info_val(kObjectType);
        mid_info_val.AddMember("mid", mid_val, allocator);
        mid_info_val.AddMember("is_mcn", is_mcn_val, allocator);

        mid_list_val.PushBack(mid_info_val, allocator);
    }

    Value ret_code_val(ret_code);

    Value ret_msg_val;
    len = sprintf(buf, "%s", ret_msg);
    ret_msg_val.SetString(buf, len, allocator);

    Value category_val;
    len = sprintf(buf, "%s", category.c_str());
    category_val.SetString(buf, len, allocator);

    out_doc.AddMember("ret_code", ret_code_val, allocator);
    out_doc.AddMember("ret_msg", ret_msg_val, allocator);
    out_doc.AddMember("category", category_val, allocator);
    out_doc.AddMember("mids", mid_list_val, allocator);

    StringBuffer sb(0, 40960);
    Writer<StringBuffer> writer(sb);
    out_doc.Accept(writer);

    const char* sb_cstr = sb.GetString();
    size_t sb_size = sb.GetSize();

    uLongf uLongf_out = 32000;
    int result = compress((Bytef *)p_out_string, &uLongf_out, (Bytef *)sb_cstr, (uLong)sb_size);
    if(result != Z_OK){
        LOG_ERROR("zlib compress error is %d", result);
        result = -1;
    }else{
        n_out_len = uLongf_out;
        LOG_TRACE("zlib compress success:%d", result);
        result = 1;
    }

    return 1;
}

int DiscoverWorkInterface::get_mid_score(const vector<string> &mids_vec, unordered_map<string, double> &mid_score_map) {
    if (mids_vec.empty()) {
        LOG_ERROR("mids_vec is empty");
        return -1;
    }

    GlobalDbCompany* p_global_db_company = p_db_company_->get_global_db_company();
    if (NULL == p_global_db_company) {
        LOG_ERROR("get gloal db company fail");
        return -1;
    }

    GlobalUnorderedMapDbInterface* p_global_map = (GlobalUnorderedMapDbInterface*)(p_global_db_company->get_global_db_interface("HOT_SCORE"));
    if (NULL == p_global_map) {
        LOG_ERROR("get HOT SCORE fail");
        return -1;
    }

    p_global_map->mget(mids_vec, mid_score_map);

    return mid_score_map.size();
}

int DiscoverWorkInterface::get_mids_aux(const string &category, int is_mcn, vector<string> &mids_vec) {
    GlobalDbCompany* p_global_db_company = p_db_company_->get_global_db_company();
    if (NULL == p_global_db_company) {
        LOG_ERROR("get gloal db company fail");
        return -1;
    }

    GlobalDiscoverDbInterface* p_global_discover = NULL;
    if (is_mcn == 1) {
        p_global_discover = (GlobalDiscoverDbInterface*)(p_global_db_company->get_global_db_interface("MCN_DATA"));
    } else if (is_mcn == 0) {
        p_global_discover = (GlobalDiscoverDbInterface*)(p_global_db_company->get_global_db_interface("NOT_MCN_DATA"));
    }
    if (p_global_discover == NULL) {
        LOG_ERROR("get MCN or NOT MCN fail");
        return -1;
    }

    int ret = p_global_discover->get_mids(category, mids_vec);
    if (ret == -1) {
        LOG_ERROR("category:%s mids not exist", category.c_str());
        return -1;
    }

    return mids_vec.size();
}

int DiscoverWorkInterface::get_festival_mids_aux(const string &category, int is_weight, vector<string> &mids_vec) {
    GlobalDbCompany* p_global_db_company = p_db_company_->get_global_db_company();
    if (NULL == p_global_db_company) {
        LOG_ERROR("get global company fail");
        return -1;
    }

    GlobalDiscoverDbInterface* p_global_discover = NULL;
    if (is_weight == 1) {
        p_global_discover = (GlobalDiscoverDbInterface*)(p_global_db_company->get_global_db_interface("FESTIVAL_WEIGHT"));
    } else if (is_weight == 0) {
        p_global_discover = (GlobalDiscoverDbInterface*)(p_global_db_company->get_global_db_interface("FESTIVAL_NO_WEIGHT"));
    }
    if (p_global_discover == NULL) {
        LOG_ERROR("get festival fail");
        return -1;
    }

    int ret = p_global_discover->get_mids(category, mids_vec);
    if (ret == -1) {
        LOG_ERROR("category:%s mids not exist", category.c_str());
        return -1;
    }

    return mids_vec.size();
}

int DiscoverWorkInterface::get_mcn_mids(const string &category, int num, vector<MidInfo> &midinfo_vec) {
    vector<string> mids_vec;
    int ret = get_mids_aux(category, 1, mids_vec);
    if (ret <= 0) {
        LOG_ERROR("get mcn mids fail");
        return -1;
    }

    unordered_map<string, double> mid_score_map;
    ret = get_mid_score(mids_vec, mid_score_map);
    if (ret <= 0) {
        LOG_ERROR("get mid score fail");
        return -1;
    }

    vector<StringDoublePair> mid_score_vec(mid_score_map.begin(), mid_score_map.end());
    std::sort(mid_score_vec.begin(), mid_score_vec.end());

    int cnt = 0;
    for (auto &mid_score : mid_score_vec) {
        if (cnt >= num) break;
        MidInfo info;
        info.mid = mid_score.first;
        info.is_mcn = 1;
        midinfo_vec.push_back(info);
        ++cnt;
    }

    return midinfo_vec.size();
}

int DiscoverWorkInterface::get_weight_mids(const string &category, int num, vector<MidInfo> &midinfo_vec) {
    vector<string> mids_vec;
    int ret = get_festival_mids_aux(category, 1, mids_vec);
    if (ret <= 0) {
        LOG_ERROR("get weight mids fail");
        return -1;
    }

    int cnt = 0;
    for (auto &mid : mids_vec) {
        if (cnt >= num) break;
        MidInfo info;
        info.mid = mid;
        info.is_mcn = 1;
        midinfo_vec.push_back(info);
        ++cnt;
    }

    return midinfo_vec.size();
}

int myrandom(int i) {
    srand(unsigned(time(NULL)));
    return rand() % i;
}

int DiscoverWorkInterface::get_not_mcn_mids(const string &category, int num, vector<MidInfo> &midinfo_vec) {
    uint64_t cost[8];
    uint64_t f_usec;
    uint64_t n_usec;

    string s_cost;
    n_usec = get_now_usec();

    vector<string> mids_vec;
    int ret = get_mids_aux(category, 0, mids_vec);
    if (ret <= 0) {
        LOG_ERROR("get not mcn mids fail");
        return -1;
    }

    f_usec = n_usec;
    n_usec = get_now_usec();
    cost[0] = n_usec - f_usec;

    // std::random_shuffle(mids_vec.begin(), mids_vec.end(), myrandom);

    f_usec = n_usec;
    n_usec = get_now_usec();
    cost[1] = n_usec - f_usec;

    num = std::min(num, (int)mids_vec.size());
    vector<string> need_mids_vec(mids_vec.begin(), mids_vec.begin() + num);

    unordered_map<string, double> mid_score_map;
    ret = get_mid_score(need_mids_vec, mid_score_map);
    if (ret <= 0) {
        LOG_ERROR("get mid score fail");
        return -1;
    }

    f_usec = n_usec;
    n_usec = get_now_usec();
    cost[2] = n_usec - f_usec;

    vector<StringDoublePair> mid_score_vec(mid_score_map.begin(), mid_score_map.end());
    std::sort(mid_score_vec.begin(), mid_score_vec.end());

    f_usec = n_usec;
    n_usec = get_now_usec();
    cost[3] = n_usec - f_usec;

    s_cost = print_cost(cost, 4);
    LOG_DEBUG("FUCN COST:%s", s_cost.c_str());

    for (auto &mid_score : mid_score_vec) {
        MidInfo info;
        info.mid = mid_score.first;
        info.is_mcn = 0;
        midinfo_vec.push_back(info);
    }

    return midinfo_vec.size();
}

int DiscoverWorkInterface::get_no_weight_mids(const string &category, int num, vector<MidInfo> &midinfo_vec) {
    vector<string> mids_vec;
    int ret = get_festival_mids_aux(category, 0, mids_vec);
    if (ret <= 0) {
        LOG_ERROR("get no weight mids fail");
        return -1;
    }

    num = std::min(num, (int)mids_vec.size());
    vector<string> need_mids_vec(mids_vec.begin(), mids_vec.begin() + num);

    unordered_map<string, double> mid_score_map;
    ret = get_mid_score(need_mids_vec, mid_score_map);
    if (ret <= 0) {
        LOG_ERROR("get mid score fail");
        return -1;
    }
    vector<StringDoublePair> mid_score_vec(mid_score_map.begin(), mid_score_map.end());
    std::sort(mid_score_vec.begin(), mid_score_vec.end());

    for (auto &mid_score : mid_score_vec) {
        MidInfo info;
        info.mid = mid_score.first;
        info.is_mcn = 0;
        midinfo_vec.push_back(info);
    }

    return midinfo_vec.size();
}

int DiscoverWorkInterface::get_festival_mids(const string &category, int num, vector<MidInfo> &midinfo_vec) {
    int ret = get_weight_mids(category, num, midinfo_vec);
    if (ret < 0) {
        LOG_ERROR("get category:%s weight fail", category.c_str());
        ret = 0;
    }

    int no_weight_num = num - ret;
    vector<MidInfo> no_weight_vec;
    ret = get_no_weight_mids(category, no_weight_num, no_weight_vec);
    if (ret <= 0) {
        LOG_ERROR("get category:%s no weight fail", category.c_str());
    } else {
        for (auto &info : no_weight_vec) {
            midinfo_vec.push_back(info);
        }
    }

    return midinfo_vec.size();
}

int DiscoverWorkInterface::work_core(json_object* req_json, char* &p_out_string, int &n_out_len, int64_t req_id) {
    char ret_msg[255];

    const char* json_cstr = json_object_to_json_string(req_json);
    Document input_doc;
    if (input_doc.Parse(json_cstr).HasParseError()) {
        sprintf(ret_msg, "parse json fail");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }

    // category
    string category = input_doc["category"].GetString();
    int num = input_doc["num"].GetInt();
    LOG_DEBUG("category:%s num:%d", category.c_str(), num);

    // 春晚分类数据特殊处理
    if (category == "99") {
        vector<MidInfo> midinfo_vec;
        int ret = get_festival_mids(category, num, midinfo_vec);
        ret = return_json(midinfo_vec.size(), "ok", category, midinfo_vec, p_out_string, n_out_len);
        return ret;
    }

    vector<MidInfo> mcn_vec;
    int ret = get_mcn_mids(category, num, mcn_vec);
    if (ret < 0) {
        LOG_ERROR("get category:%s mcn fail", category.c_str());
        ret = 0;
    }

    int not_mcn_num = num - ret;
    vector<MidInfo> not_mcn_vec;
    ret = get_not_mcn_mids(category, not_mcn_num, not_mcn_vec);
    if (ret <= 0) {
        LOG_ERROR("get category:%s not mcn fail", category.c_str());
    } else {
        for (auto &info : not_mcn_vec) {
            mcn_vec.push_back(info);
        }
    }

    ret = return_json(mcn_vec.size(), "ok", category, mcn_vec, p_out_string, n_out_len);

    return ret;
}
