#include "venus_retrieval_work_interface.h"

DYN_WORK(VenusRetrievalWorkInterface);

int myrandom(int i) {
    return rand() % i;
}

int VenusRetrievalWorkInterface::return_fail(int ret_code, char* ret_msg, const char* req_id, char* &p_out_string, int &n_out_len) {
    n_out_len = sprintf(p_out_string, "{\"ret_code\": %d, \"req_cmd\": \"candidate\", \"ret_msg\": \"%s\", \"req_id\": \"%s\", \"cand_list\": []}", ret_code, ret_msg, req_id);
    return 1;
}

string VenusRetrievalWorkInterface::uint64_to_string(uint64_t value) {
    ostringstream os;
    os << value;
    return os.str();
}

string VenusRetrievalWorkInterface::get_rand_num_str(int mod_num) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    const uint32_t kPrime1 = 61631;
    const uint32_t kPrime2 = 64997;
    const uint32_t kPrime3 = 111857;
    uint32_t seed = kPrime1 * static_cast<uint32_t>(getpid())
        + kPrime2 * static_cast<uint32_t>(tv.tv_sec)
        + kPrime3 * static_cast<uint32_t>(tv.tv_usec);

    srand(seed);
    int rand_num = rand() % mod_num;
    char rand_num_cstr[8];
    sprintf(rand_num_cstr, "%d", rand_num);
    return string(rand_num_cstr);
}

int VenusRetrievalWorkInterface::get_user_feat(const string &uid, string &user_feat) {
    // 先不取用户特征
    // user_feat = "-1:184";
    // return 1;

    DbInterface* p_lushan_db_inteface = p_db_company_->get_db_interface("VENUS_USER_FEAT");
    if (NULL == p_lushan_db_inteface) {
        LOG_ERROR("get lushan db_interface fail");
        return -1;
    }

    char default_user_feat[] = "-1:184";

    uint64_t uid_u64 = strtoull(uid.c_str(), NULL, 10);
    string lushan_key("5-");
    lushan_key += uid;
    char* p_value = NULL;
    char split_char, second_split_char;
    int ret_code = p_lushan_db_inteface->s_get(0, lushan_key.c_str(), p_value, split_char, second_split_char, uid_u64);
    if (ret_code < 0) {
        LOG_ERROR("lushan get fail");
        return -1;
    } else if (ret_code == 0) {
        LOG_WARN("key:%s not exist", lushan_key.c_str());
        p_value = default_user_feat;
    }

    user_feat = p_value;
    return 1;
}

string VenusRetrievalWorkInterface::compute_match_feat(const string &cand_id, double score, const string &score_type) {
    string res("-1:0");
    string part1("-1:24");

    string part2;
    if (score_type == "play_percent") {
        part2 = "0:" + std::to_string(score);
    } else if (score_type == "hot") {
        part2 = "9:" + std::to_string(score);
    } else {
        // 非法分数类型
        return res;
    }

    string part3;
    if (cand_id.compare("keyword_cand") == 0) {
        part3 = "1:1";
    } else if (cand_id.compare("topic_cand") == 0) {
        part3 = "2:1";
    } else if (cand_id.compare("ucf_cand") == 0) {
        part3 = "3:1";
    } else if (cand_id.compare("hot_cand") == 0) {
        part3 = "4:1";
    } else if (cand_id.compare("short_interest_cand") == 0) {
        part3 = "5:1";
    } else if (cand_id.compare("fill_hot") == 0) {
        part3 = "6:1";
    } else if (cand_id.compare("long_interest_cand") == 0) {
        part3 = "7:1";
    } else if (cand_id.compare("spread_interest_cand") == 0) {
        part3 = "8:1";
    } else if (cand_id.compare("one_topic_cand") == 0) {
        part3 = "10:1";
    } else if (cand_id.compare("sharp_topic_cand") == 0) {
        part3 = "11:1";
    } else if (cand_id.compare("people_interest_cand") == 0) {
        part3 = "12:1";
    } else if (cand_id.compare("follow_sim_cand") == 0) {
        part3 = "13:1";
    } else if (cand_id.compare("bigdata_interest_cand") == 0) {
        part3 = "14:1";
    } else if (cand_id.compare("video_analysis_cand") == 0) {
        part3 = "15:1";
    } else if (cand_id.compare("nice_topic_cand_new") == 0) {
        part3 = "16:1";
    } else if (cand_id.compare("nice_topic_cand") == 0) {
        part3 = "17:1";
    } else if (cand_id.compare("old_topic_cand") == 0) {
        part3 = "18:1";
    } else if (cand_id.compare("nice_short_interest_cand") == 0) {
        part3 = "19:1";
    } else if (cand_id.compare("nice_long_interest_cand") == 0) {
        part3 = "20:1";
    } else if (cand_id.compare("gyx_cand") == 0) {
        part3 = "21:1";
    } else if (cand_id.compare("man_tag_cand") == 0) {
        part3 = "22:1";
    } else if (cand_id.compare("sharp2_topic_cand") == 0) {
        part3 = "23:1";
    } else if (cand_id.compare("nice_topic_shift_cand") == 0) {
        part3 = "24:1";
    }else if (cand_id.compare("nice_fine_grained_cand") == 0) {
        part3 = "25:1";
    } else {
        // 非法候选类型
        return res;
    }

    res = part1 + "," + part2 + "," + part3;
    return res;
}

int VenusRetrievalWorkInterface::get_video_feat(MidInfo &mid_info_map, int need_feature) {
    if (mid_info_map.empty()) {
        LOG_ERROR("mid_info_map is empty");
        return 0;
    }

    // 降级方案, 降级时不取视频特征
    if (need_feature == 0) {
        for (auto &mid_info : mid_info_map) {
            mid_info.second.video_feat = "-1:0";
        }
        return mid_info_map.size();
    }

    HiRedisDbInterface* p_mid_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_MID_FEATURE");
    if (NULL == p_mid_db_interface) {
        LOG_ERROR("get mid redis db_interface fail!");
        return -1;
    }

    set<string> mids_set;
    for (auto &mid_info : mid_info_map) {
        mids_set.insert(mid_info.first);
    }

    MapStringString mid_feature_map;
    int ret_code = p_mid_db_interface->string_mget(mids_set, mid_feature_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget mids feature from redis fail!");
        return ret_code;
    }

    string reason = (mid_info_map.begin())->second.reason;
    LOG_DEBUG("%s mids size:%d mid_feature size:%d", reason.c_str(), mids_set.size(), mid_feature_map.size());

    for (auto &mid_feature : mid_feature_map) {
        string mid = mid_feature.first;
        string video_feat = mid_feature.second;
        mid_info_map[mid].video_feat = video_feat;
    }

    // http://stackoverflow.com/questions/263945/what-happens-if-you-call-erase-on-a-map-element-while-iterating-from-begin-to
    int cnt = 0;
    MidInfo::iterator it = mid_info_map.begin();
    while (it != mid_info_map.end()) {
        if (it->second.video_feat.empty()) {
            it = mid_info_map.erase(it);
            // 如果没有视频特征则赋为默认值, 不再删除mid
            // it->second.video_feat = "-1:0";
            ++cnt;
        } else {
            ++it;
        }
    }
    LOG_INFO("reason:%s size:%d del_size:%d", reason.c_str(), mid_info_map.size(), cnt);

    return mid_info_map.size();
}

int VenusRetrievalWorkInterface::mget_mid_gifnum(const set<string> &mids_set, unordered_map<string, double> &mid_gifnum_map) {
    if (mids_set.empty()) {
        LOG_ERROR("mids_set is empty");
        return 0;
    }

    HiRedisDbInterface* p_mid_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_MID_GIFNUM");
    if (NULL == p_mid_db_interface) {
        LOG_ERROR("get mid redis db_interface fail!");
        return -1;
    }

    MapStringString mid_gifnum_strmap;
    int ret_code = p_mid_db_interface->string_mget(mids_set, mid_gifnum_strmap);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget mid gifnum from redis fail");
        return ret_code;
    }

    for (auto &mid_gifnum : mid_gifnum_strmap) {
        string mid = mid_gifnum.first;
        double gifnum = std::stoi(mid_gifnum.second);
        mid_gifnum_map[mid] = gifnum;
    }

    return mid_gifnum_map.size();
}

int VenusRetrievalWorkInterface::return_json(const char* req_cmd, int ret_code, const char* ret_msg, const char* req_id, const string &user_feat, const MidInfo &mid_info_map, char* &p_out_string, int &n_out_len) {
    char buf[5120];
    int len;

    Document out_doc(kObjectType);
    Document::AllocatorType &allocator = out_doc.GetAllocator();

    Value cand_list_val(kArrayType);
    for (auto &mid_info : mid_info_map) {
        const string &mid = mid_info.first;
        const Info &info = mid_info.second;
        const string &video_feat = info.video_feat;
        const string &reason = info.reason;
        const string &match_feat = info.match_feat;

        Value mid_val;
        len = sprintf(buf, "%s", mid.c_str());
        mid_val.SetString(buf, len, allocator);

        Value video_feat_val;
        len = sprintf(buf, "%s", video_feat.c_str());
        video_feat_val.SetString(buf, len, allocator);

        Value reason_val;
        len = sprintf(buf, "%s", reason.c_str());
        reason_val.SetString(buf, len, allocator);

        Value match_feat_val;
        len = sprintf(buf, "%s", match_feat.c_str());
        match_feat_val.SetString(buf, len, allocator);

        Value cand_val(kObjectType);
        cand_val.AddMember("item_id", mid_val, allocator);
        cand_val.AddMember("video_feat", video_feat_val, allocator);
        cand_val.AddMember("reason", reason_val, allocator);
        cand_val.AddMember("match_feat", match_feat_val, allocator);

        cand_list_val.PushBack(cand_val, allocator);
    }

    Value req_cmd_val;
    len = sprintf(buf, "%s", req_cmd);
    req_cmd_val.SetString(buf, len, allocator);

    Value ret_code_val(ret_code);

    Value ret_msg_val;
    len = sprintf(buf, "%s", ret_msg);
    ret_msg_val.SetString(buf, len, allocator);

    Value req_id_val;
    len = sprintf(buf, "%s", req_id);
    req_id_val.SetString(buf, len, allocator);

    Value user_feat_val;
    len = sprintf(buf, "%s", user_feat.c_str());
    user_feat_val.SetString(buf, len, allocator);

    out_doc.AddMember("ret_code", ret_code_val, allocator);
    out_doc.AddMember("req_cmd", req_cmd_val, allocator);
    out_doc.AddMember("ret_msg", ret_msg_val, allocator);
    out_doc.AddMember("req_id", req_id_val, allocator);
    out_doc.AddMember("user_feat", user_feat_val, allocator);
    out_doc.AddMember("cand_list", cand_list_val, allocator);

    StringBuffer sb(0, 40960);
    Writer<StringBuffer> writer(sb);
    out_doc.Accept(writer);

    n_out_len = sprintf(p_out_string, sb.GetString());

    // LOG_ERROR("p_out_string: %s", p_out_string);

    return 1;
}

int VenusRetrievalWorkInterface::return_json(const char* req_cmd, int ret_code, const char* ret_msg, const char* req_id, const string &user_feat, MidInfo &mid_info_map, const vector<string> &cand_id_vec, char* &p_out_string, int &n_out_len) {
    char buf[5120];
    int len;

    Document out_doc(kObjectType);
    Document::AllocatorType &allocator = out_doc.GetAllocator();

    Value cand_list_val(kArrayType);

    Document log_doc(kObjectType);
    Document::AllocatorType &log_allocator = log_doc.GetAllocator();
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            const string &mid = mid_info.first;
            const Info &info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());

        if (this->uid_ == "1007343817") {
            LOG_INFO("vip in");
            random_shuffle(part_mid_score_vec.begin(), part_mid_score_vec.end(), myrandom);
        } else {
            std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);
        }

        Value cand_id_val;
        len = sprintf(buf, "%s", cand_id.c_str());
        cand_id_val.SetString(buf, len, log_allocator);
        log_doc.AddMember(cand_id_val, Value(part_mid_score_vec.size()), log_allocator);

        for (auto &mid_score : part_mid_score_vec) {
            const string &mid = mid_score.first;
            Info info = mid_info_map[mid];

            const string &video_feat = info.video_feat;
            const string &reason = info.reason;
            const string &match_feat = info.match_feat;

            Value mid_val;
            len = sprintf(buf, "%s", mid.c_str());
            mid_val.SetString(buf, len, allocator);

            Value video_feat_val;
            len = sprintf(buf, "%s", video_feat.c_str());
            video_feat_val.SetString(buf, len, allocator);

            Value reason_val;
            len = sprintf(buf, "%s", reason.c_str());
            reason_val.SetString(buf, len, allocator);

            Value match_feat_val;
            len = sprintf(buf, "%s", match_feat.c_str());
            match_feat_val.SetString(buf, len, allocator);

            Value cand_val(kObjectType);
            cand_val.AddMember("item_id", mid_val, allocator);
            cand_val.AddMember("video_feat", video_feat_val, allocator);
            cand_val.AddMember("reason", reason_val, allocator);
            cand_val.AddMember("match_feat", match_feat_val, allocator);

            cand_list_val.PushBack(cand_val, allocator);
        }
    }

    StringBuffer log_sb(0, 1024);
    Writer<StringBuffer> log_writer(log_sb);
    log_doc.Accept(log_writer);
    LOG_INFO("JSON_DATA:%s", log_sb.GetString());

    Value req_cmd_val;
    len = sprintf(buf, "%s", req_cmd);
    req_cmd_val.SetString(buf, len, allocator);

    // Value ret_code_val(ret_code);
    Value ret_code_val(cand_list_val.Size());

    Value ret_msg_val;
    len = sprintf(buf, "%s", ret_msg);
    ret_msg_val.SetString(buf, len, allocator);

    Value req_id_val;
    len = sprintf(buf, "%s", req_id);
    req_id_val.SetString(buf, len, allocator);

    Value user_feat_val;
    len = sprintf(buf, "%s", user_feat.c_str());
    user_feat_val.SetString(buf, len, allocator);

    out_doc.AddMember("ret_code", ret_code_val, allocator);
    out_doc.AddMember("req_cmd", req_cmd_val, allocator);
    out_doc.AddMember("ret_msg", ret_msg_val, allocator);
    out_doc.AddMember("req_id", req_id_val, allocator);
    out_doc.AddMember("user_feat", user_feat_val, allocator);
    out_doc.AddMember("cand_list", cand_list_val, allocator);

    StringBuffer sb(0, 40960);
    Writer<StringBuffer> writer(sb);
    out_doc.Accept(writer);

    n_out_len = sprintf(p_out_string, sb.GetString());

    // LOG_DEBUG("p_out_string: %s", p_out_string);

    return 1;
}

int VenusRetrievalWorkInterface::return_gif_json(const char* req_cmd, int ret_code, const char* ret_msg, const char* req_id, const string &user_feat, MidInfo &mid_info_map, const vector<string> &cand_id_vec, char* &p_out_string, int &n_out_len) {
    char buf[5120];
    int len;

    Document out_doc(kObjectType);
    Document::AllocatorType &allocator = out_doc.GetAllocator();

    Value cand_list_val(kArrayType);

    set<string> mids_set;
    for (auto &mid_info : mid_info_map) {
        mids_set.insert(mid_info.first);
    }

    unordered_map<string, double> mid_gifnum_map;
    int ret = mget_mid_gifnum(mids_set, mid_gifnum_map);
    LOG_DEBUG("mget_mid_gifnum ret:%d", ret);

    Document log_doc(kObjectType);
    Document::AllocatorType &log_allocator = log_doc.GetAllocator();
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            const string &mid = mid_info.first;
            const Info &info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());
        std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);

        vector<StringDoublePair> part_mid_gifnum_vec;
        for (auto &mid_score : part_mid_score_vec) {
            const string &mid = mid_score.first;
            unordered_map<string, double>::const_iterator found = mid_gifnum_map.find(mid);
            if (found == mid_gifnum_map.end()) {
                LOG_WARN("mid:%s gifnum is empty", mid.c_str());
                continue;
            }
            double gifnum = mid_gifnum_map[mid];
            part_mid_gifnum_vec.push_back(pair<string, double>(mid, gifnum));
        }

        std::stable_sort(part_mid_gifnum_vec.begin(), part_mid_gifnum_vec.end(), compare_by_value_asc);

        Value cand_id_val;
        len = sprintf(buf, "%s", cand_id.c_str());
        cand_id_val.SetString(buf, len, log_allocator);
        log_doc.AddMember(cand_id_val, Value(part_mid_gifnum_vec.size()), log_allocator);

        for (auto &mid_gifnum : part_mid_gifnum_vec) {
            const string &mid = mid_gifnum.first;
            Info info = mid_info_map[mid];

            const string &video_feat = info.video_feat;
            const string &reason = info.reason;
            const string &match_feat = info.match_feat;

            Value mid_val;
            len = sprintf(buf, "%s", mid.c_str());
            mid_val.SetString(buf, len, allocator);

            Value video_feat_val;
            len = sprintf(buf, "%s", video_feat.c_str());
            video_feat_val.SetString(buf, len, allocator);

            Value reason_val;
            len = sprintf(buf, "%s", reason.c_str());
            reason_val.SetString(buf, len, allocator);

            Value match_feat_val;
            len = sprintf(buf, "%s", match_feat.c_str());
            match_feat_val.SetString(buf, len, allocator);

            Value cand_val(kObjectType);
            cand_val.AddMember("item_id", mid_val, allocator);
            cand_val.AddMember("video_feat", video_feat_val, allocator);
            cand_val.AddMember("reason", reason_val, allocator);
            cand_val.AddMember("match_feat", match_feat_val, allocator);

            cand_list_val.PushBack(cand_val, allocator);
        }
    }

    StringBuffer log_sb(0, 1024);
    Writer<StringBuffer> log_writer(log_sb);
    log_doc.Accept(log_writer);
    LOG_INFO("JSON_DATA:%s", log_sb.GetString());

    Value req_cmd_val;
    len = sprintf(buf, "%s", req_cmd);
    req_cmd_val.SetString(buf, len, allocator);

    // Value ret_code_val(ret_code);
    Value ret_code_val(cand_list_val.Size());

    Value ret_msg_val;
    len = sprintf(buf, "%s", ret_msg);
    ret_msg_val.SetString(buf, len, allocator);

    Value req_id_val;
    len = sprintf(buf, "%s", req_id);
    req_id_val.SetString(buf, len, allocator);

    Value user_feat_val;
    len = sprintf(buf, "%s", user_feat.c_str());
    user_feat_val.SetString(buf, len, allocator);

    out_doc.AddMember("ret_code", ret_code_val, allocator);
    out_doc.AddMember("req_cmd", req_cmd_val, allocator);
    out_doc.AddMember("ret_msg", ret_msg_val, allocator);
    out_doc.AddMember("req_id", req_id_val, allocator);
    out_doc.AddMember("user_feat", user_feat_val, allocator);
    out_doc.AddMember("cand_list", cand_list_val, allocator);

    StringBuffer sb(0, 40960);
    Writer<StringBuffer> writer(sb);
    out_doc.Accept(writer);

    n_out_len = sprintf(p_out_string, sb.GetString());

    // LOG_DEBUG("p_out_string: %s", p_out_string);

    return 1;
}

int VenusRetrievalWorkInterface::get_need_num_mid_by_score(MidInfo &mid_info_map, int need_num, const vector<string> &cand_id_vec) {
    if (mid_info_map.empty()) {
        LOG_ERROR("mid_info_map is empty");
        return -1;
    }

    HiRedisDbInterface* p_mid_score_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_MID_SCORE");
    if (NULL == p_mid_score_db_interface) {
        LOG_ERROR("get mid score redis db_interface fail!");
        return -1;
    }

    set<string> mids_set;
    for (auto &mid_info : mid_info_map) {
        mids_set.insert(mid_info.first);
    }

    MapStringString mid_compound_score_map;
    int ret_code = p_mid_score_db_interface->string_mget(mids_set, mid_compound_score_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget mids score from redis fail!");
        return ret_code;
    }
    LOG_DEBUG("mids size:%d mid_score size:%d", mids_set.size(), mid_compound_score_map.size());

    double default_score = 0.1;
    time_t cur_time = time(NULL);
    for (auto &mid_info : mid_info_map) {
        string mid = mid_info.first;
        double score = 0.0;
        MapStringString::const_iterator found = mid_compound_score_map.find(mid);
        if (found == mid_compound_score_map.end()) {
            // redis中没有流量利用率, 用默认值
            score = default_score;
        } else {
            string compound_score = found->second;
            size_t split_pos = compound_score.find_first_of('#');
            string score_str = compound_score.substr(0, split_pos);
            double old_score = strtod(score_str.c_str(), NULL);
            string old_time_str = compound_score.substr(split_pos + 1, compound_score.length());
            long old_time = strtol(old_time_str.c_str(), NULL, 10);
            // LOG_DEBUG("old_score:%f old_time:%d cur_time:%d", old_score, old_time, cur_time);

            long time_diff = cur_time - old_time;
            if (time_diff > 3600) {
                // 需重新计算score
                score = old_score * pow(0.9, log(time_diff / 3600));
                // LOG_DEBUG("new score:%lf", score);
            } else {
                // 直接用redis中的值即可
                score = old_score;
            }
        }
        // LOG_DEBUG("mid:%s score:%lf", mid.c_str(), score);
        mid_info_map[mid].score = score;
    }

    MidInfo new_mid_info_map;
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);
        string num_str = cand_id_num.substr(found + 1, cand_id_num.length());
        int num = atoi(num_str.c_str());

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            string mid = mid_info.first;
            Info info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());
        if (this->uid_ == "1007343817") {
            LOG_INFO("vip in");
            random_shuffle(part_mid_score_vec.begin(), part_mid_score_vec.end(), myrandom);
        } else {
            std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);
        }

        int cnt = 0;
        for (auto &mid_score : part_mid_score_vec) {
            if (cnt >= num) break;
            string mid = mid_score.first;
            double score = mid_score.second;

            Info info;
            info.mid = mid;
            info.score = score;
            info.reason = mid_info_map[mid].reason;
            info.match_feat = compute_match_feat(info.reason, info.score, "play_percent");

            new_mid_info_map[mid] = info;
            ++cnt;
        }
    }
    mid_info_map = new_mid_info_map;

    return 1;
}

int VenusRetrievalWorkInterface::get_need_num_mid_by_playduration(MidInfo &mid_info_map, int need_num, const vector<string> &cand_id_vec) {
    if (mid_info_map.empty()) {
        LOG_ERROR("mid_info_map is empty");
        return -1;
    }

    HiRedisDbInterface* p_mid_score_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_MID_DURATION");
    if (NULL == p_mid_score_db_interface) {
        LOG_ERROR("get mid playduration redis db_interface fail!");
        return -1;
    }

    set<string> mids_set;
    for (auto &mid_info : mid_info_map) {
        mids_set.insert(mid_info.first);
    }

    MapStringString mid_compound_score_map;
    int ret_code = p_mid_score_db_interface->string_mget(mids_set, mid_compound_score_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget mids playduration from redis fail!");
        return ret_code;
    }
    LOG_DEBUG("mids size:%d mid_score size:%d", mids_set.size(), mid_compound_score_map.size());

    double default_score = 0.1;
    time_t cur_time = time(NULL);
    for (auto &mid_info : mid_info_map) {
        string mid = mid_info.first;
        double score = 0.0;
        MapStringString::const_iterator found = mid_compound_score_map.find(mid);
        if (found == mid_compound_score_map.end()) {
            // redis中没有播放时间率, 用默认值
            score = default_score;
        } else {
            string compound_score = found->second;
            size_t split_pos = compound_score.find_first_of('#');
            string score_str = compound_score.substr(0, split_pos);
            double old_score = strtod(score_str.c_str(), NULL);
            string old_time_str = compound_score.substr(split_pos + 1, compound_score.length());
            long old_time = strtol(old_time_str.c_str(), NULL, 10);
            // LOG_DEBUG("old_score:%f old_time:%d cur_time:%d", old_score, old_time, cur_time);

            long time_diff = cur_time - old_time;
            if (time_diff > 3600) {
                // 需重新计算score
                score = old_score * pow(0.9, log(time_diff / 3600));
                // LOG_DEBUG("new score:%lf", score);
            } else {
                // 直接用redis中的值即可
                score = old_score;
            }
        }
        // LOG_DEBUG("mid:%s score:%lf", mid.c_str(), score);
        mid_info_map[mid].score = score;
    }

    MidInfo new_mid_info_map;
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);
        string num_str = cand_id_num.substr(found + 1, cand_id_num.length());
        int num = atoi(num_str.c_str());

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            string mid = mid_info.first;
            Info info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());
        if (this->uid_ == "1007343817") {
            LOG_INFO("vip in");
            random_shuffle(part_mid_score_vec.begin(), part_mid_score_vec.end(), myrandom);
        } else {
            std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);
        }

        int cnt = 0;
        for (auto &mid_score : part_mid_score_vec) {
            if (cnt >= num) break;
            string mid = mid_score.first;
            double score = mid_score.second;

            Info info;
            info.mid = mid;
            info.score = score;
            info.reason = mid_info_map[mid].reason;
            info.match_feat = compute_match_feat(info.reason, info.score, "hot");

            new_mid_info_map[mid] = info;
            ++cnt;
        }
    }
    mid_info_map = new_mid_info_map;

    return 1;
}

int VenusRetrievalWorkInterface::get_need_num_mid_by_offline_playduration(MidInfo &mid_info_map, int need_num, const vector<string> &cand_id_vec) {
    if (mid_info_map.empty()) {
        LOG_ERROR("mid_info_map is empty");
        return -1;
    }

    HiRedisDbInterface* p_mid_score_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_MID_OFFLINE_DURATION");
    if (NULL == p_mid_score_db_interface) {
        LOG_ERROR("get mid offline playduration redis db_interface fail!");
        return -1;
    }

    set<string> mids_set;
    for (auto &mid_info : mid_info_map) {
        mids_set.insert(mid_info.first);
    }

    MapStringString mid_compound_score_map;
    int ret_code = p_mid_score_db_interface->string_mget(mids_set, mid_compound_score_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget mids offline playduration from redis fail!");
        return ret_code;
    }
    LOG_DEBUG("mids size:%d mid_score size:%d", mids_set.size(), mid_compound_score_map.size());

    double default_score = 0.11;
    time_t cur_time = time(NULL);
    for (auto &mid_info : mid_info_map) {
        string mid = mid_info.first;
        double score = 0.0;
        MapStringString::const_iterator found = mid_compound_score_map.find(mid);
        if (found == mid_compound_score_map.end()) {
            // redis中没有播放时间率, 用默认值
            score = default_score;
        } else {
            string compound_score = found->second;
            size_t split_pos = compound_score.find_first_of('#');
            string score_str = compound_score.substr(0, split_pos);
            double old_score = strtod(score_str.c_str(), NULL);
            string old_time_str = compound_score.substr(split_pos + 1, compound_score.length());
            long old_time = strtol(old_time_str.c_str(), NULL, 10);
            // LOG_DEBUG("old_score:%f old_time:%d cur_time:%d", old_score, old_time, cur_time);

            long time_diff = cur_time - old_time;
            if (time_diff > 3600) {
                // 需重新计算score
                score = old_score * pow(0.9, log(time_diff / 3600));
                // LOG_DEBUG("new score:%lf", score);
            } else {
                // 直接用redis中的值即可
                score = old_score;
            }
        }
        // LOG_DEBUG("mid:%s score:%lf", mid.c_str(), score);
        mid_info_map[mid].score = score;
    }

    MidInfo new_mid_info_map;
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);
        string num_str = cand_id_num.substr(found + 1, cand_id_num.length());
        int num = atoi(num_str.c_str());

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            string mid = mid_info.first;
            Info info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());
        if (this->uid_ == "1007343817") {
            LOG_INFO("vip in");
            random_shuffle(part_mid_score_vec.begin(), part_mid_score_vec.end(), myrandom);
        } else {
            std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);
        }

        int cnt = 0;
        for (auto &mid_score : part_mid_score_vec) {
            if (cnt >= num) break;
            string mid = mid_score.first;
            double score = mid_score.second;

            Info info;
            info.mid = mid;
            info.score = score;
            info.reason = mid_info_map[mid].reason;
            info.match_feat = compute_match_feat(info.reason, info.score, "hot");

            new_mid_info_map[mid] = info;
            ++cnt;
        }
    }
    mid_info_map = new_mid_info_map;

    return 1;
}

int VenusRetrievalWorkInterface::get_need_num_mid_by_realtime_playduration(MidInfo &mid_info_map, int need_num, const vector<string> &cand_id_vec) {
    if (mid_info_map.empty()) {
        LOG_ERROR("mid_info_map is empty");
        return -1;
    }

    HiRedisDbInterface* p_mid_score_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_MID_REALTIME_DURATION");
    if (NULL == p_mid_score_db_interface) {
        LOG_ERROR("get mid realtime playduration redis db_interface fail!");
        return -1;
    }

    set<string> mids_set;
    for (auto &mid_info : mid_info_map) {
        mids_set.insert(mid_info.first);
    }

    MapStringString mid_compound_score_map;
    int ret_code = p_mid_score_db_interface->string_mget(mids_set, mid_compound_score_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget mids realtime playduration from redis fail!");
        return ret_code;
    }
    LOG_DEBUG("mids size:%d mid_score size:%d", mids_set.size(), mid_compound_score_map.size());

    double default_score = 0.12;
    time_t cur_time = time(NULL);
    for (auto &mid_info : mid_info_map) {
        string mid = mid_info.first;
        double score = 0.0;
        MapStringString::const_iterator found = mid_compound_score_map.find(mid);
        if (found == mid_compound_score_map.end()) {
            // redis中没有播放时间率, 用默认值
            score = default_score;
        } else {
            string compound_score = found->second;
            size_t split_pos = compound_score.find_first_of('#');
            string score_str = compound_score.substr(0, split_pos);
            double old_score = strtod(score_str.c_str(), NULL);
            string old_time_str = compound_score.substr(split_pos + 1, compound_score.length());
            long old_time = strtol(old_time_str.c_str(), NULL, 10);
            // LOG_DEBUG("old_score:%f old_time:%d cur_time:%d", old_score, old_time, cur_time);

            long time_diff = cur_time - old_time;
            if (time_diff > 3600) {
                // 需重新计算score
                score = old_score * pow(0.9, log(time_diff / 3600));
                // LOG_DEBUG("new score:%lf", score);
            } else {
                // 直接用redis中的值即可
                score = old_score;
            }
        }
        // LOG_DEBUG("mid:%s score:%lf", mid.c_str(), score);
        mid_info_map[mid].score = score;
    }

    MidInfo new_mid_info_map;
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);
        string num_str = cand_id_num.substr(found + 1, cand_id_num.length());
        int num = atoi(num_str.c_str());

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            string mid = mid_info.first;
            Info info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());
        if (this->uid_ == "1007343817") {
            LOG_INFO("vip in");
            random_shuffle(part_mid_score_vec.begin(), part_mid_score_vec.end(), myrandom);
        } else {
            std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);
        }

        int cnt = 0;
        for (auto &mid_score : part_mid_score_vec) {
            if (cnt >= num) break;
            string mid = mid_score.first;
            double score = mid_score.second;

            Info info;
            info.mid = mid;
            info.score = score;
            info.reason = mid_info_map[mid].reason;
            info.match_feat = compute_match_feat(info.reason, info.score, "hot");

            new_mid_info_map[mid] = info;
            ++cnt;
        }
    }
    mid_info_map = new_mid_info_map;

    return 1;
}

int VenusRetrievalWorkInterface::get_need_num_mid_by_realtime_playduration_v2(MidInfo &mid_info_map, int need_num, const vector<string> &cand_id_vec) {
    if (mid_info_map.empty()) {
        LOG_ERROR("mid_info_map is empty");
        return -1;
    }

    HiRedisDbInterface* p_mid_score_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_MID_NEW_REALTIME_DURATION");
    if (NULL == p_mid_score_db_interface) {
        LOG_ERROR("get mid realtime playduration v2 redis db_interface fail!");
        return -1;
    }

    set<string> mids_set;
    for (auto &mid_info : mid_info_map) {
        mids_set.insert(mid_info.first);
    }

    MapStringString mid_compound_score_map;
    int ret_code = p_mid_score_db_interface->string_mget(mids_set, mid_compound_score_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget mids realtime v2 playduration from redis fail!");
        return ret_code;
    }
    LOG_DEBUG("mids size:%d mid_score size:%d", mids_set.size(), mid_compound_score_map.size());

    double default_score = 0.01;
    time_t cur_time = time(NULL);
    for (auto &mid_info : mid_info_map) {
        string mid = mid_info.first;
        double score = 0.0;
        MapStringString::const_iterator found = mid_compound_score_map.find(mid);
        if (found == mid_compound_score_map.end()) {
            // redis中没有播放时间率, 用默认值
            score = default_score;
        } else {
            string compound_score = found->second;
            size_t split_pos = compound_score.find_first_of('#');
            string score_str = compound_score.substr(0, split_pos);
            double old_score = strtod(score_str.c_str(), NULL);
            string old_time_str = compound_score.substr(split_pos + 1, compound_score.length());
            long old_time = strtol(old_time_str.c_str(), NULL, 10);
            //LOG_DEBUG("old_score:%f old_time:%d cur_time:%d", old_score, old_time, cur_time);

            long time_diff = cur_time - old_time;
            if (time_diff > 3600) {
                // 需重新计算score
                score = old_score * pow(0.9, log(time_diff / 3600));
                // LOG_DEBUG("new score:%lf", score);
            } else {
                // 直接用redis中的值即可
                score = old_score;
            }
        }
        //LOG_DEBUG("mid:%s score:%lf", mid.c_str(), score);
        mid_info_map[mid].score = score;
    }

    MidInfo new_mid_info_map;
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);
        string num_str = cand_id_num.substr(found + 1, cand_id_num.length());
        int num = atoi(num_str.c_str());

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            string mid = mid_info.first;
            Info info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());
        if (this->uid_ == "1007343817") {
            LOG_INFO("vip in");
            random_shuffle(part_mid_score_vec.begin(), part_mid_score_vec.end(), myrandom);
        } else {
            std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);
        }

        int cnt = 0;
        for (auto &mid_score : part_mid_score_vec) {
            if (cnt >= num) break;
            string mid = mid_score.first;
            double score = mid_score.second;

            Info info;
            info.mid = mid;
            info.score = score;
            info.reason = mid_info_map[mid].reason;
            info.match_feat = compute_match_feat(info.reason, info.score, "hot");

            new_mid_info_map[mid] = info;
            ++cnt;
        }
    }
    mid_info_map = new_mid_info_map;

    return 1;
}

int VenusRetrievalWorkInterface::get_need_num_mid_by_hotscore(MidInfo &mid_info_map, int need_num, const vector<string> &cand_id_vec) {
    if (mid_info_map.empty()) {
        LOG_ERROR("mid_info_map is empty");
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

    vector<string> mids_vec;
    mids_vec.reserve(mid_info_map.size());
    for (auto &mid_info : mid_info_map) {
        mids_vec.push_back(mid_info.first);
    }
    unordered_map<string, double> mid_score_map;
    // 若mid不存在, 则赋默认值为0.0
    p_global_map->mget(mids_vec, mid_score_map);

    for (auto &mid_info : mid_info_map) {
        string mid = mid_info.first;
        mid_info_map[mid].score = mid_score_map[mid];
    }

    MidInfo new_mid_info_map;
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);
        string num_str = cand_id_num.substr(found + 1, cand_id_num.length());
        int num = atoi(num_str.c_str());

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            string mid = mid_info.first;
            Info info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());
        std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);

        int cnt = 0;
        for (auto &mid_score : part_mid_score_vec) {
            if (cnt >= num) break;
            string mid = mid_score.first;
            double score = mid_score.second;

            Info info;
            info.mid = mid;
            info.score = score;
            info.reason = mid_info_map[mid].reason;
            info.match_feat = compute_match_feat(info.reason, info.score, "hot");

            new_mid_info_map[mid] = info;
            ++cnt;
        }
    }
    mid_info_map = new_mid_info_map;

    return 1;
}

int VenusRetrievalWorkInterface::get_need_num_mid_by_gif_hotscore(MidInfo &mid_info_map, int need_num, const vector<string> &cand_id_vec) {
    if (mid_info_map.empty()) {
        LOG_ERROR("mid_info_map is empty");
        return -1;
    }

    GlobalDbCompany* p_global_db_company = p_db_company_->get_global_db_company();
    if (NULL == p_global_db_company) {
        LOG_ERROR("get gloal db company fail");
        return -1;
    }

    GlobalUnorderedMapDbInterface* p_global_map = (GlobalUnorderedMapDbInterface*)(p_global_db_company->get_global_db_interface("GIF_HOT_SCORE"));
    if (NULL == p_global_map) {
        LOG_ERROR("get GIF HOT SCORE fail");
        return -1;
    }

    vector<string> mids_vec;
    mids_vec.reserve(mid_info_map.size());
    for (auto &mid_info : mid_info_map) {
        mids_vec.push_back(mid_info.first);
    }
    unordered_map<string, double> mid_score_map;
    // 若mid不存在, 则赋默认值为0.0
    p_global_map->mget(mids_vec, mid_score_map);

    for (auto &mid_info : mid_info_map) {
        string mid = mid_info.first;
        mid_info_map[mid].score = mid_score_map[mid];
    }

    MidInfo new_mid_info_map;
    for (auto &cand_id_num : cand_id_vec) {
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);
        string num_str = cand_id_num.substr(found + 1, cand_id_num.length());
        int num = atoi(num_str.c_str());

        unordered_map<string, double> part_mid_score_map;
        for (auto &mid_info : mid_info_map) {
            string mid = mid_info.first;
            Info info = mid_info.second;
            if (cand_id.compare(info.reason) == 0) {
                part_mid_score_map[mid] = info.score;
            }
        }
        vector<StringDoublePair> part_mid_score_vec(part_mid_score_map.begin(), part_mid_score_map.end());
        std::sort(part_mid_score_vec.begin(), part_mid_score_vec.end(), compare_by_value);

        int cnt = 0;
        for (auto &mid_score : part_mid_score_vec) {
            if (cnt >= num) break;
            string mid = mid_score.first;
            double score = mid_score.second;

            Info info;
            info.mid = mid;
            info.score = score;
            info.reason = mid_info_map[mid].reason;
            info.match_feat = compute_match_feat(info.reason, info.score, "gif_hot");

            new_mid_info_map[mid] = info;
            ++cnt;
        }
    }
    mid_info_map = new_mid_info_map;

    return 1;
}

int VenusRetrievalWorkInterface::get_mids_sim_hash(const set<string> &mids_set, unordered_map<string, uint64_t> &mid_sim_hash_map) {
    if (mids_set.empty()) {
        LOG_ERROR("mids set is empty");
        return -1;
    }

    HiRedisDbInterface* p_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_SIM_HASH");
    if (NULL == p_db_interface) {
        LOG_ERROR("get VENUS_SIM_HASH fail");
        return -1;
    }

    MapStringString mid_sim_hash_str_map;
    int ret_code = p_db_interface->string_mget(mids_set, mid_sim_hash_str_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget VENUS_SIM_HASH fail");
        return ret_code;
    }

    for (auto &item : mid_sim_hash_str_map) {
        string mid = item.first;
        string sim_hash_str = item.second;
        uint64_t sim_hash = *((uint64_t*)(sim_hash_str.data()));
        mid_sim_hash_map[mid] = sim_hash;
    }

    return 1;
}

int VenusRetrievalWorkInterface::compute_hamming_distance(uint64_t num1, uint64_t num2) {
    uint64_t tmp = (num1 ^ num2) & 0xffffffffffffffff;
    int count = 0;
    while (tmp != 0) {
        tmp &= (tmp - 1);
        ++count;
    }
    return count;
}

int VenusRetrievalWorkInterface::del_sim_mids(const string &source_mid, MidInfo &mid_info_map) {
    if (mid_info_map.empty()) {
        LOG_ERROR("del sim mids input is empty");
        return -1;
    }

    set<string> mids_set;
    for (auto &mid_info : mid_info_map) {
        mids_set.insert(mid_info.first);
    }
    if (!source_mid.empty()) {
        mids_set.insert(source_mid);
    }

    unordered_map<string, uint64_t> mid_sim_hash_map;
    int ret = get_mids_sim_hash(mids_set, mid_sim_hash_map);
    if (ret <= 0) {
        LOG_WARN("get_mids_sim_hash fail");
        return -1;
    }

    auto found_source_mid = mid_sim_hash_map.find(source_mid);
    if (found_source_mid != mid_sim_hash_map.end()) {
        uint64_t source_mid_sim_hash = found_source_mid->second;

        for (auto &mid_sim_hash : mid_sim_hash_map) {
            string mid = mid_sim_hash.first;
            uint64_t sim_hash = mid_sim_hash.second;
            int distance = compute_hamming_distance(source_mid_sim_hash, sim_hash);
            // LOG_DEBUG("distance:%d source mid:%s mid:%s", distance, source_mid.c_str(), mid.c_str());
            if (distance <= 3) {
                LOG_DEBUG("succ del mid source mid:%s %"PRIu64" mid:%s %"PRIu64" ", source_mid.c_str(), source_mid_sim_hash, mid.c_str(), sim_hash);
                mid_info_map.erase(mid);
            }
        }
    }

    // 对两两相似的候选集mid做排重
    // 1. 按热度或流量利用率的分数排序
    // 2. 删除排序在后面的相似mid
    unordered_map<string, double> mid_score_map;
    for (auto &mid_info : mid_info_map) {
        mid_score_map[mid_info.first] = mid_info.second.score;
    }
    vector<StringDoublePair> mid_score_vec(mid_score_map.begin(), mid_score_map.end());
    std::sort(mid_score_vec.begin(), mid_score_vec.end(), compare_by_value);

    int del_cnt = 0;
    int mid_size = mid_score_vec.size();
    for (int i = 0; i < mid_size - 1; ++i) {
        string mid1 = mid_score_vec[i].first;
        uint64_t sim_hash1 = mid_sim_hash_map[mid1];
        for (int j = i + 1; j < mid_size; ++j) {
            string mid2 = mid_score_vec[j].first;
            uint64_t sim_hash2 = mid_sim_hash_map[mid2];

            int distance = compute_hamming_distance(sim_hash1, sim_hash2);
            // LOG_DEBUG("distance:%d mid1:%s mid2:%s", distance, mid1.c_str(), mid2.c_str());
            if (distance <= 3) {
                LOG_DEBUG("succ del mid2 mid1:%s mid2:%s", mid1.c_str(), mid2.c_str());
                mid_info_map.erase(mid2);
                ++del_cnt;
            }
        }
    }
    LOG_DEBUG("del %d sim mids", del_cnt);

    return 1;
}

void* VenusRetrievalWorkInterface::pthread_get_mids(void *args) {
    ThreadData* p_thread_data = (ThreadData*)args;
    string reason = p_thread_data->reason;
    set<string> &mids_set = p_thread_data->mids_set;
    int need_num = p_thread_data->need_num;
    string source_mid = p_thread_data->source_mid;
    string uid = p_thread_data->uid;
    VenusRetrievalWorkInterface* p_instance = p_thread_data->instance;

    int ret_code = 0;
    if (reason.compare("hot_cand") == 0) {
        ret_code = p_instance->hot_cand(need_num * 2, mids_set);
    } else if (reason.compare("keyword_cand") == 0) {
        ret_code = p_instance->keyword_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("ucf_cand") == 0) {
        ret_code = p_instance->ucf_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("topic_cand") == 0) {
        ret_code = p_instance->topic_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("short_interest_cand") == 0) {
        ret_code = p_instance->short_interest_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("bigdata_interest_cand") == 0) {
        ret_code = p_instance->bigdata_interest_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("long_interest_cand") == 0) {
        ret_code = p_instance->long_interest_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("spread_interest_cand") == 0) {
        ret_code = p_instance->spread_interest_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("one_topic_cand") == 0) {
        ret_code = p_instance->one_topic_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("sharp_topic_cand") == 0) {
        ret_code = p_instance->sharp_topic_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("people_interest_cand") == 0) {
        ret_code = p_instance->people_interest_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("follow_sim_cand") == 0) {
        ret_code = p_instance->follow_sim_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("gif_topic_cand") == 0) {
        ret_code = p_instance->gif_topic_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("nice_topic_cand") == 0) {
        ret_code = p_instance->nice_topic_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("video_analysis_cand") == 0) {
        ret_code = p_instance->video_analysis_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("nice_topic_cand_new") == 0) {
        ret_code = p_instance->nice_topic_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("nice_topic_shift_cand") == 0) {
        ret_code = p_instance->nice_topic_shift_cand(source_mid, need_num * 2, mids_set);
    }else if (reason.compare("nice_fine_grained_cand") == 0) {
        ret_code = p_instance->nice_fine_grained_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("old_topic_cand") == 0) {
        ret_code = p_instance->old_topic_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("nice_short_interest_cand") == 0) {
        ret_code = p_instance->nice_short_interest_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("nice_long_interest_cand") == 0) {
        ret_code = p_instance->nice_long_interest_cand(uid, need_num * 2, mids_set);
    } else if (reason.compare("gyx_cand") == 0) {
        ret_code = p_instance->gyx_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("man_tag_cand") == 0) {
        ret_code = p_instance->man_tag_cand(source_mid, need_num * 2, mids_set);
    } else if (reason.compare("sharp2_topic_cand") == 0) {
        ret_code = p_instance->sharp_topic_cand(source_mid, need_num * 2, mids_set);
    } else {
        LOG_ERROR("not support cand type:%s", reason.c_str());
    }
    LOG_DEBUG("pthread_get_mids %s ret_code:%d", reason.c_str(), ret_code);

    pthread_exit(NULL);
}

int VenusRetrievalWorkInterface::multithread_method(vector<ThreadData> &thread_data_vec, const string &source_mid, MidInfo &mid_info_map) {
    int thread_num = thread_data_vec.size();
    pthread_t threads[thread_num];
    for (int i = 0; i < thread_num; i++) {
        int rc = pthread_create(&threads[i], NULL, &VenusRetrievalWorkInterface::pthread_get_mids, &thread_data_vec[i]);
        if (rc) {
            LOG_ERROR("%d thread create fail", i);
            return -1;
        }
    }
    for (int i = 0; i < thread_num; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc) {
            LOG_ERROR("%d thread join fail", i);
            return -1;
        }
    }
    for (int i = 0; i < thread_num; i++) {
        set<string> &mids_set = thread_data_vec[i].mids_set;
        string &reason = thread_data_vec[i].reason;
        for (set<string>::iterator it = mids_set.begin(); it != mids_set.end(); ++it) {
            string mid = *it;
            Info info;
            info.mid = mid;
            info.reason = reason;

            mid_info_map[mid] = info;
        }
    }
    return 1;
}

int VenusRetrievalWorkInterface::split_keywords_add_index(const string &keywords_str, vector<string> &keywords_vec) {
    // keyword1_$index,keyword2_$index,keyword3_$index
    string rand_num_str = get_rand_num_str(5);
    // string rand_num_str = std::to_string(this->re_query_);
    size_t pos1 = 0;
    size_t pos2 = keywords_str.find(',');
    while (string::npos != pos2) {
        string keyword = keywords_str.substr(pos1, pos2 - pos1) + "_" + rand_num_str;
        keywords_vec.push_back(keyword);
        pos1 = pos2 + 1;
        pos2 = keywords_str.find(',', pos1);
    }

    if (pos1 != keywords_str.length()) {
        string keyword = keywords_str.substr(pos1) + "_" + rand_num_str;
        keywords_vec.push_back(keyword);
    }

    return 1;
}

int VenusRetrievalWorkInterface::split_topics_add_index(const string &topics_str, vector<string> &topics_vec) {
    // topic1_$index,topic2_$index,topic3_$index
    string rand_num_str = get_rand_num_str(10);
    // string rand_num_str = std::to_string(this->re_query_);
    size_t pos1 = 0;
    size_t pos2 = topics_str.find(',');
    while (string::npos != pos2) {
        string topic = topics_str.substr(pos1, pos2 - pos1) + "_" + rand_num_str;
        topics_vec.push_back(topic);
        pos1 = pos2 + 1;
        pos2 = topics_str.find(',', pos1);
    }

    if (pos1 != topics_str.length()) {
        string topic = topics_str.substr(pos1) + "_" + rand_num_str;
        topics_vec.push_back(topic);
    }

    return 1;
}

int VenusRetrievalWorkInterface::mget_mids_by_keywords(const set<string> &keywords_set, MapStringString &keyword_mids_map) {
    if (keywords_set.empty()) {
        LOG_ERROR("keywords set is empty");
        return -1;
    }

    HiRedisDbInterface* p_keyword_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_KEYWORD_MIDS");
    if (NULL == p_keyword_mids_db_interface) {
        LOG_ERROR("get keyword mids redis db_interface fail");
        return -1;
    }

    int ret_code = p_keyword_mids_db_interface->string_mget(keywords_set, keyword_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget keyword mids from redis fail");
        return ret_code;
    }

    return 1;
}

int VenusRetrievalWorkInterface::mget_mids_by_topics(const string &db_name, const set<string> &topics_set, MapStringString &topic_mids_map) {
    if (topics_set.empty()) {
        LOG_ERROR("topics set is empty");
        return -1;
    }

    HiRedisDbInterface* p_topic_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface(db_name.c_str());
    if (NULL == p_topic_mids_db_interface) {
        LOG_ERROR("get topic mids redis db_interface fail");
        return -1;
    }

    int ret_code = p_topic_mids_db_interface->string_mget(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget topic mids from redis fail");
        return ret_code;
    }

    return 1;
}

int VenusRetrievalWorkInterface::mget_mids_by_gif_topics(const set<string> &topics_set, MapStringString &topic_mids_map) {
    if (topics_set.empty()) {
        LOG_ERROR("topics set is empty");
        return -1;
    }

    HiRedisDbInterface* p_topic_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_GIF_TOPIC_MIDS");
    if (NULL == p_topic_mids_db_interface) {
        LOG_ERROR("get topic mids redis db_interface fail");
        return -1;
    }

    int ret_code = p_topic_mids_db_interface->string_mget(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget topic mids from redis fail");
        return ret_code;
    }

    return 1;
}

int VenusRetrievalWorkInterface::mget_mids_by_one_topics(const set<string> &topics_set, MapStringString &topic_mids_map) {
    if (topics_set.empty()) {
        LOG_ERROR("topics set is empty");
        return -1;
    }

    HiRedisDbInterface* p_topic_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_ONE_TOPIC_MIDS");
    if (NULL == p_topic_mids_db_interface) {
        LOG_ERROR("get topic mids redis db_interface fail");
        return -1;
    }

    int ret_code = p_topic_mids_db_interface->string_mget(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget topic mids from redis fail");
        return ret_code;
    }

    return 1;
}

int VenusRetrievalWorkInterface::mget_mids_by_sharp_topics(const set<string> &topics_set, MapStringString &topic_mids_map) {
    if (topics_set.empty()) {
        LOG_ERROR("topics set is empty");
        return -1;
    }

    HiRedisDbInterface* p_topic_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_SHARP_TOPIC_MIDS");
    if (NULL == p_topic_mids_db_interface) {
        LOG_ERROR("get topic mids redis db_interface fail");
        return -1;
    }

    int ret_code = p_topic_mids_db_interface->string_mget(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget topic mids from redis fail");
        return ret_code;
    }

    return 1;
}

double VenusRetrievalWorkInterface::compute_keyword_and_score(time_t cur_time, const string &keyword_compound, int decay_time, string &keyword, double* score) {
    size_t keyword_pos1 = keyword_compound.find('#');
    keyword = keyword_compound.substr(0, keyword_pos1);

    size_t keyword_pos2 = keyword_compound.find('#', keyword_pos1 + 1);
    string weight_str = keyword_compound.substr(keyword_pos1 + 1, keyword_pos2 - keyword_pos1 - 1);
    long weight = strtoul(weight_str.c_str(), NULL, 10);

    size_t keyword_pos3 = keyword_compound.find('#', keyword_pos2 + 1);
    string old_time_str = keyword_compound.substr(keyword_pos2 + 1, keyword_pos3 - keyword_pos2 - 1);
    long old_time = strtoul(old_time_str.c_str(), NULL, 10);

    long time_diff = cur_time - old_time;
    double time_decay = pow((time_diff / decay_time + 2), 1.8);

    *score = weight / time_decay;

    long period = time_diff / decay_time;
    LOG_DEBUG("keyword:%s weight:%ld period:%ld score:%f", keyword.c_str(), weight, period, *score);
    return *score;
}

int VenusRetrievalWorkInterface::get_top_keywords(const string &compound_value, int need_all_mid_num, int need_keyword_num, int decay_time, set<string> &keywords_set, unordered_map<string, int> &keyword_need_mid_num_map) {
    time_t  cur_time = time(NULL);
    map<string, double> keyword_score_map;

    size_t pos1 = 0;
    size_t pos2 = compound_value.find(',');
    while (pos2 != string::npos) {
        string keyword_compound = compound_value.substr(pos1, pos2 - pos1);
        string keyword;
        double score;
        compute_keyword_and_score(cur_time, keyword_compound, decay_time, keyword, &score);

        keyword_score_map[keyword] = score;

        pos1 = pos2 + 1;
        pos2 = compound_value.find(',', pos1);
    }

    if (pos1 != compound_value.length()) {
        string keyword_compound = compound_value.substr(pos1);
        string keyword;
        double score;
        compute_keyword_and_score(cur_time, keyword_compound, decay_time, keyword, &score);

        keyword_score_map[keyword] = score;
    }

    vector<StringDoublePair> keyword_score_vec(keyword_score_map.begin(), keyword_score_map.end());
    std::sort(keyword_score_vec.begin(), keyword_score_vec.end(), compare_by_value);

    need_keyword_num = std::min(need_keyword_num, (int)keyword_score_vec.size());
    double top_sum_score = std::accumulate(keyword_score_vec.begin(), keyword_score_vec.begin() + need_keyword_num, 0.0, pair_add<double>());

    int cnt = 0;
    string rand_num_str = get_rand_num_str(10);
    // string rand_num_str = std::to_string(this->re_query_);
    for (vector<StringDoublePair>::const_iterator it = keyword_score_vec.begin(); it != keyword_score_vec.end(); ++it) {
        if (cnt >= need_keyword_num) break;
        string keyword = it->first + "_" + rand_num_str;
        double score = it->second;
        int need_mid_num = ceil((score / top_sum_score) * need_all_mid_num);
        keyword_need_mid_num_map[keyword] = need_mid_num;
        LOG_DEBUG("keyword:%s score:%f need_num:%d", keyword.c_str(), it->second, need_mid_num);
        keywords_set.insert(keyword);
        ++cnt;
    }

    return keywords_set.size();
}

int VenusRetrievalWorkInterface::get_spread_topics(const string &compound_value, int need_all_mid_num, int need_topic_num, set<string> &topics_set, unordered_map<string, int> &topic_need_mid_num_map) {
    string rand_num_str = get_rand_num_str(10);
    // string rand_num_str = std::to_string(this->re_query_);
    unordered_map<string, long> topic_score_map;
    long top_sum_score = 0;

    bool tokenizer_finish = false;
    int cnt = 0;
    size_t pos1 = 0;
    size_t pos2 = compound_value.find(',');
    while (pos2 != string::npos) {
        if (cnt >= need_topic_num) {
            tokenizer_finish = true;
            break;
        }
        string topic_compound = compound_value.substr(pos1, pos2 - pos1);
        size_t topic_pos = topic_compound.find('#');
        string topic = topic_compound.substr(0, topic_pos) + "_" + rand_num_str;
        string weight_str = topic_compound.substr(topic_pos + 1, topic_compound.size() - 1);
        long weight = strtoul(weight_str.c_str(), NULL, 10);

        topics_set.insert(topic);
        topic_score_map[topic] = weight;
        top_sum_score += weight;
        ++cnt;

        pos1 = pos2 + 1;
        pos2 = compound_value.find(',', pos1);
    }
    if (!tokenizer_finish && pos1 != compound_value.length()) {
        string topic_compound = compound_value.substr(pos1);
        size_t topic_pos = topic_compound.find('#');
        string topic = topic_compound.substr(0, topic_pos)  + "_" + rand_num_str;
        string weight_str = topic_compound.substr(topic_pos + 1, topic_compound.size() - 1);
        long weight = strtoul(weight_str.c_str(), NULL, 10);
        topics_set.insert(topic);
        topic_score_map[topic] = weight;
        top_sum_score += weight;
    }

    // LOG_DEBUG("TEST sum:%d", top_sum_score);
    for (auto &topic : topics_set) {
        long score = topic_score_map[topic];
        int need_mid_num = ceil(((double)score / top_sum_score) * need_all_mid_num);
        topic_need_mid_num_map[topic] = need_mid_num;
        LOG_DEBUG("topic:%s score:%d need_num:%d", topic.c_str(), score, need_mid_num);
    }

    return topics_set.size();
}

int VenusRetrievalWorkInterface::get_people_topics(const string &compound_value, set<string> &topics_set) {
    string rand_num_str = get_rand_num_str(10);

    size_t pos1 = 0;
    size_t pos2 = compound_value.find(',');
    while (pos2 != string::npos) {
        string topic = compound_value.substr(pos1, pos2 - pos1) + "_" + rand_num_str;
        topics_set.insert(topic);

        pos1 = pos2 + 1;
        pos2 = compound_value.find(',', pos1);
    }

    return topics_set.size();
}

int VenusRetrievalWorkInterface::hot_cand(int need_num, set<string> &mids_set) {
    GlobalDbCompany* p_global_db_company = p_db_company_->get_global_db_company();
    if (NULL == p_global_db_company) {
        LOG_ERROR("get gloal db company fail");
        return -1;
    }

    GlobalVectorDbInterface* p_global_vector = (GlobalVectorDbInterface*)(p_global_db_company->get_global_db_interface("HOT_VIDEO"));
    if (NULL == p_global_vector) {
        LOG_ERROR("get HOT VIDEO fail");
        return -1;
    }

    vector<string> mids_vec;
    p_global_vector->get_values(mids_vec);

    srand(unsigned(time(NULL)));
    random_shuffle(mids_vec.begin(), mids_vec.end(), myrandom);

    int cnt = 0;
    for (auto &mid : mids_vec) {
        if (cnt >= need_num) break;
        mids_set.insert(mid);
        ++cnt;
    }

    LOG_DEBUG("hot_cand mids size:%d", mids_set.size());

    return 1;
}

int VenusRetrievalWorkInterface::gif_hot_cand(int need_num, set<string> &mids_set) {
    GlobalDbCompany* p_global_db_company = p_db_company_->get_global_db_company();
    if (NULL == p_global_db_company) {
        LOG_ERROR("get gloal db company fail");
        return -1;
    }

    GlobalVectorDbInterface* p_global_vector = (GlobalVectorDbInterface*)(p_global_db_company->get_global_db_interface("GIF_HOT_VIDEO"));
    if (NULL == p_global_vector) {
        LOG_ERROR("get GIF HOT VIDEO fail");
        return -1;
    }

    vector<string> mids_vec;
    p_global_vector->get_values(mids_vec);

    srand(unsigned(time(NULL)));
    random_shuffle(mids_vec.begin(), mids_vec.end(), myrandom);

    int cnt = 0;
    for (auto &mid : mids_vec) {
        if (cnt >= need_num) break;
        mids_set.insert(mid);
        ++cnt;
    }

    LOG_DEBUG("gif_hot_cand mids size:%d", mids_set.size());

    return 1;
}

int VenusRetrievalWorkInterface::keyword_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_keywords_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_KEYWORD_MIDS");
    if (NULL == p_mid_keywords_db_interface) {
        LOG_ERROR("get mid_keywords redis db_interface fail");
        return -1;
    }

    string keywords_str;
    int redis_value_len = p_mid_keywords_db_interface->string_get(source_mid, keywords_str);
    vector<string> keywords_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        LOG_ERROR("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("video_recom_keywords");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);

        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s no value", source_mid.c_str());
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_keywords_add_index(keywords_str, keywords_vec);
    }

    LOG_DEBUG("keywords_vec size:%d", keywords_vec.size());
    // 最多取5个关键词
    int keyword_num = std::min((int)keywords_vec.size(), 5);
    set<string> keywords_set(keywords_vec.begin(), keywords_vec.begin() + keyword_num);
    MapStringString keyword_mids_map;
    int ret_code = mget_mids_by_keywords(keywords_set, keyword_mids_map);
    if (ret_code <= 0) {
        LOG_DEBUG("mget_mids_by_keywords fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = keyword_mids_map.begin(); it != keyword_mids_map.end(); ++it) {
        string keyword((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("keyword_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::topic_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_OLD_TOPIC_MIDS");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        LOG_WARN("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("video_recom_keywords");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);

        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    string db_name("VENUS_OLD_TOPIC_MIDS");
    int ret_code = mget_mids_by_topics(db_name, topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("topic_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::video_analysis_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_VIDEO_ANALYSIS_ORDER");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        LOG_WARN("mid %s not in redis", source_mid.c_str());
        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());

        LOG_INFO("topic id: 0");
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
        LOG_INFO("topic id: %s", topics_str.c_str());
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    string db_name("VENUS_VIDEO_ANALYSIS_INVERSION_ORDER");
    int ret_code = mget_mids_by_topics(db_name, topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("video_analysis mids size:%d", mids_set.size());
    return ret_code;
}


int VenusRetrievalWorkInterface::nice_fine_grained_cand(const string &source_mid,int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_FINE_GRAINED_TOPIC_SOURCE");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        LOG_WARN("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL_FINE");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("video_recom_keywords");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);

        LOG_INFO("topic id: 0");
        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());

        LOG_INFO("topic id: 0");
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
        LOG_INFO("topic id: %s", topics_str.c_str());
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    string db_name("VENUS_FINE_GRAINED_TOPIC");
    int ret_code = mget_mids_by_topics(db_name, topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("nice_fine_grained_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::nice_topic_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_NICE_TOPICS_NEW");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        LOG_WARN("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("video_recom_keywords");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);

        LOG_INFO("topic id: 0");
        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());

        LOG_INFO("topic id: 0");
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
        LOG_INFO("topic id: %s", topics_str.c_str());
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    string db_name("VENUS_TOPIC_MIDS");
    int ret_code = mget_mids_by_topics(db_name, topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("nice_topic_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::nice_topic_shift_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_NICE_TOPIC_SHIFT_SOURCE");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;
    
    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        LOG_WARN("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("video_recom_keywords");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);

        LOG_INFO("topic id: 0");
        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());

        LOG_INFO("topic id: 0");
        return -1;
    } else {
        // 取第一个tid;
        topics_str = topics_str.substr(0, topics_str.find(','));
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
        LOG_INFO("topic id: %s", topics_str.c_str());
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    string db_name("VENUS_NICE_TOPIC_SHIFT");
    int ret_code = mget_mids_by_topics(db_name, topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("nice_topic_shift_cand mids size:%d", mids_set.size());
    return ret_code;
}

int VenusRetrievalWorkInterface::nice_topic_cand_new(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_NICE_TOPICS_NEW");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics_new redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        /*
        LOG_WARN("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("video_recom_keywords");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);
        */

        LOG_INFO("new topic id: 0");
        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());

        LOG_INFO("new topic id: 0");
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
        LOG_INFO("new topic id: %s", topics_str.c_str());
    }

    LOG_DEBUG("new_topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    string db_name("VENUS_TOPIC_MIDS");
    int ret_code = mget_mids_by_topics(db_name, topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_topics_new fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("nice_topic_cand_new mids size:%d", mids_set.size());

    return ret_code;
}


int VenusRetrievalWorkInterface::man_tag_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_MANTAG_TOPICS");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len <= 0) {
        // mid获取失败或不在redis中
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    string db_name("VENUS_MANTAG_TOPICS");
    int ret_code = mget_mids_by_topics(db_name, topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("man_tag_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::gyx_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p__db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_GYX");
    if (NULL == p__db_interface) {
        LOG_ERROR("get gyx redis db_interface fail");
        return -1;
    }

    string rand_num_str = get_rand_num_str(10);
    string redis_key = string("gyx_") + rand_num_str;

    string mids_str;
    int redis_value_len = p__db_interface->string_get(redis_key, mids_str);

    if (redis_value_len <= 0) {
        LOG_WARN("gyx mid %s not value %s", source_mid.c_str(), mids_str.c_str());
        return -1;
    }

    int cnt = 0;
    size_t mids_num = mids_str.size() / 8;
    const char* p_value = mids_str.data();
    uint64_t* mids = (uint64_t*)&(p_value[0]);
    for (size_t i = 0; i < mids_num; i++) {
        if (cnt >= need_num) break;
        string mid_string = uint64_to_string(mids[i]);
        mids_set.insert(mid_string);
        ++cnt;
    }

    LOG_DEBUG("gyx_cand mids size:%d", mids_set.size());

    return 1;
}

int VenusRetrievalWorkInterface::old_topic_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_OLD_TOPIC_MIDS");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        LOG_WARN("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("video_recom_old_keywords");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);

        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    string db_name("VENUS_OLD_TOPIC_MIDS");
    int ret_code = mget_mids_by_topics(db_name, topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("old_topic_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::gif_topic_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_GIF_TOPIC_MIDS");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        LOG_WARN("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("gif_recom_keywords");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);

        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    int ret_code = mget_mids_by_gif_topics(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_gif_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("gif_topic_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::one_topic_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_ONE_TOPIC_MIDS");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    LOG_DEBUG("topics_str:%s", topics_str.c_str());
    vector<string> topics_vec;

    if (redis_value_len < 0) {
        // mid获取失败或不在redis中, 调分词服务
        // 改为在后台异步执行, 往队列传mid后, 用补足mid补候选即可
        LOG_WARN("mid %s not in redis", source_mid.c_str());

        // mc是长连接, 有获取失败的可能性
        McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
        if (NULL == p_mc_db_interface) {
            LOG_ERROR("get mc db_interface fail");
            return -1;
        }

        string key("video_recom_oneword");
        char buff[256];
        snprintf(buff, sizeof(buff), "{\"text\": \"\", \"from\": \"retrieval\", \"mid\": \"%s\", \"source\": \"0\"}", source_mid.c_str());
        string json_value(buff);
        int ret_code = p_mc_db_interface->set(key, json_value);
        LOG_DEBUG("mc set %s %s ret_code: %d", key.c_str(), json_value.c_str(), ret_code);

        return -1;
    } else if (redis_value_len == 0) {
        // mid分词结果为空, 直接返回补候选
        LOG_WARN("mid %s not value", source_mid.c_str());
        return -1;
    } else {
        // mid正常在库存中, 正常分词
        split_topics_add_index(topics_str, topics_vec);
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个topic
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    int ret_code = mget_mids_by_one_topics(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("mget_mids_by_one_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("one_topic_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::ucf_cand(const string &uid, int need_num, set<string> &mids_set) {
    DbInterface* p_lushan_db_inteface = p_db_company_->get_db_interface("VENUS_SIM_USER");
    if (NULL == p_lushan_db_inteface) {
        LOG_ERROR("get lushan db_interface fail");
        return -1;
    }

    uint64_t uid_u64 = strtoull(uid.c_str(), NULL, 10);
    string lushan_key("0-");
    lushan_key += uid;
    char* p_value = NULL;
    char split_char, second_split_char;
    int ret_code = p_lushan_db_inteface->s_get(0, lushan_key.c_str(), p_value, split_char, second_split_char, uid_u64);
    if (ret_code < 0) {
        LOG_ERROR("lushan get fail");
        return -1;
    } else if (ret_code == 0) {
        LOG_WARN("key:%s not exist", lushan_key.c_str());
        return 0;
    }

    set<string> uids_set;
    LOG_DEBUG("sim uids size:%d", ret_code / 12);
    // 最多取5个相似用户
    int uid_num = std::min(ret_code / 12, 5);
    for (int i = 0; i < uid_num; i++) {
        uint64_t sim_uid;
        memcpy(&sim_uid, p_value, sizeof(uint64_t));
        p_value += 8;
        uids_set.insert(uint64_to_string(sim_uid));
        uint32_t sim_score;
        memcpy(&sim_score, p_value, sizeof(uint32_t));
        p_value += 4;
    }

    // 取相似用户看过的视频加入到mid集合中
    HiRedisDbInterface* p_uid_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_UID_MIDS");
    if (NULL == p_uid_mids_db_interface) {
        LOG_ERROR("get uid_mids redis db_interface fail");
        return -1;
    }

    MapStringString uid_mids_map;
    ret_code = p_uid_mids_db_interface->string_hash_mget(uids_set, uid_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_hash_mget uid mids from redis fail");
        return ret_code;
    }

    vector<string> all_mids_vec;
    for (MapStringString::const_iterator it = uid_mids_map.begin(); it != uid_mids_map.end(); ++it) {
        string sim_uid(it->first);
        string mids_str(it->second);
        size_t mids_num = mids_str.size() / 12;
        const char* p_value = mids_str.data();
        for (size_t i = 0; i < mids_num; i++) {
            uint64_t mid;
            memcpy(&mid, p_value, sizeof(uint64_t));
            all_mids_vec.push_back(uint64_to_string(mid));
            p_value += 8;
            uint32_t score;
            memcpy(&score, p_value, sizeof(uint32_t));
            p_value += 4;
            // LOG_DEBUG("sim_uid:%s mid:%"PRIu64" score:%"PRIu32"", sim_uid.c_str(), mid, score);
        }
    }

    if ((int)all_mids_vec.size() <= need_num) {
        for (auto &mid : all_mids_vec) {
            mids_set.insert(mid);
        }
    } else {
        for (int i = 0; i < need_num; i++) {
            int rand_num = rand() % all_mids_vec.size();
            mids_set.insert(all_mids_vec[rand_num]);
        }
    }

    LOG_DEBUG("ucf_cand mids size:%d", mids_set.size());

    return 1;
}

int VenusRetrievalWorkInterface::follow_sim_cand(const string &uid, int need_num, set<string> &mids_set) {
    DbInterface* p_lushan_db_inteface = p_db_company_->get_db_interface("VENUS_FOLLOW_SIM");
    if (NULL == p_lushan_db_inteface) {
        LOG_ERROR("get lushan db_interface fail");
        return -1;
    }

    uint64_t uid_u64 = strtoull(uid.c_str(), NULL, 10);
    string lushan_key("4-");
    lushan_key += uid;
    char* p_value = NULL;
    char split_char, second_split_char;
    int ret_code = p_lushan_db_inteface->s_get(0, lushan_key.c_str(), p_value, split_char, second_split_char, uid_u64);
    if (ret_code < 0) {
        LOG_ERROR("lushan get fail");
        return -1;
    } else if (ret_code == 0) {
        LOG_WARN("key:%s not exist", lushan_key.c_str());
        return 0;
    }

    set<string> uids_set;
    LOG_DEBUG("sim uids size:%d", ret_code / 12);
    // 最多取5个相似用户
    int uid_num = std::min(ret_code / 12, 5);
    for (int i = 0; i < uid_num; i++) {
        uint64_t sim_uid;
        memcpy(&sim_uid, p_value, sizeof(uint64_t));
        p_value += 8;
        uids_set.insert(uint64_to_string(sim_uid));
        uint32_t sim_score;
        memcpy(&sim_score, p_value, sizeof(uint32_t));
        p_value += 4;
    }

    // 取相似用户看过的视频加入到mid集合中
    HiRedisDbInterface* p_uid_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_UID_MIDS");
    if (NULL == p_uid_mids_db_interface) {
        LOG_ERROR("get uid_mids redis db_interface fail");
        return -1;
    }

    MapStringString uid_mids_map;
    ret_code = p_uid_mids_db_interface->string_hash_mget(uids_set, uid_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_hash_mget uid mids from redis fail");
        return ret_code;
    }

    vector<string> all_mids_vec;
    for (MapStringString::const_iterator it = uid_mids_map.begin(); it != uid_mids_map.end(); ++it) {
        string sim_uid(it->first);
        string mids_str(it->second);
        size_t mids_num = mids_str.size() / 12;
        const char* p_value = mids_str.data();
        for (size_t i = 0; i < mids_num; i++) {
            uint64_t mid;
            memcpy(&mid, p_value, sizeof(uint64_t));
            all_mids_vec.push_back(uint64_to_string(mid));
            p_value += 8;
            uint32_t score;
            memcpy(&score, p_value, sizeof(uint32_t));
            p_value += 4;
            // LOG_DEBUG("sim_uid:%s mid:%"PRIu64" score:%"PRIu32"", sim_uid.c_str(), mid, score);
        }
    }

    if ((int)all_mids_vec.size() <= need_num) {
        for (auto &mid : all_mids_vec) {
            mids_set.insert(mid);
        }
    } else {
        for (int i = 0; i < need_num; i++) {
            int rand_num = rand() % all_mids_vec.size();
            mids_set.insert(all_mids_vec[rand_num]);
        }
    }

    LOG_DEBUG("follow_sim_cand mids size:%d", mids_set.size());

    return 1;
}

int VenusRetrievalWorkInterface::short_interest_cand(const string &uid, int need_num, set<string> &mids_set) {
    if (uid.size() == 0) {
        LOG_ERROR("uid is null");
        return -1;
    }

    HiRedisDbInterface* p_short_interest_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_SHORT_INTEREST");
    if (NULL == p_short_interest_db_interface) {
        LOG_ERROR("get short interest redis db_interface fail");
        return -1;
    }

    string compound_value;
    int ret_code = p_short_interest_db_interface->string_get(uid, compound_value);
    if (ret_code <= 0) {
        LOG_ERROR("string_get uid short interest from redis fail ret_code:%d", ret_code);
        return ret_code;
    }

    set<string> keywords_set;
    unordered_map<string, int> keyword_need_mid_num_map;
    int keywords_num = get_top_keywords(compound_value, need_num, 5, 21600, keywords_set, keyword_need_mid_num_map);    // 6小时为一轮衰减周期
    LOG_DEBUG("short interest keywords size:%d", keywords_num);

    HiRedisDbInterface* p_keyword_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_SHORT_KEYWORD_MIDS");
    if (NULL == p_keyword_mids_db_interface) {
        LOG_ERROR("get keyword mids redis db_interface fail");
        return -1;
    }

    MapStringString keyword_mids_map;
    ret_code = p_keyword_mids_db_interface->string_mget(keywords_set, keyword_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget keyword mids from redis fail");
        return ret_code;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = keyword_mids_map.begin(); it != keyword_mids_map.end(); ++it) {
        string keyword((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        int sub_cnt = 0;
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            if (sub_cnt >= keyword_need_mid_num_map[keyword]) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            // LOG_DEBUG("TEST keyword:%s mid:%s", keyword.c_str(), mid_string.c_str());
            ++cnt;
            ++sub_cnt;
        }
    }
    LOG_DEBUG("short_interest_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::bigdata_interest_cand(const string &uid, int need_num, set<string> &mids_set) {
    if (uid.size() == 0) {
        LOG_ERROR("uid is null");
        return -1;
    }

    HiRedisDbInterface* p_bigdata_interest_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_BIGDATA_INTEREST");
    if (NULL == p_bigdata_interest_db_interface) {
        LOG_ERROR("get bigdata interest redis db_interface fail");
        return -1;
    }

    string compound_value;
    int ret_code = p_bigdata_interest_db_interface->string_get(uid, compound_value);
    if (ret_code <= 0) {
        LOG_ERROR("string_get bigdata interest from redis fail ret_code:%d", ret_code);
        return ret_code;
    }

    set<string> keywords_set;
    size_t pos1 = 0;
    size_t pos2 = compound_value.find(',');
    while(pos2 != string::npos)
    {
        string keyword = compound_value.substr(pos1, pos2-pos1);
        string rand_num_str = get_rand_num_str(10);
        keywords_set.insert(keyword+"_"+rand_num_str);
        pos1 = pos2+1;
        pos2 = compound_value.find(',', pos1);
    }

    if (pos1 != compound_value.length()) {
        string keyword = compound_value.substr(pos1);
        string rand_num_str = get_rand_num_str(10);
        keywords_set.insert(keyword+"_"+rand_num_str);
    }
    int keywords_num = keywords_set.size();

    HiRedisDbInterface* p_keyword_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_BIGDATA_TOPIC_MIDS");
    if (NULL == p_keyword_mids_db_interface) {
        LOG_ERROR("get keyword mids redis db_interface fail");
        return -1;
    }

    MapStringString keyword_mids_map;
    ret_code = p_keyword_mids_db_interface->string_mget(keywords_set, keyword_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget keyword mids from redis fail");
        return ret_code;
    }

    set<string> total_mids;
    for (MapStringString::const_iterator it = keyword_mids_map.begin(); it != keyword_mids_map.end(); ++it) {
        string keyword((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        // int sub_cnt = 0;
        for (size_t i = 0; i < mids_num; i++) {
            string mid_string = uint64_to_string(mids[i]);
            total_mids.insert(mid_string);
        }
    }

    vector<string> vec_mids;
    for(set<string>::iterator it = total_mids.begin(); it != total_mids.end(); ++it)
    {
        vec_mids.push_back(*it);
    }
    random_shuffle(vec_mids.begin(), vec_mids.end(), myrandom);

    int cnt = 0;
    for (vector<string>::iterator it = vec_mids.begin(); it != vec_mids.end(); ++it)
    {
        if (cnt >= need_num) break;
        mids_set.insert(*it);
        ++cnt;

    }
    LOG_DEBUG("bigdata_interest_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::nice_short_interest_cand(const string &uid, int need_num, set<string> &mids_set) {
    if (uid.size() == 0) {
        LOG_ERROR("uid is null");
        return -1;
    }

    HiRedisDbInterface* p_short_interest_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_NICE_SHORT_INTEREST");
    if (NULL == p_short_interest_db_interface) {
        LOG_ERROR("get nice short interest redis db_interface fail");
        return -1;
    }

    string compound_value;
    int ret_code = p_short_interest_db_interface->string_get(uid, compound_value);
    if (ret_code <= 0) {
        LOG_ERROR("string_get uid nice short interest from redis fail ret_code:%d", ret_code);
        return ret_code;
    }

    set<string> keywords_set;
    unordered_map<string, int> keyword_need_mid_num_map;
    int keywords_num = get_top_keywords(compound_value, need_num, 5, 21600, keywords_set, keyword_need_mid_num_map);    // 6小时为一轮衰减周期
    LOG_DEBUG("nice short interest keywords size:%d", keywords_num);

    HiRedisDbInterface* p_keyword_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_NICE_SHORT_KEYWORD_MIDS");
    if (NULL == p_keyword_mids_db_interface) {
        LOG_ERROR("get keyword mids redis db_interface fail");
        return -1;
    }

    MapStringString keyword_mids_map;
    ret_code = p_keyword_mids_db_interface->string_mget(keywords_set, keyword_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget keyword mids from redis fail");
        return ret_code;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = keyword_mids_map.begin(); it != keyword_mids_map.end(); ++it) {
        string keyword((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        int sub_cnt = 0;
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            if (sub_cnt >= keyword_need_mid_num_map[keyword]) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            // LOG_DEBUG("TEST keyword:%s mid:%s", keyword.c_str(), mid_string.c_str());
            ++cnt;
            ++sub_cnt;
        }
    }
    LOG_DEBUG("nice_short_interest_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::long_interest_cand(const string &uid, int need_num, set<string> &mids_set) {
    if (uid.size() == 0) {
        LOG_ERROR("uid is null");
        return -1;
    }

    HiRedisDbInterface* p_long_interest_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_LONG_INTEREST");
    if (NULL == p_long_interest_db_interface) {
        LOG_ERROR("get long interest redis db_interface fail");
        return -1;
    }

    string compound_value;
    int ret_code = p_long_interest_db_interface->string_get(uid, compound_value);
    if (ret_code <= 0) {
        LOG_ERROR("string_get uid long interest from redis fail ret_code:%d", ret_code);
        return ret_code;
    }

    set<string> keywords_set;
    unordered_map<string, int> keyword_need_mid_num_map;
    int keywords_num = get_top_keywords(compound_value, need_num, 5, 86400, keywords_set, keyword_need_mid_num_map);    // 一天为一轮衰减周期
    LOG_DEBUG("long interest keywords size:%d", keywords_num);

    HiRedisDbInterface* p_keyword_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_TOPIC_MIDS");
    if (NULL == p_keyword_mids_db_interface) {
        LOG_ERROR("get keyword mids redis db_interface fail");
        return -1;
    }

    MapStringString keyword_mids_map;
    ret_code = p_keyword_mids_db_interface->string_mget(keywords_set, keyword_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget keyword mids from redis fail");
        return ret_code;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = keyword_mids_map.begin(); it != keyword_mids_map.end(); ++it) {
        string keyword((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        int sub_cnt = 0;
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            if (sub_cnt >= keyword_need_mid_num_map[keyword]) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            // LOG_DEBUG("TEST keyword:%s mid:%s", keyword.c_str(), mid_string.c_str());
            ++cnt;
            ++sub_cnt;
        }
    }
    LOG_DEBUG("long_interest_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::nice_long_interest_cand(const string &uid, int need_num, set<string> &mids_set) {
    if (uid.size() == 0) {
        LOG_ERROR("uid is null");
        return -1;
    }

    HiRedisDbInterface* p_long_interest_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_NICE_LONG_INTEREST");
    if (NULL == p_long_interest_db_interface) {
        LOG_ERROR("get nice long interest redis db_interface fail");
        return -1;
    }

    string compound_value;
    int ret_code = p_long_interest_db_interface->string_get(uid, compound_value);
    if (ret_code <= 0) {
        LOG_ERROR("string_get uid nice long interest from redis fail ret_code:%d", ret_code);
        return ret_code;
    }

    set<string> keywords_set;
    unordered_map<string, int> keyword_need_mid_num_map;
    int keywords_num = get_top_keywords(compound_value, need_num, 5, 86400, keywords_set, keyword_need_mid_num_map);    // 一天为一轮衰减周期
    LOG_DEBUG("nice long interest keywords size:%d", keywords_num);

    HiRedisDbInterface* p_keyword_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_NICE_LONG_KEYWORD_MIDS");

    if (NULL == p_keyword_mids_db_interface) {
        LOG_ERROR("get keyword mids redis db_interface fail");
        return -1;
    }

    MapStringString keyword_mids_map;
    ret_code = p_keyword_mids_db_interface->string_mget(keywords_set, keyword_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget keyword mids from redis fail");
        return ret_code;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = keyword_mids_map.begin(); it != keyword_mids_map.end(); ++it) {
        string keyword((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        int sub_cnt = 0;
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            if (sub_cnt >= keyword_need_mid_num_map[keyword]) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            // LOG_DEBUG("TEST keyword:%s mid:%s", keyword.c_str(), mid_string.c_str());
            ++cnt;
            ++sub_cnt;
        }
    }
    LOG_DEBUG("nice_long_interest_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::spread_interest_cand(const string &uid, int need_num, set<string> &mids_set) {
    if (uid.size() == 0) {
        LOG_ERROR("uid is null");
        return -1;
    }

    DbInterface* p_lushan_db_inteface = p_db_company_->get_db_interface("VENUS_SPREAD_INTEREST");
    if (NULL == p_lushan_db_inteface) {
        LOG_ERROR("get lushan db_interface fail");
        return -1;
    }

    uint64_t uid_u64 = strtoull(uid.c_str(), NULL, 10);
    string lushan_key("2-");
    lushan_key += uid;
    char* p_value = NULL;
    char split_char, second_split_char;
    int ret_code = p_lushan_db_inteface->s_get(0, lushan_key.c_str(), p_value, split_char, second_split_char, uid_u64);
    if (ret_code < 0) {
        LOG_ERROR("lushan get fail");
        return -1;
    } else if (ret_code == 0) {
        LOG_WARN("key:%s not exist", lushan_key.c_str());
        return 0;
    }

    string compound_value(p_value);
    set<string> topics_set;
    unordered_map<string, int> topic_need_mid_num_map;
    int topics_num = get_spread_topics(compound_value, need_num, 5, topics_set, topic_need_mid_num_map);    // 最多取5个topic
    LOG_DEBUG("spread interest topics size:%d", topics_num);

    HiRedisDbInterface* p_topic_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_SPREAD_TOPIC_MIDS");
    if (NULL == p_topic_mids_db_interface) {
        LOG_ERROR("get topic mids redis db_interface fail");
        return -1;
    }

    MapStringString topic_mids_map;
    ret_code = p_topic_mids_db_interface->string_mget(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget topic mids from redis fail");
        return ret_code;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        int sub_cnt = 0;
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            if (sub_cnt >= topic_need_mid_num_map[topic]) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            // LOG_DEBUG("TEST topic:%s mid:%s", topic.c_str(), mid_string.c_str());
            ++cnt;
            ++sub_cnt;
        }
    }
    LOG_DEBUG("spread_interest_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::people_interest_cand(const string &uid, int need_num, set<string> &mids_set) {
    if (uid.size() == 0) {
        LOG_ERROR("uid is null");
        return -1;
    }

    DbInterface* p_lushan_db_inteface = p_db_company_->get_db_interface("VENUS_PEOPLE_INTEREST");
    if (NULL == p_lushan_db_inteface) {
        LOG_ERROR("get lushan db_interface fail");
        return -1;
    }

    uint64_t uid_u64 = strtoull(uid.c_str(), NULL, 10);
    string lushan_key("3-");
    lushan_key += uid;
    char* p_value = NULL;
    char split_char, second_split_char;
    int ret_code = p_lushan_db_inteface->s_get(0, lushan_key.c_str(), p_value, split_char, second_split_char, uid_u64);
    if (ret_code < 0) {
        LOG_ERROR("lushan get fail");
        return -1;
    } else if (ret_code == 0) {
        LOG_WARN("key:%s not exist", lushan_key.c_str());
        return 0;
    }

    string compound_value(p_value);
    set<string> topics_set;
    int topics_num = get_people_topics(compound_value, topics_set);
    LOG_DEBUG("people interest topics size:%d", topics_num);
    if (topics_num <= 0) {
        LOG_WARN("get_people_topics fail");
        return -1;
    }

    HiRedisDbInterface* p_topic_mids_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_PEOPLE_TOPIC_MIDS");
    if (NULL == p_topic_mids_db_interface) {
        LOG_ERROR("get topic mids redis db_interface fail");
        return -1;
    }

    MapStringString topic_mids_map;
    ret_code = p_topic_mids_db_interface->string_mget(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_ERROR("string_mget topic mids from redis fail");
        return ret_code;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        int topic_need_num = std::min(need_num / topics_num, (int)mids_num);

        for (int i = 0; i < topic_need_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            LOG_DEBUG("TEST topic:%s mid:%s", topic.c_str(), mid_string.c_str());
            ++cnt;
        }
    }
    LOG_DEBUG("people_interest_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::sharp_topic_cand(const string &source_mid, int need_num, set<string> &mids_set) {
    if (source_mid.size() == 0) {
        LOG_ERROR("source_mid is null");
        return -1;
    }

    HiRedisDbInterface* p_mid_topics_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("VENUS_SHARP_TOPIC_MIDS");
    if (NULL == p_mid_topics_db_interface) {
        LOG_ERROR("get mid_topics redis db_interface fail");
        return -1;
    }

    string topics_str;
    int redis_value_len = p_mid_topics_db_interface->string_get(source_mid, topics_str);
    vector<string> topics_vec;

    if (redis_value_len <= 0) {
        return -1;
    } else {
        split_topics_add_index(topics_str, topics_vec);
    }

    LOG_DEBUG("topics_vec size:%d", topics_vec.size());
    // 最多取5个话题
    int topic_num = std::min((int)topics_vec.size(), 5);
    set<string> topics_set(topics_vec.begin(), topics_vec.begin() + topic_num);
    MapStringString topic_mids_map;
    int ret_code = mget_mids_by_sharp_topics(topics_set, topic_mids_map);
    if (ret_code <= 0) {
        LOG_DEBUG("mget_mids_by_sharp_topics fail");
        return -1;
    }

    int cnt = 0;
    for (MapStringString::const_iterator it = topic_mids_map.begin(); it != topic_mids_map.end(); ++it) {
        string topic((*it).first);
        string mids_str((*it).second);
        size_t mids_num = mids_str.size() / 8;
        const char* p_value = mids_str.data();
        uint64_t* mids = (uint64_t*)&(p_value[0]);
        for (size_t i = 0; i < mids_num; i++) {
            if (cnt >= need_num) break;
            string mid_string = uint64_to_string(mids[i]);
            mids_set.insert(mid_string);
            ++cnt;
        }
    }
    LOG_DEBUG("sharp_topic_cand mids size:%d", mids_set.size());

    return ret_code;
}

int VenusRetrievalWorkInterface::work_core(json_object* req_json, char* &p_out_string, int &n_out_len, int64_t req_id) {
    srand((unsigned)time(NULL));

    char ret_msg[255];

    const char* json_cstr = json_object_to_json_string(req_json);
    Document input_doc;
    if (input_doc.Parse(json_cstr).HasParseError()) {
        sprintf(ret_msg, "parse json fail");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    if (!input_doc.IsObject()) {
        sprintf(ret_msg, "json is not object");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    if (!input_doc.HasMember("body")) {
        sprintf(ret_msg, "json body is empty");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }

    // req_id
    Value::ConstMemberIterator itr = input_doc["body"].FindMember("req_id");
    if (itr == input_doc["body"].MemberEnd()) {
        sprintf(ret_msg, "json req_id is empty");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    const char* req_id_str = itr->value.GetString();

    // cand_id
    itr = input_doc["body"].FindMember("cand_id");
    if (itr == input_doc["body"].MemberEnd()) {
        sprintf(ret_msg, "json cand_id is empty");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    const Value &cand_id_val = itr->value;
    if (!cand_id_val.IsArray()) {
        sprintf(ret_msg, "json cand_id is not array");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    vector<string> cand_id_vec;
    for (Value::ConstValueIterator it = cand_id_val.Begin(); it != cand_id_val.End(); ++it) {
        cand_id_vec.push_back(it->GetString());
    }

    // re_query
    itr = input_doc["body"].FindMember("re_query");
    if (itr == input_doc["body"].MemberEnd()) {
        sprintf(ret_msg, "json re_query is empty");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    int re_query = itr->value.GetInt();
    this->re_query_ = re_query;

    // obj_id
    itr = input_doc["body"].FindMember("obj_id");
    if (itr == input_doc["body"].MemberEnd()) {
        sprintf(ret_msg, "json obj_id is empty");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    string obj_id_str = itr->value.GetString();
    this->uid_ = obj_id_str;

    // tobj_id
    itr = input_doc["body"].FindMember("tobj_id");
    if (itr == input_doc["body"].MemberEnd()) {
        sprintf(ret_msg, "json tobj_id is empty");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    string tobj_id_str = itr->value.GetString();

    // num
    itr = input_doc["body"].FindMember("num");
    if (itr == input_doc["body"].MemberEnd()) {
        sprintf(ret_msg, "json num is empty");
        return return_fail(-1, ret_msg, "", p_out_string, n_out_len);
    }
    int num = itr->value.GetInt();
    if (num <= 0 || num > 100) {
        sprintf(ret_msg, "input json num is invalid. input:%d", num);
        return return_fail(-1, ret_msg, req_id_str, p_out_string, n_out_len);
    }

    // score_type
    string score_type;
    itr = input_doc["body"].FindMember("score_type");
    if (itr == input_doc["body"].MemberEnd()) {
        // 若没有该字段, 默认取流量利用率
        score_type = "play_percent";
    } else {
        score_type = itr->value.GetString();
    }

    // fill_method
    string fill_method;
    itr = input_doc["body"].FindMember("fill_method");
    if (itr == input_doc["body"].MemberEnd()) {
        fill_method = "fill_hot";
    } else {
        fill_method = itr->value.GetString();
    }

    // need_feature
    int need_feature = 0;
    itr = input_doc["body"].FindMember("need_feature");
    if (itr == input_doc["body"].MemberEnd()) {
        need_feature = 0;
    } else {
        need_feature = itr->value.GetInt();
    }

    int ret_code = 0;

    string user_feat;
    ret_code = get_user_feat(obj_id_str, user_feat);
    if (ret_code <= 0) {
        sprintf(ret_msg, "get user feat fail");
        LOG_ERROR("%s", ret_msg);
        return return_fail(ret_code, ret_msg, req_id_str, p_out_string, n_out_len);
    }

    vector<ThreadData> thread_data_vec;
    thread_data_vec.reserve(cand_id_vec.size());
    for (vector<string>::iterator it = cand_id_vec.begin(); it != cand_id_vec.end(); ++it) {
        string cand_id_num = *it;
        size_t found = cand_id_num.find_first_of('#');
        string cand_id = cand_id_num.substr(0, found);
        string need_num_str = cand_id_num.substr(found + 1, cand_id_num.length());
        int need_num = atoi(need_num_str.c_str());

        ThreadData thread_data;
        thread_data.reason = cand_id;
        thread_data.need_num = need_num;
        thread_data.source_mid = tobj_id_str;
        thread_data.uid = obj_id_str;
        thread_data.instance = this;

        thread_data_vec.push_back(thread_data);
    }

    MidInfo mid_info_map;
    ret_code = multithread_method(thread_data_vec, tobj_id_str, mid_info_map);
    if (ret_code <= 0) {
        sprintf(ret_msg, "multithread_method fail");
        LOG_ERROR("%s", ret_msg);
        return return_fail(ret_code, ret_msg, req_id_str, p_out_string, n_out_len);
    }

    if (score_type == "play_percent") {
        ret_code = get_need_num_mid_by_score(mid_info_map, num, cand_id_vec);
    }else if (score_type == "play_duration") {
        ret_code = get_need_num_mid_by_playduration(mid_info_map, num, cand_id_vec);
    }else if (score_type == "play_realtime_duration") {
        ret_code = get_need_num_mid_by_realtime_playduration(mid_info_map, num, cand_id_vec);
    }else if (score_type == "play_realtime_duration_v2") {
        ret_code = get_need_num_mid_by_realtime_playduration_v2(mid_info_map, num, cand_id_vec);
    }else if (score_type == "play_offline_duration") {
        ret_code = get_need_num_mid_by_offline_playduration(mid_info_map, num, cand_id_vec);
    } else if (score_type == "hot") {
        ret_code = get_need_num_mid_by_hotscore(mid_info_map, num, cand_id_vec);
    } else if (score_type == "gif_hot") {
        ret_code = get_need_num_mid_by_gif_hotscore(mid_info_map, num, cand_id_vec);
    } else {
        sprintf(ret_msg, "unsupport score type:%s", score_type.c_str());
        LOG_ERROR("%s", ret_msg);
        return return_fail(ret_code, ret_msg, req_id_str, p_out_string, n_out_len);
    }

    int cand_num = get_video_feat(mid_info_map, need_feature);

    // 补足
    if (cand_num < num) {
        int need_num = num - cand_num;

        set<string> fill_mids_set;
        if (fill_method == "fill_hot") {
            ret_code = hot_cand(num, fill_mids_set);
        } else if (fill_method == "gif_fill_hot") {
            ret_code = gif_hot_cand(num, fill_mids_set);
        } else if (fill_method == "fill_topic") {
            ret_code = nice_topic_cand(tobj_id_str, num, fill_mids_set);
        }
        if (ret_code <= 0) {
            sprintf(ret_msg, "fill fail");
            LOG_ERROR("%s", ret_msg);
            return return_fail(ret_code, ret_msg, req_id_str, p_out_string, n_out_len);
        }

        set<string> cand_mids_set;
        for (auto &mid_info : mid_info_map) {
            cand_mids_set.insert(mid_info.first);
        }

        set<string> diff_mids_set;
        std::set_difference(fill_mids_set.begin(), fill_mids_set.end(), cand_mids_set.begin(), cand_mids_set.end(), std::inserter(diff_mids_set, diff_mids_set.end()));
        LOG_DEBUG("diff_mids_set size:%d", diff_mids_set.size());

        set<string> need_mids_set;
        int cnt = 0;
        for (auto &mid : diff_mids_set) {
            if (cnt >= need_num) break;
            need_mids_set.insert(mid);
            ++cnt;
        }
        LOG_DEBUG("need_mids_set size:%d need_num:%d", need_mids_set.size(), need_num);

        MidInfo fill_mid_info_map;
        for (auto &mid : need_mids_set) {
            Info info;
            info.mid = mid;
            info.reason = fill_method;
            fill_mid_info_map[mid] = info;
        }

        char buf[64];
        snprintf(buf, sizeof(buf), "%s#%d", fill_method.c_str(), need_num);
        vector<string> fill_cand_id_vec;
        fill_cand_id_vec.push_back(buf);
        cand_id_vec.push_back(buf);

        if (score_type == "play_percent") {
            get_need_num_mid_by_score(fill_mid_info_map, need_num, fill_cand_id_vec);
        }else if (score_type == "play_duration") {
            get_need_num_mid_by_playduration(fill_mid_info_map, need_num, fill_cand_id_vec);
        }else if (score_type == "play_realtime_duration") {
            get_need_num_mid_by_realtime_playduration(fill_mid_info_map, need_num, fill_cand_id_vec);
        }else if (score_type == "play_realtime_duration_v2") {
            get_need_num_mid_by_realtime_playduration_v2(fill_mid_info_map, need_num, fill_cand_id_vec);
        }else if (score_type == "play_offline_duration") {
            get_need_num_mid_by_offline_playduration(fill_mid_info_map, need_num, fill_cand_id_vec);
        } else if (score_type == "hot") {
            get_need_num_mid_by_hotscore(fill_mid_info_map, need_num, fill_cand_id_vec);
        } else if (score_type == "gif_hot") {
            get_need_num_mid_by_gif_hotscore(fill_mid_info_map, need_num, fill_cand_id_vec);
        }

        int fill_num = get_video_feat(fill_mid_info_map, need_feature);
        cand_num += fill_num;

        for (auto &fill_mid_info : fill_mid_info_map) {
            mid_info_map[fill_mid_info.first] = fill_mid_info.second;
        }
    }

    // del sim mid by sim_hash
    del_sim_mids(tobj_id_str, mid_info_map);

    if (score_type == "gif_hot") {
        return  return_gif_json("candidate", cand_num, "ok", req_id_str, user_feat, mid_info_map, cand_id_vec, p_out_string, n_out_len);
    }

    return  return_json("candidate", cand_num, "ok", req_id_str, user_feat, mid_info_map, cand_id_vec, p_out_string, n_out_len);
}
