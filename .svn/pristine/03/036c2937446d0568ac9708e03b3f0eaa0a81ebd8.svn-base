#include "test_work_interface.h"

DYN_WORK(TestWorkInterface);

int TestWorkInterface::return_fail(char* &p_out_string, int &n_out_len) {
    n_out_len = sprintf(p_out_string, "{\"ret_code\": -1}");
    return 0;
}

int TestWorkInterface::redis_get(const string &key, string &value) {
    LOG_DEBUG("key:%s", key.c_str());
    HiRedisDbInterface* p_redis_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("TEST");
    if (NULL == p_redis_db_interface) {
        LOG_ERROR("get redis db_interface fail");
        return -1;
    }

    int redis_value_len = p_redis_db_interface->string_get(key, value);
    if (redis_value_len < 0) {
        LOG_ERROR("redis get fail");
        return -1;
    }

    LOG_DEBUG("value:%s", value.c_str());

    int len = value.size();
    LOG_DEBUG("len:%d", len);

    // const char* p_value = value.data();
    // uint64_t* mids = (uint64_t*)&p_value[0];
    // for (int i = 0; i < len / 8; i++) {
    //     uint64_t mid = mids[i];
    //     LOG_DEBUG("mid: %"PRIu64" ", mid);
    // }

    return 0;
}

int TestWorkInterface::redis_mget(const set<string> &keys, MapStringString &key_value_map) {
    HiRedisDbInterface* p_redis_db_interface = (HiRedisDbInterface*)p_db_company_->get_db_interface("TEST");
    if (NULL == p_redis_db_interface) {
        LOG_ERROR("get redis db_interface fail");
        return -1;
    }

    int ret_code = p_redis_db_interface->string_mget(keys, key_value_map);
    if (ret_code < 0) {
        LOG_ERROR("redis get fail");
        return -1;
    }

    for (MapStringString::iterator it = key_value_map.begin(); it != key_value_map.end(); ++it) {
        string key = (*it).first;
        string value = (*it).second;

        LOG_DEBUG("key:%s value:%s", key.c_str(), value.c_str());
    }

    return 0;
}

int TestWorkInterface::lushan_get(const string &key, string &value) {
    DbInterface* p_lushan_db_inteface = p_db_company_->get_db_interface("VENUS_SIM_USER");
    if (NULL == p_lushan_db_inteface) {
        LOG_ERROR("get lushan db_interface fail");
        return -1;
    }

    char* p_value = NULL;
    char split_char, second_split_char;
    int ret_code = p_lushan_db_inteface->s_get(0, key.c_str(), p_value, split_char, second_split_char, 0);
    if (ret_code < 0) {
        LOG_ERROR("lushan get fail");
        return -1;
    } else if (ret_code == 0) {
        LOG_ERROR("key:%s not exist", key.c_str());
        return 0;
    }

    value = "";
    for (int i = 0; i < ret_code; i++) {
        value += *(p_value + i);
    }

    int len = value.size();
    LOG_DEBUG("value:%s len:%d", value.c_str(), len);

    int uid_num = len / 12;
    for (int i = 0; i < uid_num; i++) {
        uint64_t sim_uid;
        memcpy(&sim_uid, p_value, sizeof(uint64_t));
        p_value += 8;
        uint32_t sim_score;
        memcpy(&sim_score, p_value, sizeof(uint32_t));
        p_value += 4;
        LOG_ERROR("uid:%"PRIu64" score:%"PRIu32"", sim_uid, sim_score);
    }

    return len;
}

int TestWorkInterface::test_rapidjson(const char* input_json_cstr) {
    rapidjson::Document document;
    if (document.Parse(input_json_cstr).HasParseError()) {
        LOG_ERROR("parse input json: %s fail", input_json_cstr);
        return -11;
    }

    if (!document.IsObject()) {
        LOG_ERROR("input json: %s is not object", input_json_cstr);
        return -1;
    }

    if (!document.HasMember("req_id")) {
        LOG_ERROR("input json:%s req_id is empty", input_json_cstr);
        return -1;
    }
    string req_id_str = document["req_id"].GetString();

    if (!document.HasMember("cand_id")) {
        LOG_ERROR("input json:%s cand_id is empty", input_json_cstr);
        return -1;
    }
    int cand_id = document["cand_id"].GetInt();
    LOG_DEBUG("cand_id:%d", cand_id);

    if (!document.HasMember("re_query")) {
        LOG_ERROR("input json:%s re_query is empty", input_json_cstr);
        return -1;
    }
    int re_query = document["re_query"].GetInt();
    if (re_query < 0 || re_query > 3) {
        LOG_ERROR("input json:%s re_query is invalid", input_json_cstr);
    }

    if (!document.HasMember("obj_id")) {
        LOG_ERROR("input json:%s obj_id is empty", input_json_cstr);
        return -1;
    }
    string obj_id_str = document["obj_id"].GetString();

    if (!document.HasMember("tobj_id")) {
        LOG_ERROR("input json:%s tobj_id is empty", input_json_cstr);
        return -1;
    }
    string tobj_id_str = document["tobj_id"].GetString();

    if (!document.HasMember("num")) {
        LOG_ERROR("input json:%s num is empty", input_json_cstr);
        return -1;
    }
    int num = document["num"].GetInt();
    if (num <= 0 || num > 100) {
        LOG_ERROR("input json:%s num is invalid", input_json_cstr);
        return -1;
    }

    rapidjson::Document out_doc;
    out_doc.SetObject();
    rapidjson::Value ret_code;
    ret_code.SetInt(1);
    out_doc.AddMember("ret_code", ret_code, out_doc.GetAllocator());

    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    out_doc.Accept(writer);

    string out_str = sb.GetString();
    LOG_INFO("ouput json:%s", out_str.c_str());

    return 0;
}

int TestWorkInterface::return_json(const char* req_cmd, int ret_code, const char* ret_msg, const char* req_id, int cand_id, rapidjson::Value &cand_list_val, char* &p_out_string, int &n_out_len) {
    rapidjson::Document out_doc(rapidjson::kObjectType);

    rapidjson::Value req_cmd_val;
    req_cmd_val.SetString(rapidjson::StringRef(req_cmd));
    rapidjson::Value ret_code_val(ret_code);
    rapidjson::Value cand_id_val(cand_id);
    rapidjson::Value ret_msg_val;
    ret_msg_val.SetString(rapidjson::StringRef(ret_msg));
    rapidjson::Value req_id_val;
    req_id_val.SetString(rapidjson::StringRef(req_id));

    out_doc.AddMember("ret_code", ret_code_val, out_doc.GetAllocator());
    out_doc.AddMember("cand_id", cand_id_val, out_doc.GetAllocator());
    out_doc.AddMember("req_cmd", req_cmd_val, out_doc.GetAllocator());
    out_doc.AddMember("ret_msg", ret_msg_val, out_doc.GetAllocator());
    out_doc.AddMember("req_id", req_id_val, out_doc.GetAllocator());
    out_doc.AddMember("cand_list", cand_list_val, out_doc.GetAllocator());

    rapidjson::StringBuffer sb(0, 1024);
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    out_doc.Accept(writer);

    n_out_len = sprintf(p_out_string, sb.GetString());

    LOG_DEBUG("p_out_string:%s", p_out_string);

    return 1;
}

int TestWorkInterface::test_openapi(const string &mid, string &value) {
    DbInterface* p_http_db_interface = p_db_company_->get_db_interface("OPEN_API_STATUSES_SHOW");
    if (NULL == p_http_db_interface) {
        LOG_ERROR("get http db_interface fail");
        return -1;
    }

    char* p_result;
    char split_char, second_split_char;
    int ret_code = p_http_db_interface->s_get(0, mid.c_str(), p_result, split_char, second_split_char);
    if (ret_code <= 0 || p_result == NULL) {
        LOG_ERROR("s_get open fail");
        return -1;
    }
    LOG_DEBUG("open result:%s", p_result);

    rapidjson::Document doc;
    if (doc.Parse(p_result).HasParseError()) {
        LOG_ERROR("parse open result fail json: %s", p_result);
        return -1;
    }

    if (!doc.IsObject()) {
        LOG_ERROR("open result json: %s is not object", p_result);
        return -1;
    }

    if (!doc.HasMember("text")) {
        LOG_ERROR("open result json: %s text is empty", p_result);
        return -1;
    }

    value = doc["text"].GetString();
    return 0;
}

int TestWorkInterface::woo_get(const string &request,string &response) {
    WooDbInterface* p_woo_db_interface = (WooDbInterface*)p_db_company_->get_db_interface("WOO_TEXT_ANALYZE");
    if (NULL == p_woo_db_interface) {
        LOG_ERROR("get woo db_interface fail");
        return -1;
    }

    char temp[1024];
    snprintf(temp, 1024, "%s", request.c_str());
    char* p_result = NULL;
    char split_char, second_split_char;
    p_woo_db_interface->s_get(0, temp, p_result, split_char, second_split_char);
    LOG_DEBUG("woo result:%s", p_result);
    response = p_result;

    return 1;
}

int TestWorkInterface::mc_set(const string &key, const string &value) {
    McDbInterface* p_mc_db_interface = (McDbInterface*)p_db_company_->get_db_interface("CKESTREL");
    if (NULL == p_mc_db_interface) {
        LOG_ERROR("get mc db_interface fail");
        return -1;
    }
    int ret_code = p_mc_db_interface->set(key, value);
    LOG_DEBUG("mc set %s ret_code: %d", key.c_str(), ret_code);
    return 0;
}

int TestWorkInterface::global_data_get() {
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
    for (auto &mid : mids_vec) {
        LOG_ERROR("mid:%s", mid.c_str());
    }

    return -1;
}

int TestWorkInterface::work_core(json_object* req_json, char* &p_out_string, int &n_out_len, int64_t req_id) {
    const char* json_cstr = json_object_to_json_string(req_json);
    LOG_INFO("json_cstr:%s", json_cstr);

    // string mid("3996019803398893");
    // string value;
    // int ret_code = test_openapi(mid, value);
    // LOG_DEBUG("text:%s", value.c_str());

    // rapidjson::Document request_doc(rapidjson::kObjectType);

    // rapidjson::Value extend_topn_val(10);
    // rapidjson::Value topic_topn_val(3);
    // rapidjson::Value keyword_topn_val(5);
    // rapidjson::Value text_val;
    // text_val.SetString(rapidjson::StringRef(value.c_str()));

    // rapidjson::Value body_val(rapidjson::kObjectType);
    // body_val.AddMember("extend_topn", extend_topn_val, request_doc.GetAllocator());
    // body_val.AddMember("topic_topn", topic_topn_val, request_doc.GetAllocator());
    // body_val.AddMember("keyword_topn", keyword_topn_val, request_doc.GetAllocator());
    // body_val.AddMember("text", text_val, request_doc.GetAllocator());

    // request_doc.AddMember("body", body_val, request_doc.GetAllocator());
    // rapidjson::Value cmd_val;
    // cmd_val.SetString(rapidjson::StringRef("text_analyze"));
    // request_doc.AddMember("cmd", cmd_val, request_doc.GetAllocator());

    // rapidjson::StringBuffer sb(0, 1024);
    // rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    // request_doc.Accept(writer);

    // string request = sb.GetString();
    // LOG_DEBUG("woo request:%s", request.c_str());

    // string response;
    // char temp[1024];
    // snprintf(temp, 1024, "{\"body\":{\"extend_topn\":10,\"topic_topn\":3,\"keyword_topn\":5,\"text\":\"%s\"},\"cmd\":\"text_analyze\"}", value.c_str());
    // LOG_DEBUG("temp %s", temp);
    // string request(temp);
    // woo_get(request, response);
    // LOG_DEBUG("woo response:%s", response.c_str());

    // json_object* redis_key_json = json_object_object_get(req_json, "redis_key");
    // if (NULL == redis_key_json) {
    //     return return_fail(p_out_string, n_out_len);
    // }

    // const char* redis_key_cstr = json_object_get_string(redis_key_json);

    // string key(redis_key_cstr);
    // string value;

    // int ret_code = redis_get(key, value);

    // string key2("7654321");
    // set<string> keys;
    // keys.insert(key);
    // keys.insert(key2);

    // MapStringString key_value_map;

    // int ret_code = redis_mget(keys, key_value_map);

    // json_object* lushan_key_json = json_object_object_get(req_json, "lushan_key");
    // if (NULL == redis_key_json) {
    //     return return_fail(p_out_string, n_out_len);
    // }
    // const char* lushan_key_cstr = json_object_get_string(lushan_key_json);

    // string lushan_key(lushan_key_cstr);
    // ret_code = lushan_get(lushan_key, value);

    // int ret_code = test_rapidjson(json_cstr);

    // rapidjson::Value cand_list_val(rapidjson::kArrayType);
    // return_json("candidate", 1, "test msg", "1234567", 2001, cand_list_val, p_out_string, n_out_len);

    // string key("test_queue");

    // mc_set(key, value);

    global_data_get();

    return 0;
}
