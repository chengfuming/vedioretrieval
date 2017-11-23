#ifndef _USER_WORK_INTERFACE_HEADER_
#define _USER_WORK_INTERFACE_HEADER_

#include "db_company.h"
#include "ini_file.h"
#include "woo/log.h"
#include <map>
#include <set>
#include <vector>
#include <fstream>
#include <iostream>
#include "json.h"
#include <time.h>
#include <algorithm_interface.h>
#include <algorithm_func.h>

class UserWorkInterface : public WorkInterface{
	public:
		UserWorkInterface(DbCompany*& p_db_company, int interface_id):
			WorkInterface(p_db_company, interface_id){
			
		}
		virtual ~UserWorkInterface(){
		}

	protected:
		int get_token_uid(uint64_t uid, char* uid_token_str){
			if(NULL == p_db_company_ || NULL == uid_token_str)
				return -1;
		
			char* token_str = NULL;
			DbInterface* p_token_db_interface = p_db_company_->get_db_interface("TOKEN_DB");
			if(NULL == p_token_db_interface){
				LOG_ERROR("token db_interface is NULL!");
				return 0;
			}else{
				char split_char, second_split_char;
				int result = p_token_db_interface->s_get(0, "open_api_token", token_str, split_char, second_split_char);
				if(result <= 0){
					LOG_DEBUG("get token is error!");
					return 0;
				}
			}

			snprintf(uid_token_str, PATH_MAX, "%"PRIu64":%s", uid, token_str);//"794198");
			return 1;

		}

		int get_fri_filter_set(char* p_fri_result, char split_char, char second_split_char,
				__gnu_cxx::hash_set<uint64_t>& hashset_filter_uids, int num = -1){
			if(NULL == p_fri_result)
				return -1;

			uint32_t id_num = 0;
			split_string_set_ids(hashset_filter_uids, id_num, 
					(const char* &)p_fri_result, split_char, num);

			return 1;
		}
	
		// 获取关注列表 from open API
		int get_follow_from_API(const uint64_t uid, __gnu_cxx::hash_set<uint64_t>& hashset_filter_uids, 
				int num = -1){

			DbInterface* p_follow_db_interface = p_db_company_->get_db_interface("OPEN_API_FOLLOW_DB");
			if (NULL == p_follow_db_interface){
				LOG_ERROR("gz api db_interface error!");
				return -3;
			}
			char uid_token_str[PATH_MAX];
			char* token_str = NULL;
			snprintf(uid_token_str, PATH_MAX, "%"PRIu64":%s", uid, token_str);
			char* p_result = NULL;
			char split_char, second_split_char;
			int result = p_follow_db_interface->s_get(0, uid_token_str, 
					p_result, split_char, second_split_char);
			if (result <= 0 ){
				LOG_ERROR("get gz from api error!");
				return -2;
			}
			if (NULL == p_result){
				LOG_ERROR("api gz data is NULL!");
				return -1;
			}

			// json解析平台取到的关注数据
			json_object* req_json = json_tokener_parse(p_result);		// 结果解析成json对象
			if ( is_error(req_json) || NULL == req_json ){
				LOG_ERROR("json parse error! result: %s", p_result);
				json_object_put(req_json);
				return -1;
			}

			json_object* follow_ids_json = json_object_object_get(req_json, "ids");		// 获取id字段
			if (NULL == follow_ids_json){
				LOG_ERROR("json parse error!");
				json_object_put(req_json);
				return -1;
			}else{
				const char* follow_ids_str = NULL;
				follow_ids_str = (const char*)json_object_get_string(follow_ids_json);
				uint32_t follow_uid_num = 0;
				split_string_set_ids(hashset_filter_uids, follow_uid_num, 
						(const char* &)follow_ids_str, split_char, num);
			}
			
			json_object_put(req_json);
			return 1;
		}
		
		// 个人黑名单
		int get_block_filter_set(char* p_block_result, char sep_char, char second_sep_char,
				__gnu_cxx::hash_set<uint64_t>& hashset_filter_uids, int num = -1){
			if(NULL == p_block_result)
				return -1;

			char* p_temp = p_block_result;

			uint64_t id = 0;
			int index = 0;
			while(*p_temp != '\0'){
				if(num != -1 && index >= num){
					break;
				}
				if(*p_temp != '[' && *p_temp != ']'){
					if(*p_temp == sep_char){
						if(id != 0){
							index ++;
							hashset_filter_uids.insert(id);
						}
						id = 0;
					}
					else if(*p_temp >= '0' && *p_temp <= '9'){
						id = id * 10 + (*p_temp) - '0';
					}
				}
				p_temp ++;
			}
			if((num == -1 || index < num) && id != 0){
				hashset_filter_uids.insert(id);
			}

			return 1;
		}

		// 不感兴趣
		int get_uninterest_filter_set(char* p_uninterest_result, char sep_char, char second_sep_char,
				__gnu_cxx::hash_set<uint64_t>& hashset_filter_uids, int num = -1){

			if(NULL == p_uninterest_result)
				return -1;

			char* p_temp = p_uninterest_result;
			uint64_t id = 0;
			int index = 0;
			bool id_flag = false;
			while((*p_temp) != '\0'){
				if(num != -1 && index >= num){
					break;
				}
				if ((*p_temp) >= '0' && (*p_temp) <= '9'){
					if (id_flag){
						id = id * 10 + (*p_temp) - '0';
					}
				}else if((*p_temp) == second_sep_char){
					if(id != 0){
						index ++;
						hashset_filter_uids.insert(id);
					}

					id = 0;
					id_flag = false;
				}else if((*p_temp) == sep_char){
					id_flag = true;
				}
				p_temp ++;
			}
			if((num == -1 || index < num) && id != 0){
				hashset_filter_uids.insert(id);
			}
			return 1;
		}

		// 取消关注
		int get_unfollow_filter_set(char* p_unfollow_result, char sep_char, char second_sep_char,
				__gnu_cxx::hash_set<uint64_t>& hashset_filter_uids, int num = -1){

			if(NULL == p_unfollow_result)
				return -1;

			char* p_temp = p_unfollow_result;

			char temp_arr[WORD_LEN] = {0};
			int temp_num = 0;
			bool item_flag = false;
			uint64_t id = 0;
			int index = 0;
			while(*p_temp != '\0'){
				if(num != -1 && index >= num)
					break;

				if(*p_temp == sep_char){
					item_flag = false;

					if(id != 0){
						index ++;
						hashset_filter_uids.insert(id);
					}
					id = 0;
					temp_arr[0] = '\0';
					temp_num = 0;
				}else{
					if(!item_flag){
						temp_arr[temp_num] = *p_temp;
						temp_num ++;
						temp_arr[temp_num]= '\0';

						if(strcmp(temp_arr, "\"item_key\":") == 0){
							item_flag = true;
						}
					}else{
						if(*p_temp >= '0' && *p_temp <= '9')
							id = id * 10 + *p_temp - '0';
					}
				}
					
				p_temp ++;
			}

			//lookis to add last one

			return 1;
		}
		
		// 获取不能推荐的用户集合
		int get_filter_uids(uint64_t uid, __gnu_cxx::hash_set<uint64_t>& hashset_filter_uids,
				int num = -1){
			
			if(NULL == p_db_company_){
				LOG_ERROR("db_company is NULL!");
				return -1;
			}

			//添加自己;
			hashset_filter_uids.insert(uid);

			//获取关注，屏蔽，不感兴趣以及取消关注服务
			ReqResultMap req_result_map;
			
			int fri_db_id = 3; // 关注db
			int result = push_request_result_map(fri_db_id, uid, 0, 0, NULL, true, req_result_map);
			if(result <= 0)
				LOG_ERROR("create friend db is failed!");

			int block_db_id = 7; //屏蔽db
			char uid_token_str[PATH_MAX];
			result = get_token_uid(uid, uid_token_str);

			if(result <= 0 )
				LOG_ERROR("get token is failed!");
			else{
				result = push_request_result_map(block_db_id, uid, 0, 0, uid_token_str, false, req_result_map);
				if(result <= 0)
					LOG_ERROR("get block uid is failed!");
			}

			int uninterest_db_id = 1; // 不感兴趣db
			char uid_un_str[WORD_LEN];
			snprintf(uid_un_str, WORD_LEN, "%"PRIu64":user", uid);
			result = push_request_result_map(uninterest_db_id, uid, 0, 0, uid_un_str, false, req_result_map);

			int unfollow_db_id = 0; // 取消关注db
			char uid_unfollow_str[WORD_LEN];
			snprintf(uid_unfollow_str, WORD_LEN, 
					"{\"cmd\":\"merge_key\",\"keys\":[\"%"PRIu64"\"],\"req_num\":200}", uid);
			result = push_request_result_map(unfollow_db_id, uid, 0, 0, uid_unfollow_str, false, req_result_map);

			if(p_db_company_->get_multi_db(req_result_map) > 0){
				char* p_fri_result = NULL;
				char split_char, second_split_char;
				
				// 获取关注数据
				get_data_from_req_result_map(fri_db_id, req_result_map,
						        p_fri_result, split_char, second_split_char);
				result = get_fri_filter_set(p_fri_result, split_char, 
								second_split_char, hashset_filter_uids, num);
				if(result <= 0){// 没有取到关注数据，从平台取
					get_follow_from_API(uid, hashset_filter_uids, num);	
				}

				// 获取个人黑名单
				char* p_block_result = NULL;
				get_data_from_req_result_map(block_db_id, req_result_map,
						        p_block_result, split_char, second_split_char);
				get_block_filter_set(p_block_result, split_char,
								second_split_char, hashset_filter_uids, num);

				// 获取不感兴趣
				char* p_uninterest_result = NULL;
				get_data_from_req_result_map(uninterest_db_id, req_result_map,
						        p_uninterest_result, split_char, second_split_char);
				get_uninterest_filter_set(p_uninterest_result, split_char, 
								second_split_char, hashset_filter_uids, num);

				// 获取取消关注
				char* p_unfollow_result = NULL;
				get_data_from_req_result_map(unfollow_db_id, req_result_map,
						        p_unfollow_result, split_char, second_split_char);
				get_unfollow_filter_set(p_unfollow_result, split_char, 
								second_split_char, hashset_filter_uids, num);

			}
			release_request_result_map(req_result_map);
			return 1;
		}

		// 过滤模块函数
		int filter_users(const char* uid_str,
				const std::vector<uint64_t>& vec_reco_uids,
				std::vector<uint64_t>& diff_uids){
			if( NULL == uid_str || NULL == p_db_company_ )
				return -1;

			//int spend_msec = -1;
            //struct timeval tv_temp;
            //tv_temp = calc_spend_time(tv_temp, "filter start", spend_msec, true);
			
			uint64_t uid = strtoull(uid_str, NULL, 10);
		
			__gnu_cxx::hash_set<uint64_t> hashset_filter_uids;
			get_filter_uids(uid, hashset_filter_uids);
			//tv_temp = calc_spend_time(tv_temp, "get data", spend_msec);
			
			for(std::vector<uint64_t>::const_iterator it = vec_reco_uids.begin();
					it != vec_reco_uids.end(); it++){

				__gnu_cxx::hash_set<uint64_t>::iterator other_it = hashset_filter_uids.find((*it));
				if(other_it != hashset_filter_uids.end()){
					;
				}else{
					diff_uids.push_back((*it));
				}
			}
			//tv_temp = calc_spend_time(tv_temp, "filter all finish", spend_msec);
			return 1;
		}

		int generate_output_str(const std::vector<uint64_t>& result_uids, int num,
				char*& p_out_string, int& n_len){

			n_len = 0;
			char* p_out_temp = p_out_string;
			int len = sprintf(p_out_temp, "%s", "{\"return_code\":\"200\",\"result\":[");
			p_out_temp = p_out_temp + len;
			n_len += len;

			int index = 0;
			for(std::vector<uint64_t>::const_iterator it = 
					result_uids.begin(); it != result_uids.end(); it++){

				if(index == 0)
					len = sprintf(p_out_temp, "%"PRIu64, (*it));
				else
					len = sprintf(p_out_temp, ",%"PRIu64, (*it));
				p_out_temp = p_out_temp + len;
				n_len += len;

				index ++;
			}
			
			len = sprintf(p_out_temp, "%s", "]}");
			p_out_temp = p_out_temp + len;
			n_len += len;

			return 1;
		}
		
		int generate_output_str(__gnu_cxx::hash_set<uint64_t>& result_uids, int num,
				char*& p_out_string, int& n_len){

			n_len = 0;
			char* p_out_temp = p_out_string;
			int len = sprintf(p_out_temp, "%s", "{\"return_code\":\"200\",\"result\":[");
			p_out_temp = p_out_temp + len;
			n_len += len;

			int index = 0;
			for(__gnu_cxx::hash_set<uint64_t>::const_iterator it = 
					result_uids.begin(); it != result_uids.end(); it++){

				if(index == 0)
					len = sprintf(p_out_temp, "%"PRIu64, (*it));
				else
					len = sprintf(p_out_temp, ",%"PRIu64, (*it));
				p_out_temp = p_out_temp + len;
				n_len += len;

				index ++;
			}
			
			len = sprintf(p_out_temp, "%s", "]}");
			p_out_temp = p_out_temp + len;
			n_len += len;

			return 1;
		}
		
	public:
		//定义数据
		#define MAX_NUM 150
		#define MAX_SEC_NUM 100
		#define BRI_LEN 10 
		#define LARGE_PERCENT 1000000
		#define MAX_FOL_NUM MAX_FOLLOW_NUM

		typedef enum _REL_TYPE{
			REL_TYPE_FOL,		// 关注
			REL_TYPE_HUF		// 互粉
		} REL_TYPE;

		typedef enum _RECO_TYPE{
			RECO_TYPE_GOG,		// 关注的关注
			RECO_TYPE_GOH,		// 好友的关注
			RECO_TYPE_HOG,		// 关注的好友
			RECO_TYPE_HOH		// 好友的好友
		} RECO_TYPE;

		typedef enum _USER_TYPE{
			USER_TYPE_CMN,
			USER_TYPE_YV,
			USER_TYPE_BV
		} USER_TYPE;

		struct REL_UID{
			uint64_t uid_; // uid
			uint32_t qmd_; // qmd score
			uint8_t type_; // relation type: 0 is follow; 1 is for friend
		};

		int release_rel_uids( std::map<uint64_t, REL_UID*>& rel_uids){
			for(std::map<uint64_t, REL_UID*>::const_iterator it = rel_uids.begin();
					it != rel_uids.end(); it++){
				if((*it).second != NULL){
					delete (*it).second;
				}	
			}
			rel_uids.clear();
			return 1;
		}
		
		typedef pair<uint64_t, float> PAIR_INT_FLOAT;	
		static bool compare_pair_int_float(const PAIR_INT_FLOAT& c1, const PAIR_INT_FLOAT& c2){
				return c1.second > c2.second;
		}

		struct RECO_UID{
			uint64_t uid_;
			uint32_t score_;
			uint16_t bri_num_;
			uint8_t type_; //0 is for gz of gz and  3 is for hufen of hufen
			uint64_t bri_uids_[BRI_LEN];	// 最多10个桥梁 
		};

		typedef std::map<uint64_t, RECO_UID*> MAP_RECO_UID;
		typedef std::vector<RECO_UID*> VEC_RECO_UID;
		static bool compare_reco_result(const RECO_UID* c1, const RECO_UID* c2){
			return c1->score_ > c2->score_;
		}

		int release_map_reco_uid(std::map<uint64_t, RECO_UID*>& map_reco_uid){
			for(std::map<uint64_t, RECO_UID*>::iterator it = map_reco_uid.begin();
					it != map_reco_uid.end(); it ++){
				if((*it).second != NULL){
					delete (*it).second;
				}
			}
			map_reco_uid.clear();
			return 1;
		}
		
		//定义数据结束

		//批量获取关注或者互粉数据
		int mget_rel_uids(uint64_t qmd_uids[], uint32_t get_uid_num, MapMResult& map_m_hoh_result,
				char& split_char, char& second_split_char, int db_id){ // 6 is hufen; 3 is gz
			DbInterface* p_db_interface = p_db_company_->get_db_interface(db_id);
			if(NULL == p_db_interface){
				LOG_ERROR("%d db_interface is NULL!", db_id);
				return -1;
			}
            int result = p_db_interface->mget(0, qmd_uids, get_uid_num, map_m_hoh_result, 
					split_char, second_split_char);
			if (result <= 0)
				return result;

			return 1;
		}

		// 这个函数可以取多种二度关系，但是设计之时，只保证关注的关注和好友的好友
		// map_reco_uid表示推荐结果字典，包含推荐理由；vec_reco_uid是推荐结果集合(带桥梁)
		int get_indirect_func( MAP_RECO_UID& map_reco_uid, VEC_RECO_UID& vec_reco_uid,
				uint64_t only_uids[], uint32_t only_uid_num, 
				const std::map<uint64_t, REL_UID*>& rel_uids,
				uint8_t type/*type = 0 for gz of gz ; 3 for hu of hu*/){

			MapMResult map_m_result;
			char split_char, second_split_char;

			// 只取两种，其实可以取四种二度关系的
			if(type == RECO_TYPE_GOG) // 关注的关注
				mget_rel_uids(only_uids, only_uid_num, map_m_result,
							split_char, second_split_char, 3);
			else //好友的好友
				mget_rel_uids(only_uids, only_uid_num, map_m_result,
							split_char, second_split_char, 6);

			for(MapMResult::const_iterator it = map_m_result.begin();
					                    it != map_m_result.end(); it++){
				// 对于每一个结果，第一个元素是桥梁uid，第二个元素是推荐uid集合字符串
				uint64_t bri_uid = (*it).first;

				uint32_t qmd = 0;					// 桥梁亲密度，否则为0
				std::map<uint64_t, REL_UID*>::const_iterator rel_it = rel_uids.find(bri_uid);
				if(rel_it != rel_uids.end()){
					if((*rel_it).second != NULL){
						qmd = (*rel_it).second->qmd_;
					}
				}

				const char* p_result = (*it).second;	// 桥梁的关注或者互粉
				if(NULL == p_result){
					continue;
				}
				uint32_t uid_num = 0;
				/*uint64_t uids[MAX_SEC_NUM];				// 截断推荐结果
				split_string_ids(uids, uid_num, (const char* &)p_result, split_char, MAX_SEC_NUM);
				MAP_RECO_UID::iterator reco_uid_it;
				for(uint32_t index = 0; index < uid_num; index ++){
					uint64_t reco_uid = uids[index];*/
				//2015/04/15修改，使用集合来保证推荐结果不重复，以此来保证桥梁不会出现重复
				__gnu_cxx::hash_set<uint64_t> uids;
				MAP_RECO_UID::iterator reco_uid_it;
				split_string_set_ids(uids, uid_num, (const char* &)p_result, split_char, MAX_FOL_NUM);
				uid_num = (uids.size() < MAX_SEC_NUM)? uids.size() : MAX_SEC_NUM;
				uint32_t index = 0;
				for(__gnu_cxx::hash_set<uint64_t>::iterator it = uids.begin(); it != uids.end(); ++ it){
					uint64_t reco_uid = *it;
					index ++;
					if(index >= uid_num) break;
					reco_uid_it = map_reco_uid.find(reco_uid);
					if(reco_uid_it != map_reco_uid.end()){	// 若推荐结果在集合中
						RECO_UID* p_reco_uid = (*reco_uid_it).second;
						if(p_reco_uid != NULL){				// 这个取模很重要，否则桥梁可能越界
							p_reco_uid->bri_uids_[p_reco_uid->bri_num_ % BRI_LEN] = bri_uid;
							p_reco_uid->bri_num_ ++;		// 桥梁数增加
							p_reco_uid->score_ += qmd;		// 分值为qmd累加
						}

					}else{
						RECO_UID* p_reco_uid = new RECO_UID;
						if(NULL == p_reco_uid){
							LOG_ERROR("new reco uid is failed!");
							continue;
						}
						p_reco_uid->uid_ = reco_uid;
						p_reco_uid->score_ = qmd;
						p_reco_uid->type_ = type;
						p_reco_uid->bri_num_ = 0;
						p_reco_uid->bri_uids_[p_reco_uid->bri_num_ % BRI_LEN] = bri_uid;
						p_reco_uid->bri_num_ = 1;

						map_reco_uid.insert(MAP_RECO_UID::value_type(reco_uid, p_reco_uid));
						vec_reco_uid.push_back(p_reco_uid);
					}
				}
		// map_reco_uid在后续并没有被使用，只是用来改变p_reco_uid这个指针的，这样vec_reco_uid的内容也改变了
			}
			map_m_result.clear();
			//std::sort(vec_reco_uid.begin(), vec_reco_uid.end(), compare_reco_result);
			return 1;
		}
	
		// 获取关系数据
		int get_rel_uids(uint64_t uid, uint32_t num,  
				uint64_t fri_uids[], uint32_t fri_uid_num,
				uint64_t fol_uids[], uint32_t fol_uid_num,
				const std::vector<PAIR_INT_FLOAT>& qmd_uids,
				char* p_out_string, int& n_out_len){

			std::map<uint64_t, REL_UID*> rel_uids;
			int real_num = 0;

			// 先放亲密度
			for(vector<PAIR_INT_FLOAT>::const_iterator qmd_it = qmd_uids.begin();
					qmd_it != qmd_uids.end(); qmd_it++){

				if(real_num >= MAX_NUM)				// 亲密度数据充足了，则退出
					break;

				uint64_t qmd_uid = (*qmd_it).first;
				uint32_t score_ = uint32_t((*qmd_it).second * LARGE_PERCENT);
				std::map<uint64_t, REL_UID*>::iterator rel_it = rel_uids.find(qmd_uid);
			
				if(rel_it != rel_uids.end()){		// 若uid已存在集合中，更新qmd数值
					REL_UID* rel_uid = (*rel_it).second;
					if(rel_uid != NULL){
						rel_uid->qmd_ = score_;
					}
				}else{								// 若uid不在集合中，则新建一个
					REL_UID* rel_uid = new REL_UID;
					if(rel_uid == NULL){
						LOG_ERROR("new real uid is failed!");
						continue;
					}
					rel_uid->uid_ = qmd_uid;
					rel_uid->qmd_ = score_;
					rel_uid->type_ = REL_TYPE_FOL;	// 基于QMD的推荐

					real_num ++;					// 只取部分亲密度

					rel_uids.insert(std::map<uint64_t, REL_UID*>::value_type(qmd_uid, rel_uid));
				}
			}
			
			uint64_t only_fri_uids[MAX_NUM];		// 只取部分好友
			uint64_t only_fri_uid_num = 0;

			for(uint32_t i = 0; i < fri_uid_num; i++){		// 这里有个坑。假设fri_uids是不重复的，那么最多存亲密度数量个用户进去
				uint64_t fri_uid = fri_uids[i];				// 这样only_fri_uids就不会越界。但是如果fri_uids重复了，那很容易就越界了
				std::map<uint64_t, REL_UID*>::iterator rel_it = rel_uids.find(fri_uid);
			
				if(rel_it != rel_uids.end()){		// 若uid在集合中，即QMD已经放进去了
					REL_UID* rel_uid = (*rel_it).second;
					if(rel_uid != NULL){
						rel_uid->type_ = REL_TYPE_HUF;				// 更新type类型
						only_fri_uids[only_fri_uid_num] = fri_uid;	// 将好友保存下来
						only_fri_uid_num ++;						// 用于数量控制
					}
				}else{

					if(real_num >= MAX_NUM)			// 只取部分好友
						continue;					// 这里不能用break，为了保证上面的逻辑

					REL_UID* rel_uid = new REL_UID;
					if(rel_uid == NULL){
						LOG_ERROR("new real uid is failed!");
						continue;
					}
					rel_uid->uid_ = fri_uid;
					rel_uid->qmd_ = 0;
					rel_uid->type_ = REL_TYPE_HUF;

					only_fri_uids[only_fri_uid_num] = fri_uid;
					only_fri_uid_num ++;

					real_num ++;

					rel_uids.insert(std::map<uint64_t, REL_UID*>::value_type(fri_uid, rel_uid));
				}
			}
			
			uint64_t only_fol_uids[MAX_NUM];	// 只取部分关注
			uint64_t only_fol_uid_num = 0;

			for(uint32_t i = 0; i < fol_uid_num; i++){
				uint64_t fol_uid = fol_uids[i];
				std::map<uint64_t, REL_UID*>::iterator rel_it = rel_uids.find(fol_uid);

				if(rel_it != rel_uids.end()){	// 若uid在集合中，即QMD已经放进去了	
					REL_UID* rel_uid = (*rel_it).second;
					if(rel_uid != NULL && rel_uid->type_ != 1){
						only_fol_uids[only_fol_uid_num] = fol_uid;
						only_fol_uid_num ++;	// qmd和关注的type类型是一样的
					}
				}else{
					if(real_num >= MAX_NUM)
						continue;
					REL_UID* rel_uid = new REL_UID;
					if(rel_uid == NULL){
						LOG_ERROR("new real uid is failed!");
						continue;
					}
					real_num ++;
					rel_uid->uid_ = fol_uid;
					rel_uid->qmd_ = 0;
					rel_uid->type_ = REL_TYPE_FOL;
					only_fol_uids[only_fol_uid_num] = fol_uid;
					only_fol_uid_num ++;
					rel_uids.insert(std::map<uint64_t, REL_UID*>::value_type(fol_uid, rel_uid));
				}
			}
			
			//int spend_msec = -1;
			//struct timeval tv_temp;
			//tv_temp = calc_spend_time(tv_temp, "mget start", spend_msec, true);
	
			//批量获取好友的好友
			MAP_RECO_UID h_map_reco_uid;	// 存储基于好友的好友的推荐<uid,理由>
			VEC_RECO_UID h_vec_reco_uid;	// 存储基于好友的好友的推荐uid集合
			get_indirect_func( h_map_reco_uid, h_vec_reco_uid, only_fri_uids, only_fri_uid_num, 
					rel_uids, RECO_TYPE_HOH);
			// 20150527添加，将全排序改为部分排序
			int sortfri = 1000 < h_vec_reco_uid.size() ? 1000 : h_vec_reco_uid.size();
			std::partial_sort(h_vec_reco_uid.begin(), h_vec_reco_uid.begin() + sortfri,
					h_vec_reco_uid.end(), compare_reco_result);
			//std::sort(h_vec_reco_uid.begin(), h_vec_reco_uid.end(), compare_reco_result);
			//tv_temp = calc_spend_time(tv_temp, "mget hufen of hufen", spend_msec, false);	

			//批量获取关注的关注
			MAP_RECO_UID g_map_reco_uid;	// 存储基于关注的关注的推荐<uid,理由>
			VEC_RECO_UID g_vec_reco_uid;	// 存储基于关注的关注的推荐uid集合
			get_indirect_func( g_map_reco_uid, g_vec_reco_uid, only_fol_uids, only_fol_uid_num,
					rel_uids, RECO_TYPE_GOG);
			// 20150527添加，将全排序改为部分排序
			int sortgz = 1000 < g_vec_reco_uid.size() ? 1000 : g_vec_reco_uid.size();
			std::partial_sort(g_vec_reco_uid.begin(), g_vec_reco_uid.begin() + sortgz, 
					g_vec_reco_uid.end(), compare_reco_result);
			//std::sort(g_vec_reco_uid.begin(), g_vec_reco_uid.end(), compare_reco_result);

			//tv_temp = calc_spend_time(tv_temp, "mget gz of gz", spend_msec, false);
			// 将推荐结果生成最终结果，包括过滤、格式转化等
			generate_online_result(uid, num, h_vec_reco_uid, g_vec_reco_uid,
					p_out_string, n_out_len);

			release_rel_uids(rel_uids);
			release_map_reco_uid(h_map_reco_uid);
			h_vec_reco_uid.clear();
			release_map_reco_uid(g_map_reco_uid);
			g_vec_reco_uid.clear();

			return 1;	
		}
	
		// 生成在线结果
		int generate_online_result(uint64_t uid, uint32_t num, 
				const VEC_RECO_UID& h_vec_reco_uid,
				const VEC_RECO_UID& g_vec_reco_uid,
				char* p_out_string, int& n_len){
			
			GlobalDbCompany* p_global_db_company = p_db_company_->get_global_db_company();
			if(NULL == p_global_db_company){
				LOG_ERROR("global db_company is NULL!");
				return -1;
			}
			GlobalDbInterface* global_gu_db_interface = 
				p_global_db_company->get_global_db_interface("GOOD_USER_DB");
			if(NULL == global_gu_db_interface){
				LOG_ERROR("gooduser db_interface is NULL!");
			}
			
			GlobalDbInterface* global_vu_db_interface = 
				p_global_db_company->get_global_db_interface("V_USER_DB");

			if(NULL == global_vu_db_interface){
				LOG_ERROR("global_vuser db_interface is NULL!");
			}

			__gnu_cxx::hash_set<uint64_t> hashset_filter_uids;
			get_filter_uids(uid, hashset_filter_uids);
			
			uint32_t index = 0;

			size_t h_num = h_vec_reco_uid.size();	// 好友的好友
			size_t g_num = g_vec_reco_uid.size();	// 关注的关注

			size_t h_index = 0;
			size_t g_index = 0;
	
			char* p_out_temp = p_out_string;
			int len = sprintf(p_out_temp, "%s", "{\"return_code\":\"200\",\"result\":[");
			p_out_temp = p_out_temp + len;
			n_len += len;
		
			while((h_index < h_num || g_index < g_num) && index < num){
				// 关注的关注和好友的好友结果交叉存放
				while(h_index < h_num){		// 先存放好友的好友
					RECO_UID* p_reco_uid = h_vec_reco_uid[h_index];
					h_index ++;
					if(p_reco_uid != NULL){
						uint64_t reco_uid = p_reco_uid->uid_;
						uint8_t type = p_reco_uid->type_;
						uint32_t score = p_reco_uid->score_; 
						uint16_t bri_num = p_reco_uid->bri_num_;
						uint32_t user_type = 0;
						
						// 仅保留优质用户
						if(global_gu_db_interface != NULL &&
								!global_gu_db_interface->is_exist(reco_uid)){
							continue;
						}

						// 做过滤
						__gnu_cxx::hash_set<uint64_t>::iterator fil_it = hashset_filter_uids.find(reco_uid);
						if(fil_it != hashset_filter_uids.end()){
							continue;
						}

						// 判断用户V类型
						if(global_vu_db_interface != NULL){
							// 是否是V用户
							bool flag = ((GlobalKVDbInterface*)global_vu_db_interface)->
								get_value(reco_uid, user_type);	
							if(user_type == 3)				// 过滤企业蓝v
								continue;
							else if(user_type >= 2)			// V类型>=2则认为是蓝V
								user_type = USER_TYPE_BV;
						}

						//防止推荐结果重复
						hashset_filter_uids.insert(reco_uid);
						
						if(index == 0)
							len = sprintf(p_out_temp, "{\"tid\":%"PRIu64, reco_uid);
						else
							len = sprintf(p_out_temp, ",{\"tid\":%"PRIu64, reco_uid);

						p_out_temp = p_out_temp + len;
						n_len += len;

						len = sprintf(p_out_temp, ",\"sco\":%"PRIu32, score);
						p_out_temp = p_out_temp + len;
						n_len += len;

						len = sprintf(p_out_temp, ",\"utype\":%"PRIu8, user_type);
						p_out_temp = p_out_temp + len;
						n_len += len;
						
						if(type == RECO_TYPE_HOH)
							len = sprintf(p_out_temp, ",\"hoh_bnum\":%"PRIu16, bri_num);
						else
							len = sprintf(p_out_temp, ",\"gog_bnum\":%"PRIu16, bri_num);
						p_out_temp = p_out_temp + len;
						n_len += len;

						if(type == RECO_TYPE_HOH)
							len = sprintf(p_out_temp, ",\"hoh_ext\":[");
						else
							len = sprintf(p_out_temp, ",\"gog_ext\":[");
						p_out_temp = p_out_temp + len;
						n_len += len;

						// 桥梁，最多保存两个
						for(uint16_t bri_index = 0; bri_index < (bri_num > 2 ? 2 : bri_num); bri_index ++){
							uint64_t bri_uid = p_reco_uid->bri_uids_[bri_index];

							if(bri_index == 0){
								len = sprintf(p_out_temp, "%"PRIu64, bri_uid);
							}
							else
								len = sprintf(p_out_temp, ",%"PRIu64, bri_uid);

							p_out_temp = p_out_temp + len;
							n_len += len;
						}
						len = sprintf(p_out_temp, "]}");
						p_out_temp = p_out_temp + len;
						n_len += len;

						index ++;
						break;
					}
				}

			 	if(index >= num) break;		// 若达到结果数量，退出
				while(g_index < g_num){
					RECO_UID* p_reco_uid = g_vec_reco_uid[g_index];
					g_index ++;
					if(p_reco_uid != NULL){
						uint64_t reco_uid = p_reco_uid->uid_;
						uint8_t type = p_reco_uid->type_;
						uint32_t score = p_reco_uid->score_; 
						uint16_t bri_num = p_reco_uid->bri_num_;
						uint32_t user_type = 0;

						if(global_gu_db_interface != NULL &&
								!global_gu_db_interface->is_exist(reco_uid)){
							continue;
						}
						
						__gnu_cxx::hash_set<uint64_t>::iterator fil_it = hashset_filter_uids.find(reco_uid);
						if(fil_it != hashset_filter_uids.end()){
							continue;
						}

						// 判断用户V类型
						if(global_vu_db_interface != NULL){
							// 是否是V用户
							bool flag = ((GlobalKVDbInterface*)global_vu_db_interface)->
								get_value(reco_uid, user_type);	
							if(user_type == 3)				// 过滤企业蓝v
								continue;
							else if(user_type >= 2)			// V类型>=2则认为是蓝V
								user_type = USER_TYPE_BV;
						}

						if(index == 0)
							len = sprintf(p_out_temp, "{\"tid\":%"PRIu64, reco_uid);
						else
							len = sprintf(p_out_temp, ",{\"tid\":%"PRIu64, reco_uid);

						p_out_temp = p_out_temp + len;
						n_len += len;
						
						len = sprintf(p_out_temp, ",\"sco\":%"PRIu32, score);
						p_out_temp = p_out_temp + len;
						n_len += len;

						len = sprintf(p_out_temp, ",\"utype\":%"PRIu8, user_type);
						p_out_temp = p_out_temp + len;
						n_len += len;
						
						if(type == RECO_TYPE_HOH)
							len = sprintf(p_out_temp, ",\"hoh_bnum\":%"PRIu16, bri_num);
						else
							len = sprintf(p_out_temp, ",\"gog_bnum\":%"PRIu16, bri_num);
						p_out_temp = p_out_temp + len;
						n_len += len;

						if(type == RECO_TYPE_HOH)
							len = sprintf(p_out_temp, ",\"hoh_ext\":[");
						else
							len = sprintf(p_out_temp, ",\"gog_ext\":[");
						p_out_temp = p_out_temp + len;
						n_len += len;

						for(uint16_t bri_index = 0; bri_index < (bri_num > 2 ? 2 : bri_num); bri_index ++){
							uint64_t bri_uid = p_reco_uid->bri_uids_[bri_index];

							if(bri_index == 0){
								len = sprintf(p_out_temp, "%"PRIu64, bri_uid);
							}
							else
								len = sprintf(p_out_temp, ",%"PRIu64, bri_uid);

							p_out_temp = p_out_temp + len;
							n_len += len;
						}
						len = sprintf(p_out_temp, "]}");
						p_out_temp = p_out_temp + len;
						n_len += len;

						index ++;
						break;
					}
				}
			}

			len = sprintf(p_out_temp, "]}");
			p_out_temp = p_out_temp + len;
			n_len += len;

			return 1;
		}
	
		// 使用在线计算模块计算推荐结果
		int get_online_reco_uids(uint64_t uid, uint32_t num,
				char* p_out_string, int& n_out_len){

			if(NULL == p_db_company_){
				LOG_ERROR("db_company is NULL!");
				return -1;
			}

			int spend_msec = -1;
			struct timeval tv_temp;
			tv_temp = calc_spend_time(tv_temp, "get online reco uids start", spend_msec, true);
			
			//获取关注，互粉，亲密度数
			ReqResultMap req_result_map;
			
			int fri_db_id = 6; // 互粉好友db
			int result = push_request_result_map(fri_db_id, uid, 0, 0, NULL, true, req_result_map);
			if(result <= 0)
				LOG_ERROR("create friend db is failed!");

			int fol_db_id = 3; // 关注单向db
			result = push_request_result_map(fol_db_id, uid, 0, 0, NULL, true, req_result_map);
			if(result <= 0)
				LOG_ERROR("create follow db is failed!");

			int qmd_db_id = 21; // 亲密度db
			const int intimacy_db_num = 19 + int(uid%12)%2;
			char intimacy_str[20];
			snprintf(intimacy_str, 20, "%d-%"PRIu64, intimacy_db_num, uid);
			result = push_request_result_map(qmd_db_id, uid, 0, 0, intimacy_str, false, req_result_map);
			if(result <= 0)
				LOG_ERROR("create qmd db is failed!");
			
			if(p_db_company_->get_multi_db(req_result_map) > 0){
				char split_char, second_split_char;
				__gnu_cxx::hash_set<uint64_t> unique_uids;		// 使用set保证uid不重复

				// 获取好友数据
				char* p_fri_result = NULL;
				get_data_from_req_result_map(fri_db_id, req_result_map,
							p_fri_result, split_char, second_split_char);
				uint64_t fri_uids[MAX_FOL_NUM];
				uint32_t fri_id_num = 0;
				if(p_fri_result != NULL){
					split_string_ids(fri_uids, fri_id_num, p_fri_result, split_char, MAX_FOL_NUM);
					/* 2014/04/15互粉数据不存在重复问题，暂时不用修改
					split_string_set_ids(unique_uids, fri_id_num, 
							(const char* &)p_fri_result, split_char, MAX_FOL_NUM);
					fri_id_num = 0;
					for(__gnu_cxx::hash_set<uint64_t>::iterator it = unique_uids.begin();
							it != unique_uids.end(); ++ it){
						fri_uids[fri_id_num++] = *it;
					}
					unique_uids.clear();*/
				}

				// 获取关注数据
				char* p_fol_result = NULL;
				get_data_from_req_result_map(fol_db_id, req_result_map,
								p_fol_result, split_char, second_split_char);
				uint32_t fol_id_num = 0;
				if(p_fol_result != NULL){
					//2014/04/15修改，关注数据可能存在重复，必须先生成set再转数组，否则容易越界
					split_string_set_ids(unique_uids, fol_id_num, 
							(const char* &)p_fol_result, split_char, MAX_FOL_NUM);
				}else{// 从平台获取关注
					get_follow_from_API(uid, unique_uids, MAX_FOL_NUM);
				}
				uint64_t fol_uids[MAX_FOL_NUM];
				fol_id_num = 0;
				for(__gnu_cxx::hash_set<uint64_t>::iterator it = unique_uids.begin(); 
						it != unique_uids.end(); ++ it){	// set转成数组，方便后续使用
					fol_uids[fol_id_num++] = *it;
				}

				// 获取亲密度数据
				char* p_qmd_result = NULL;
				get_data_from_req_result_map(qmd_db_id, req_result_map,
								p_qmd_result, split_char, second_split_char);
				vector<PAIR_INT_FLOAT> qmd_uids;
				if(p_qmd_result != NULL){
					generate_intimacy_data(p_qmd_result, qmd_uids);
				}
				std::sort(qmd_uids.begin(), qmd_uids.end(), compare_pair_int_float);

				tv_temp = calc_spend_time(tv_temp, "online get", spend_msec);

				get_rel_uids(uid, num, fri_uids, fri_id_num, 
						fol_uids, fol_id_num, qmd_uids, 
						p_out_string, n_out_len);

			}

			//must release to aviod mem-leak
			release_request_result_map(req_result_map);
			tv_temp = calc_spend_time(tv_temp, "online finish", spend_msec);
			return 1;
		}

		// 根据数据结果获取亲密度结果 
		int generate_intimacy_data(const char* p_qmd_result, vector<PAIR_INT_FLOAT>& qmd_uids){
			const char* p_result = p_qmd_result;
		    // 解析亲密度结果
		    uint32_t res_num;
		    memcpy(&res_num, p_result, sizeof(int));                        // 前四个字节表示结果数量
		    res_num = transformatUInt32(res_num);
		    p_result += sizeof(res_num);
		    for(int i = 0; i < int(res_num); ++ i){  
		    	uint64_t tid; 
		        memcpy(&tid, p_result, sizeof(uint64_t));                   // 八个字节表示用户id
		        tid = transformatUInt64(tid);
		        p_result += sizeof(uint64_t);
		        p_result += sizeof(char);                                   // 一个字节表示关系，不使用，跳过
		        uint32_t score;
		        memcpy(&score, p_result, sizeof(uint32_t));                 // 四个字节表示亲密度得分
		        score = transformatUInt32(score);
		        p_result += sizeof(uint32_t);
				// 新的亲密度数值是乘过10000的，为了不溢出，这里除以10000，后续会乘以100W
				qmd_uids.push_back(make_pair(tid, float(score/10000)));	
			}
			return 1;
		}

		

};
#endif
