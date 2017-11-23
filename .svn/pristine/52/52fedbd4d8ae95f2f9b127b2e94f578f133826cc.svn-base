#! /bin/bash
last_dt=$(date +%Y%m%d_%H -d "-1 hours")
echo -e "begin ${last_dt}\n"
mv lab_common.log lab_common_${last_dt}.log

grep 'reason' lab_common_${last_dt}.log | grep -v 'fill_hot' > no_feature.txt
grep 'JSON_DATA' lab_common_${last_dt}.log | grep 'nice_topic_cand' > nice_topic.txt
grep 'JSON_DATA' lab_common_${last_dt}.log | grep 'nice_short_interest_cand' > short_interest.txt
grep 'JSON_DATA' lab_common_${last_dt}.log | grep 'nice_long_interest_cand' > long_interest.txt

flow_item=$(wc -l no_feature.txt | awk '{print $1}')
feature_missed_item=$(awk 'BEGIN{FS=":";sum=0}{sum+=$NF}END{print sum}' no_feature.txt)

high_quality_recall=$(awk 'BEGIN{FS="nice_topic_cand\":"}{print $2}' nice_topic.txt | awk 'BEGIN{FS=",";sum=0}{sum+=$1}END{print sum}')
high_quality_request=$(wc -l nice_topic.txt | awk '{print $1}')
high_quality_return=$(awk 'BEGIN{FS="nice_topic_cand\":"}{print $2}' nice_topic.txt | awk 'BEGIN{FS=","}{if ($1 != 0) print $1}' | wc -l)

recent_interest_recall=$(awk 'BEGIN{FS="nice_short_interest_cand\":"}{print $2}' short_interest.txt | awk 'BEGIN{FS=",";sum=0}{sum+=$1}END{print sum}')
recent_interest_request=$(wc -l short_interest.txt | awk '{print $1}')
recent_interest_response=$(awk 'BEGIN{FS="nice_short_interest_cand\":"}{print $2}' short_interest.txt | awk 'BEGIN{FS=","}{if ($1 != 0) print $1}' | wc -l)

long_interest_recall=$(awk 'BEGIN{FS="nice_long_interest_cand\":"}{print $2}' long_interest.txt | awk 'BEGIN{FS=",";sum=0}{sum+=$1}END{print sum}')
long_interest_request=$(wc -l long_interest.txt | awk '{print $1}')
long_interest_response=$(awk 'BEGIN{FS="nice_long_interest_cand\":"}{print $2}' long_interest.txt | awk 'BEGIN{FS=","}{if ($1 != 0) print $1}' | wc -l)

api_time=$(date +"%Y-%m-%d %H" -d "-1 hours")
api_ts=$(date -d "$api_time" +%s)

key="video_recom:basic_data:retrieval:flow:item:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${flow_item}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:feature:missed:item:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${feature_missed_item}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:high_quality:recall:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${high_quality_recall}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:high_quality:request:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${high_quality_request}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:high_quality:return:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${high_quality_return}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:recent_interest:recall:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${recent_interest_recall}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:recent_interest:request:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${recent_interest_request}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:recent_interest:response:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${recent_interest_response}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:long_interest:recall:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${long_interest_recall}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:long_interest:request:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${long_interest_request}"
echo -e "\n$url"
curl -s $url

key="video_recom:basic_data:retrieval:long_interest:response:sample"
url="http://recom.intra.weibo.com/api/dataplat/rank/set?region=4&name=$key&time=${api_ts}&rank=${long_interest_response}"
echo -e "\n$url"
curl -s $url

echo -e "\nend ${last_dt}\n"

rm_dt=$(date +%Y%m%d_%H -d "-12 hours")
rm lab_common_${rm_dt}.log
