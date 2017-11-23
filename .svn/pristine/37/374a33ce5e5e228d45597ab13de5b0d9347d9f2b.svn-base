#!/bin/bash
cd /usr/local/Retrieval_release/bin

echo "begin" $(date)

rsync_path="10.85.132.217::hot_data"
ip=$(/sbin/ifconfig | grep inet | head -1 | awk '{print $2}' | awk -F: '{print $2}')

data_name_arr=(hotVideo_topN.data sevenDaysHotValue discover_tag_mcn.txt discover_tag_not_mcn.txt gifHotTopN.data sevenDaysGifHotValue festival_weight.txt festival_no_weight.txt)
for data_name in ${data_name_arr[@]}
do
    rsync -v $rsync_path/$data_name ../data/
    rsync_ret=$?
    echo "rsync $data_name result" $rsync_ret
    if [[ $rsync_ret -ne 0 ]];then
        url="http://10.75.14.30:8081/add_checkpoint?check_name=service:rsync:video:$ip&check_val=$rsync_ret&check_level=ERROR&msg=$data_name"
        echo $url
        curl $url
    fi
done

global_data_arr=(HOT_VIDEO HOT_SCORE MCN_DATA NOT_MCN_DATA GIF_HOT_VIDEO GIF_HOT_SCORE FESTIVAL_WEIGHT FESTIVAL_NO_WEIGHT)
for global_data in ${global_data_arr[@]}
do
    python binserverclient.py 127.0.0.1 55556 $global_data
    sleep 5
done

echo "end" $(date)
echo ""
