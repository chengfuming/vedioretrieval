#!/usr/bin/bash

# kill saferun
ps aux | grep Retrieval | grep saferun | awk '{print $2}' | xargs kill
sleep 1

# kill predictor server
ps aux | grep Retrieval | grep -v grep | awk '{print $2}' | xargs kill
sleep 1

# rsync file
cd /data1/yujie6/Retrieval_release/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/bin/lab_common_svr bin/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/bin/update_hot_video.sh bin/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/conf/venus_Retrieval.conf conf/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/conf/work_config.ini conf/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/conf/db_config.ini conf/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/conf/global_db_config.ini conf/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/data/hotVideo_topN.data data/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/data/sevenDaysHotValue data/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/data/discover_tag_mcn.txt data/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/data/discover_tag_not_mcn.txt data/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/lib/VenusRetrievalWorkInterface.so lib/
rsync -avP 10.85.9.198::down/data1/yujie6/Retrieval_release/lib/DiscoverWorkInterface.so lib/

# start server
cd bin; sh run.sh
sleep 1

cd /data1/yujie6/Retrieval_release/log; tail -100 lab_common.log
