#!/usr/bin/bash

ip_file=$1

# verify code
# cmd="ls /data1/yujie6/Retrieval_release"

# check server
cmd="tail -50 /data1/yujie6/Retrieval_release/log/lab_common.log"

for ip in `cat $ip_file`;
do
    echo "========== $ip begin  =========="
    sshpass -p h2U^Xap6nY ssh -o ConnectTimeout=5 -o ConnectionAttempts=1 -oStrictHostKeyChecking=no root@$ip $cmd
    echo "========== $ip finish =========="
done
wait
