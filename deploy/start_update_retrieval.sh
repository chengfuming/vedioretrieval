#!/usr/bin/bash

ip_file=$1

mkfifo bufferpipe
exec 1000<>bufferpipe
rm -rf bufferpipe
for((i=0;i<10;i++))
do
    echo >&1000
done

# update server
cmd="cd /data1/yujie6/Retrieval_release; rsync -avP 10.85.9.198::down/data1/yujie6/lab_common_so_video11/deploy/update_retrieval.sh ./; sh update_retrieval.sh;"

for ip in `cat $ip_file`;
do
     read -u1000
     {
        echo "========== $ip begin  =========="
        sshpass -p h2U^Xap6nY ssh -o ConnectTimeout=5 -o ConnectionAttempts=1 -oStrictHostKeyChecking=no root@$ip $cmd
        echo "========== $ip finish =========="
        echo >&1000
     }&
done
wait
