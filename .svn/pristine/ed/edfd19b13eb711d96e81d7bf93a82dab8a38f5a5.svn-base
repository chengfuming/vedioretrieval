#!/usr/bin/bash

ip_file=$1

mkfifo bufferpipe
exec 1000<>bufferpipe
rm -rf bufferpipe
for((i=0;i<10;i++))
do
    echo >&1000
done

#cmd="echo 'net.ipv4.tcp_timestamps = 1' >> /etc/sysctl.conf; echo 'net.ipv4.tcp_tw_reuse = 1' >> /etc/sysctl.conf; echo 'net.ipv4.tcp_tw_recycle' = 1 >> /etc/sysctl.conf; echo 'net.ipv4.tcp_fin_timeout = 30' >> /etc/sysctl.conf; echo 'net.ipv4.tcp_syncookies = 1' >> /etc/sysctl.conf; /sbin/sysctl -p"

# checkout code
cmd="mkdir -p /data1/yujie6/; cd /data1/yujie6/; svn checkout \"https://svn.intra.sina.com.cn/searchengine/labs/recomgroup/branches/Retrieval_release\" --username yujie6 --password xxoo --force --no-auth-cache;"

# start server
cmd="cd /data1/yujie6/Retrieval_release/bin/; sh run.sh"

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
