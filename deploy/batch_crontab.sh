#!/bin/bash
# eg:
# bash ./ssh.sh 203-219.txt "echo '# yujie6 clear retrieval log' >> /var/spool/cron/root; echo '0 1,15 * * * cd /data1/yujie6/Retrieval_release/log; > lab_common.log' >> /var/spool/cron/root"
for host in `cat $1`;do
    echo "============$host==========="
    sshpass -p h2U^Xap6nY ssh  -o ConnectTimeout=5 -o ConnectionAttempts=1 -oStrictHostKeyChecking=no root@$host "$2"
done
