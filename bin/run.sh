cd /usr/local/Retrieval_release/bin
export LD_LIBRARY_PATH=/usr/local/lib/:$LD_LIBRARY_PATH
./saferun ./lab_common_svr ../conf/venus_Retrieval.conf >> ../log/work_interface.log 2>&1 &
