#!/usr/bin/bash
date
RETRIEVAL_PID=$(ps aux | grep Retrieval | grep conf | grep -v saferun | grep -v grep | grep -v vim | grep -v tail | awk '{print $2}')
echo $RETRIEVAL_PID
kill -9 $RETRIEVAL_PID
