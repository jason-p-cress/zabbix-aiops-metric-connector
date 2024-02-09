#!/bin/bash

PID=`ps -ef |grep sevone-watson-datachannel | grep -v grep | awk '{print $2}'`
echo $PID

[[ ! -z "$PID" ]] && kill -9 $PID || echo "Not running"


