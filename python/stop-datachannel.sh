#!/bin/bash

PID=`ps -ef |grep zabbix-aiops-metric-connector | grep -v grep | awk '{print $2}'`
echo $PID

[[ ! -z "$PID" ]] && kill -9 $PID || echo "Not running"


