#!/bin/bash
set -x
base_dir=$(cd "$(dirname "$0")"; pwd)
ps -ef | grep "com.alibaba.graphscope.servers.MaxNode" | grep -v grep | awk '{print $2}' | xargs kill -9
cd ${base_dir}/..
cd ./assembly/target/ && tar xvzf groot.tar.gz && cd groot

# start server
groot_dir=$(pwd)
sed -e "s@LOG4RS_CONFIG@${groot_dir}/conf/log4rs.yml@g" \
    -e "s@partition.count=8@partition.count=4@g" \
    -e "s@backup.enable=false@backup.enable=true@g" \
    -e "s@log.recycle.enable=true@log.recycle.enable=false@g" \
    conf/config.template > /tmp/max_node_backup_test.config
LOG_NAME=maxnode GROOT_CONF_FILE=/tmp/max_node_backup_test.config ./bin/store_ctl.sh start_max_node &