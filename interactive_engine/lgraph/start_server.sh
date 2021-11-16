#!/bin/bash
set -x
base_dir=$(cd "$(dirname "$0")"; pwd)
ps -ef | grep "com.alibaba.maxgraph.servers.MaxNode" | grep -v grep | awk '{print $2}' | xargs kill -9
cd ${base_dir}/..
cd ./distribution/target/ && tar xvzf maxgraph.tar.gz && cd maxgraph

# start server
maxgraph_dir=$(pwd)
sed -e "s@LOG4RS_CONFIG@${maxgraph_dir}/conf/log4rs.yml@g" \
    -e "s@partition.count=8@partition.count=4@g" \
    -e "s@backup.enable=false@backup.enable=true@g" \
    -e "s@log.recycle.enable=true@log.recycle.enable=false@g" \
    conf/config.template > /tmp/max_node_backup_test.config
LOG_NAME=maxnode MAXGRAPH_CONF_FILE=/tmp/max_node_backup_test.config ./bin/store_ctl.sh max_node_maxgraph &