#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -x

# extract snapshot jar
cd /home/maxgraph && tar zxvf 0.0.1-SNAPSHOT.tar.gz

unset HADOOP_HOME
unset HADOOP_CONF_DIR

# depends on hadoop
export HADOOP_USER_NAME=admin

JAVA_OPT="-server -verbose:gc -Xloggc:./gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -Djava.awt.headless=true -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000 -XX:+DisableExplicitGC -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=75 -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Dlogfilename=./logs/maxgraph-frontend.log -Dlogbasedir=/home/maxgraph/logs/ -Dlog4j.configurationFile=file:./0.0.1-SNAPSHOT/conf/log4j2.xml -classpath ./0.0.1-SNAPSHOT/conf/*:./0.0.1-SNAPSHOT/lib/*:"

# start app
cd /home/maxgraph
mkdir -p /home/maxgraph/logs

# wait for yarn ready1
sleep 10

set +x

inner_config=/home/maxgraph/config/frontend.application.properties
common_env=${STANDALONE}
if [ -z "$common_env" ]; then
    echo "Can not find env STANDALONE"
else
    echo $common_env | awk -F";" '{for(i=1; i<=NF; i++){print $i;}}' > $inner_config
fi
cat $inner_config

java ${JAVA_OPT} com.alibaba.maxgraph.frontendservice.FrontendServiceMain $inner_config $object_id 1>./logs/maxgraph-frontend.out 2>./logs/maxgraph-frontend.err
