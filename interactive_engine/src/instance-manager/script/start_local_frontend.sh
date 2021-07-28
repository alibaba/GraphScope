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

object_id=$1
schema_path=$2
zookeeper_port=$3

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
WORKSPACE=$SCRIPT_DIR/../
export object_id
source $SCRIPT_DIR/common.sh

JAVA_OPT="-server -verbose:gc -Xloggc:${LOG_DIR}/maxgraph-frontend.gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -Djava.awt.headless=true -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000 -XX:+DisableExplicitGC -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=75 -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Dlogfilename=${LOG_DIR}/maxgraph-frontend.log -Dlogbasedir=${LOG_DIR}/frontend -Dlog4j.configurationFile=file:$WORKSPACE/0.0.1-SNAPSHOT/conf/log4j2.xml -classpath $WORKSPACE/0.0.1-SNAPSHOT/conf/*:$WORKSPACE/0.0.1-SNAPSHOT/lib/*:"

REPLACE_SCHEMA_PATH=`echo ${schema_path//\//\\\/}`

inner_config=$CONFIG_DIR/frontend.local.vineyard.properties
cp $WORKSPACE/config/frontend.local.vineyard.properties.tpl $inner_config
sed -i "s/VINEYARD_SCHEMA_PATH/$REPLACE_SCHEMA_PATH/g" $inner_config
sed -i "s/ZOOKEEPER_PORT/$zookeeper_port/g" $inner_config
sed -i "s/graphname/${object_id}/g" $inner_config

cd $WORKSPACE/frontend/frontendservice/target/classes/

java ${JAVA_OPT} com.alibaba.maxgraph.frontendservice.FrontendServiceMain $inner_config $object_id 1>$LOG_DIR/maxgraph-frontend.out 2>$LOG_DIR/maxgraph-frontend.err &

timeout_seconds=60
wait_period_seconds=0

while true
do
  gremlin_server_port=`awk '/frontend host/ { print }' ${LOG_DIR}/maxgraph-frontend.log | awk -F: '{print $6}'`
  gremlin_server_port2=`awk '/frontend host/ { print }' ${LOG_DIR}/maxgraph-frontend.err | awk -F: '{print $4}'`
  if [ -n "$gremlin_server_port" ]; then
    echo "FRONTEND_PORT:127.0.0.1:$gremlin_server_port"
    break
  fi
  if [ -n "$gremlin_server_port2" ]; then
    echo "FRONTEND_PORT:127.0.0.1:$gremlin_server_port2"
    break
  fi
  wait_period_seconds=$(($wait_period_seconds+5))
  if [ ${wait_period_seconds} -gt ${timeout_seconds} ];then
    echo "Get external ip of ${GREMLIN_EXPOSE} failed."
    break
  fi
  sleep 5
done

echo $! > $PID_DIR/frontend.pid
