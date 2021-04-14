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
zookeeper_port=$2

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
WORKSPACE=$SCRIPT_DIR/../
export object_id
source $SCRIPT_DIR/common.sh

JAVA_OPT="-server -Xmx1024m -Xms1024m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./java.hprof -verbose:gc -Xloggc:${LOG_DIR}/maxgraph-coordinator.gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -Djava.awt.headless=true -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000 -XX:+DisableExplicitGC -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=75 -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Dlogfilename=${LOG_DIR}/maxgraph-coordinator.log -Dlogbasedir=${LOG_DIR}/coordinator -Dlog4j.configurationFile=file:${WORKSPACE}/0.0.1-SNAPSHOT/conf/log4j2.xml -classpath ${WORKSPACE}/0.0.1-SNAPSHOT/conf/*:${WORKSPACE}/0.0.1-SNAPSHOT/lib/*:"


inner_config=$CONFIG_DIR/coordinator.local.application.properties
cp $WORKSPACE/config/coordinator.local.application.properties.tpl $inner_config
sed -i "s/ZOOKEEPER_PORT/$zookeeper_port/g" $inner_config

cd $WORKSPACE/coordinator/target/classes/

java ${JAVA_OPT} com.alibaba.maxgraph.coordinator.CoordinatorMain $inner_config $object_id 1>$LOG_DIR/maxgraph-coordinator.out 2>$LOG_DIR/maxgraph-coordinator.err &

echo $! > $PID_DIR/coordinator.pid
