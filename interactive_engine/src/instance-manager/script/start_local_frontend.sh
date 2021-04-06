#!/bin/sh
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

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
WORKSPACE=$SCRIPT_DIR/../

LOG_DIR=$WORKSPACE/logs/frontend/frontend_${object_id}
mkdir -p $LOG_DIR

JAVA_OPT="-server -verbose:gc -Xloggc:./gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -Djava.awt.headless=true -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000 -XX:+DisableExplicitGC -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=75 -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Dlogfilename=${LOG_DIR}/maxgraph-frontend.log -Dlogbasedir=/home/maxgraph/logs/frontend -Dlog4j.configurationFile=file:$WORKSPACE/0.0.1-SNAPSHOT/conf/log4j2.xml -classpath $WORKSPACE/0.0.1-SNAPSHOT/conf/*:$WORKSPACE/0.0.1-SNAPSHOT/lib/*:"

REPLACE_SCHEMA_PATH=`echo ${schema_path//\//\\\/}`

cp $WORKSPACE/config/frontend.local.vineyard.properties.tpl $WORKSPACE/config/frontend.local.vineyard.properties
inner_config=$WORKSPACE/config/frontend.local.vineyard.properties
sed -i "s/VINEYARD_SCHEMA_PATH/${REPLACE_SCHEMA_PATH}/g" $inner_config

# cd ./src/frontend/frontendservice/target/classes/
cd $WORKSPACE/frontend/frontendservice/target/classes/

java ${JAVA_OPT} com.alibaba.maxgraph.frontendservice.FrontendServiceMain $inner_config $object_id 1>$LOG_DIR/maxgraph-frontend.out 2>$LOG_DIR/maxgraph-frontend.err &

echo $! > $WORKSPACE/pid/frontend_${object_id}.pid 
