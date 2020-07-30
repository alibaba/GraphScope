#!/usr/bin/env bash
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
export JAVA_HOME="/opt/taobao/java"
if [ "${JAVA_HOME}" = "" ]
then
    echo "JAVA_HOME not found"
    exit 1
fi

SCRIPT_DIR=`dirname "${BASH_SOURCE-$0}"`
BASE_DIR=`cd "${SCRIPT_DIR}/.."; pwd`
LIB_DIR=`cd "${SCRIPT_DIR}/../lib"; pwd`
CONF_DIR=`cd "${SCRIPT_DIR}/../conf"; pwd`
mkdir -p ${BASE_DIR}/logs
LOG_DIR=`cd "${BASE_DIR}/logs"; pwd`
SMOKE_CONFIG_FILE="${CONF_DIR}/smoke.properties"

latestPackageUrl=`/apsara/deploy/rpc_wrapper/rpc.sh pl | grep package | grep SNAPSHOT | awk '{print $1}' | sort | tail -n 1`
latestPackageName=`echo $latestPackageUrl | awk -F '//' '{print $2}'`

# 替换配置文件
sed -i "s#yarn.hdfs.package.path=.*#yarn.hdfs.package.path=${latestPackageUrl}#g" ${SMOKE_CONFIG_FILE}
sed -i "s#yarn.package.dir.name=.*#yarn.package.dir.name=${latestPackageName}#g" ${SMOKE_CONFIG_FILE}

function smoke
{
    JAVA_OPTS="${JAVA_OPTS} -client"
    JAVA_OPTS="${JAVA_OPTS} -Dlog4j.configurationFile=file:${CONF_DIR}/log4j2.xml -Dlogfilename=${LOG_DIR}/maxgraph-tool.log -Dlogbasedir=${LOG_DIR}"

    ${JAVA_HOME}/bin/java ${JAVA_OPTS} -cp "${CONF_DIR}:${LIB_DIR}/*" com.alibaba.maxgraph.tools.ToolMain -a smoke_test -c ${SMOKE_CONFIG_FILE} 1>"${LOG_DIR}/maxgraph-tool.out" 2>"${LOG_DIR}/maxgraph-tool.err"
}

smoke
cat "${LOG_DIR}/maxgraph-tool.out" | grep collection_flag
