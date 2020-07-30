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

if [ -z ${HADOOP_COMMON_HOME} ]; then
  echo "${HADOOP_COMMON_HOME} is not set."
  exit -1
fi
export LD_LIBRARY_PATH=${HADOOP_COMMON_HOME}/lib/native

HADOOP_MODULE_DIRS="${HADOOP_COMMON_HOME}/share/hadoop/common/lib/
${HADOOP_COMMON_HOME}/share/hadoop/common/
${HADOOP_COMMON_HOME}/share/hadoop/hdfs/
${HADOOP_COMMON_HOME}/share/hadoop/hdfs/lib/
${HADOOP_COMMON_HOME}/share/hadoop/yarn/lib/
${HADOOP_COMMON_HOME}/share/hadoop/yarn/"

HADOOP_CONF_DIR=${HADOOP_COMMON_HOME}/etc/hadoop
CLASSPATH="${HADOOP_CONF_DIR}"

for d in ${HADOOP_MODULE_DIRS}; do
  for j in $d/*.jar; do
    CLASSPATH=${CLASSPATH}:${j}
  done;
done;

export CLASSPATH

jvmLibPath=${JAVA_HOME}/jre
if [ ! -d "${jvmLibPath}" ]
then
    jvmLibPath=${JAVA_HOME}
fi

SCRIPT_DIR=`dirname "${BASH_SOURCE-$0}"`
BIN_DIR=`cd "${SCRIPT_DIR}/../bin"; pwd`
NATIVE_DIR=`cd "${SCRIPT_DIR}/../native"; pwd`
LIB_DIR=`cd "${SCRIPT_DIR}/../lib"; pwd`
CONF_DIR=`cd "${SCRIPT_DIR}/../conf"; pwd`

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${jvmLibPath}/lib/amd64/server:${NATIVE_DIR}:/usr/ali/alicpp/built/gcc-4.9.2/openssl-1.0.2a/lib
export LD_LIBRARY_PATH=/usr/ali/alicpp/built/gcc-4.9.2/gcc-4.9.2/lib64:${LD_LIBRARY_PATH}

hadoopHome=${HADOOP_COMMON_HOME}

JAVA_OPT="-server -Xmx3277m -Xms3277m -verbose:gc -Xloggc:./gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -Djava.awt.headless=true -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000 -XX:+DisableExplicitGC -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=75 -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Dlogfilename=./maxgraph-lambda-server.log -Dlogbasedir=. -Dlog4j.configurationFile=file:${CONF_DIR}/log4j2.xml -classpath ${CONF_DIR}/*:${LIB_DIR}/*:"

logDir=""
if [ "${LOG_DIRS}" != "" ]
then
    logDir="${LOG_DIRS}"
else
    logDir="."
    export LOG_DIRS="./"
fi

#Start Lambda Server
#/opt/taobao/java/bin/java ${JAVA_OPT} com.alibaba.maxgraph.lambda.MaxGraphLambdaServer $$ 1>./maxgraph-lambda-server.out 2>./maxgraph-lambda-server.err &
#sleep 5

echo -e "\n\n\n\n\n\n\n\n\n=======================================================\n\n\n\n\n\n\n\n\n" >> ${logDir}/maxgraph-executor.out
echo -e "\n\n\n\n\n\n\n\n\n=======================================================\n\n\n\n\n\n\n\n\n" >> ${logDir}/maxgraph-executor.err
RUST_BACKTRACE=1 ${BIN_DIR}/executor $@ 1>> ${logDir}/maxgraph-executor.out 2>> ${logDir}/maxgraph-executor.err


#RUST_BACKTRACE=1 CONF_DIR=${CONF_DIR} ${JAVA_HOME}/bin/java -Djna.library.path=${NATIVE_DIR} -cp ${LIB_DIR}/store-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibaba.maxgraph.store.StoreMain $@ 1>> ${logDir}/maxgraph-executor.out 2>>${logDir}/maxgraph-executor.err
#RUST_BACKTRACE=1 CONF_DIR=${CONF_DIR} ${JAVA_HOME}/bin/java -Djna.library.path=${NATIVE_DIR} -cp "${LIB_DIR}/*" com.alibaba.maxgraph.store.StoreMain $@ 1>> ${logDir}/maxgraph-executor.out 2>>${logDir}/maxgraph-executor.err
