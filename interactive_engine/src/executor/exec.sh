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

if [ -z {$HADOOP_HOME} ]; then
  echo "HADOOP_HOME is not set."
  exit -1
fi

echo "VINEYARD_ROOT_DIR = $VINEYARD_ROOT_DIR"

current=`dirname $0`
export MAXGRAPH_HOME=`cd ${current}/../../ ;pwd`

export GCC492_HOME='/usr/ali/alicpp/built/gcc-4.9.2/'

export LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native:${GCC492_HOME}/openssl-1.0.2a/lib:$GCC492_HOME/gcc-4.9.2/lib64

HADOOP_MODULE_DIRS="${HADOOP_HOME}/share/hadoop/common/lib/
${HADOOP_HOME}/share/hadoop/common/
${HADOOP_HOME}/share/hadoop/hdfs/
${HADOOP_HOME}/share/hadoop/hdfs/lib/
${HADOOP_HOME}/share/hadoop/yarn/lib/
${HADOOP_HOME}/share/hadoop/yarn/"

HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
CLASSPATH="${HADOOP_CONF_DIR}"

for d in ${HADOOP_MODULE_DIRS}; do
  for j in $d/*.jar; do
    CLASSPATH=${CLASSPATH}:${j}
  done;
done;

export CLASSPATH
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${JAVA_HOME}/jre/lib/amd64/server

$@
