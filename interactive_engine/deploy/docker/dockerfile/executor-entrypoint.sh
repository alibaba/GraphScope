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

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/apsara/alicpp/built/gcc-4.9.2/gcc-4.9.2/lib64/:/apsara/alicpp/built/gcc-4.9.2/openssl-1.0.2a/lib/:/usr/local/hadoop-2.8.4/lib/native/:/usr/local/jdk1.8.0_191/jre/lib/amd64/server/:/usr/local/hadoop-2.8.4/lib/native/:/usr/local/lib64

export HADOOP_HOME=/usr/local/hadoop-2.8.4
export HADOOP_CLASSPATH=.
for f in ${HADOOP_HOME}/share/hadoop/common/hadoop-*.jar; do
        HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
done
for f in $HADOOP_HOME/share/hadoop/common/lib/*.jar; do
        HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
done

for f in $HADOOP_HOME/share/hadoop/mapreduce/hadoop-*.jar; do
        HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
done
for f in $HADOOP_HOME/share/hadoop/hdfs/hadoop-*.jar; do
        HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
done

export CLASSPATH=.:$HADOOP_CLASSPATH:$CLASSPATH

export

# start app
cd /home/maxgraph
mkdir -p /home/maxgraph/logs_$object_id
export LOG_DIRS=/home/maxgraph/logs_$object_id

inner_config=/home/maxgraph/config_$object_id/executor.application.properties
# /home/maxgraph/config/executor.application.properties
common_env=${STANDALONE}
if [ -z "$common_env" ]; then
    echo "Can not find env STANDALONE"
else
    echo $common_env | awk -F";" '{for(i=1; i<=NF; i++){print $i;}}' > $inner_config
fi

cat $inner_config

#server_id=$RANDOM
flag="maxgraph"$object_id"executor"
RUST_BACKTRACE=full /home/maxgraph/executor --config $inner_config $flag $server_id 1>> logs_$object_id/maxgraph-executor.out 2>> logs_$object_id/maxgraph-executor.err &
