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

export cluster_type=$1
export port=$2
export instance_id=$3

BINDIR=$(cd "$(dirname "$0")";pwd)
WORKSPACE=$BINDIR/../

if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
  export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
fi

LIBPATH="."
for file in `ls ${WORKSPACE}/lib`; do
    LIBPATH=$LIBPATH":"$WORKSPACE/lib/$file
done

if [ "$cluster_type" == "local" ]; then
    INSTANCE_DIR=$GRAPHSCOPE_RUNTIME/$instance_id
    mkdir -p $INSTANCE_DIR
    echo "server.port=$port" > $INSTANCE_DIR/application_local.properties
    echo "logging.config=classpath:logback-spring.xml" >> $INSTANCE_DIR/application_local.properties
    echo "instance.createScript=$WORKSPACE/script/create_local_instance.sh" >> $INSTANCE_DIR/application_local.properties
    echo "instance.closeScript=$WORKSPACE/script/close_local_instance.sh" >> $INSTANCE_DIR/application_local.properties
    echo "instance.zookeeper.hosts=127.0.0.1:2181" >>  $INSTANCE_DIR/application_local.properties

    java -cp $LIBPATH -Dspring.config.location=$INSTANCE_DIR/application_local.properties com.alibaba.maxgraph.admin.InstanceManagerApplication
else
    java -cp $LIBPATH -Dspring.config.location=$WORKSPACE/config/application.properties com.alibaba.maxgraph.admin.InstanceManagerApplication
fi
