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
server_id=$2
export VINEYARD_IPC_SOCKET=$3
zookeeper_port=$4
graph_name=$5

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
WORKSPACE=$SCRIPT_DIR/../

export object_id
source $SCRIPT_DIR/common.sh
# export LOG_DIRS is must
export LOG_DIRS=$LOG_DIR

inner_config=$CONFIG_DIR/executor.local.vineyard.properties
cp $WORKSPACE/config/executor.local.vineyard.properties.tpl $inner_config
sed -i "s/VINEYARD_OBJECT_ID/$object_id/g" $inner_config
sed -i "s/ZOOKEEPER_PORT/$zookeeper_port/g" $inner_config

server_id=1

export flag="maxgraph"$object_id"executor"
RUST_BACKTRACE=full $WORKSPACE/bin/gaia_executor --config $inner_config $flag $server_id $graph_name 1>> $LOG_DIR/gaia-executor.out 2>> $LOG_DIR/gaia-executor.err &

echo $! >> $PID_DIR/executor.pid
