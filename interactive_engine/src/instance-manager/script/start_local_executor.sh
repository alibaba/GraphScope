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
server_id=$2
export VINEYARD_IPC_SOCKET=$3

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
WORKSPACE=$SCRIPT_DIR/../

export LOG_DIR=$WORKSPACE/logs/executor/executor_${object_id}
mkdir -p $LOG_DIR

cp $WORKSPACE/config/executor.local.vineyard.properties.tpl $WORKSPACE/config/executor.local.vineyard.properties
inner_config=$WORKSPACE/config/executor.local.vineyard.properties
sed -i "s/VINEYARD_OBJECT_ID/$object_id/g" $inner_config

server_id=1

export flag="maxgraph"$object_id"executor"
RUST_BACKTRACE=full $WORKSPACE/bin/executor --config $inner_config $flag $server_id 1>> $LOG_DIR/maxgraph-executor.out 2>> $LOG_DIR/maxgraph-executor.err &

echo $! > $WORKSPACE/pid/executor_${object_id}.pid 
