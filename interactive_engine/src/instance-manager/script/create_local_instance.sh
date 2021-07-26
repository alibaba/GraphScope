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
server_id=$3
VINEYARD_IPC_SOCKET=$4
zookeeper_port=$5
gaia_zookeeper_port=$6

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)

export object_id
source $SCRIPT_DIR/common.sh
mkdir -p $LOG_DIR $CONFIG_DIR $PID_DIR

bash ${SCRIPT_DIR}/start_local_coordinator.sh $object_id $zookeeper_port
sleep 1
bash ${SCRIPT_DIR}/start_local_frontend.sh $object_id $schema_path $zookeeper_port
sleep 1
bash ${SCRIPT_DIR}/start_local_executor.sh $object_id $server_id $VINEYARD_IPC_SOCKET $zookeeper_port

if [ $ENABLE_GAIA ];then
  bash ${SCRIPT_DIR}/start_local_coordinator.sh $object_id $gaia_zookeeper_port
  sleep 1
  bash ${SCRIPT_DIR}/start_local_gaia_frontend.sh $object_id $schema_path $gaia_zookeeper_port
  sleep 1
  bash ${SCRIPT_DIR}/start_local_gaia_executor.sh $object_id $server_id $VINEYARD_IPC_SOCKET $gaia_zookeeper_port
fi
