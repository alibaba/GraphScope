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

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
WORKSPACE=$SCRIPT_DIR/../
export object_id
source $SCRIPT_DIR/common.sh

coordinator_id=`cat $PID_DIR/coordinator.pid`
frontend_id=`cat $PID_DIR/frontend.pid`
executor_id=`cat $PID_DIR/executor.pid`

declare -a components=("coordinator" "frontend" "executor")

for component in "${components[@]}"; do

    str=$(cat $PID_DIR/${component}.pid)

    # The file may have multiple pids, each in a single line
    # This will read each line into an array
    while read -r pid; do pids+=("$pid"); done <<<"${str}"

    for pid in "${pids[@]}"; do
        sudo kill ${pid} || true
    done

done
