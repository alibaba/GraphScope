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

set -x

object_id=$1

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
WORKSPACE=$SCRIPT_DIR/../

coordinator_id=`cat $WORKSPACE/pid/coordinator_$object_id.pid`
frontend_id=`cat $WORKSPACE/pid/frontend_$object_id.pid`
executor_id=`cat $WORKSPACE/pid/executor_$object_id.pid`

sudo kill -9 $coordinator_id || true
sudo kill -9 $frontend_id || true
sudo kill -9 $executor_id || true
