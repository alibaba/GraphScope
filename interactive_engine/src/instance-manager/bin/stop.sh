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
export instance_id=$2

if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
  export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
fi

if [ "$cluster_type" == "local" ]; then
  INSTANCE_DIR=$GRAPHSCOPE_RUNTIME/$instance_id
  graphmanager_id=`cat $INSTANCE_DIR/graphmanager.pid`
  sudo kill $graphmanager_id || true > /dev/null 2>&1
else
  jps | grep InstanceManagerApplication | awk '{print $1}' | xargs kill -9
fi
