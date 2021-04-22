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

if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
  export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
fi

if [ -n "$object_id" ]; then
  export LOG_DIR=$GRAPHSCOPE_RUNTIME/logs/$object_id
  export CONFIG_DIR=$GRAPHSCOPE_RUNTIME/config/$object_id
  export PID_DIR=$GRAPHSCOPE_RUNTIME/pid/$object_id
fi
