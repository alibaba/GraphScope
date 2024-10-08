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

SCRIPT_DIR=$(dirname $(readlink -f $0))
CURR_DIR=${SCRIPT_DIR}/../
CONF_DIR=$CURR_DIR/config/interactive-benchmark.properties
java -cp $CURR_DIR:lib/* com.alibaba.graphscope.gaia.benchmark.InteractiveBenchmark $CONF_DIR
cd $CURR_DIR