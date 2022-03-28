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

CURR_DIR=$(pwd)

BIN_DIR=$(dirname $0)
cd $BIN_DIR/../
BASE_DIR=$(pwd)

CONF_DIR=$BASE_DIR/config/interactive-benchmark.properties
LIB_DIR=$BASE_DIR/lib

JAVA_CLASSPATH="."
for libfile in $LIB_DIR; do
    JAVA_CLASSPATH=$JAVA_CLASSPATH":$LIB_DIR/$libfile"
done

java -cp $JAVA_CLASSPATH com.alibaba.graphscope.gaia.benchmark.InteractiveBenchmark $CONF_DIR

cd $CURR_DIR
