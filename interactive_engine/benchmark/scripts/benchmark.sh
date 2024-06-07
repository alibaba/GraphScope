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

# Default class to run
CLASS_TO_RUN=com.alibaba.graphscope.gaia.benchmark.InteractiveBenchmark
if [ "$1" = "--cypher" ]; then
    CLASS_TO_RUN=com.alibaba.graphscope.gaia.benchmark.CypherBenchmark
elif [ "$1" = "--gremlin" ]; then
    CLASS_TO_RUN=com.alibaba.graphscope.gaia.benchmark.InteractiveBenchmark
fi

cd "$(dirname "$0")"
cd ../target
tar -xf gaia-benchmark-0.0.1-SNAPSHOT-dist.tar.gz
cd gaia-benchmark-0.0.1-SNAPSHOT
CONF_DIR=./config/interactive-benchmark.properties
java -cp '.:lib/*' $CLASS_TO_RUN "$CONF_DIR"
cd $CURR_DIR