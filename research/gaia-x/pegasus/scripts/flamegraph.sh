#!/bin/sh
# Copyright 2020 Alibaba Group Holding Limited.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# export FLAME_GRAPH_HOME="your flame graph directory";
# get the flame graph from the github repository: https://github.com/brendangregg/FlameGraph

if [ $FLAME_GRAPH_HOME ]; then
    echo "Use FlameGraph in $FLAME_GRAPH_HOME"
else
    echo "FLAME_GRAPH_HOME not set"
    exit 1;
fi

cmd=$*;

sudo -E perf record -F 120 -g --call-graph dwarf -- $cmd;
sudo perf script > out.perf
$FLAME_GRAPH_HOME/stackcollapse-perf.pl out.perf > out.fold
$FLAME_GRAPH_HOME/flamegraph.pl out.fold  > flamegraph.svg

