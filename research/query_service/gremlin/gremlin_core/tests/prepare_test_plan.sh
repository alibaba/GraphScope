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

# prepare query plan
nohup java -cp .:./gremlin-server-plugin/target/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar -Dgremlin.graph.schema="conf/modern.schema.json" -Dremove_tag=$1 -Dproperty_cache=$2 com.alibaba.graphscope.gaia.GremlinServiceMain &
sleep 3
pushd benchmark-tool
java -cp .:./target/benchmark-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GeneratePlanBinaryTool ../../gremlin_core/resource/test/queries$3 ".txt" ../../gremlin_core/resource/test/query_plans "" 0.0.0.0 8182
popd
# kill gremlin server
ID=$(ps -ef | grep "GremlinServiceMain" | grep -v "$0" | grep -v "grep" | awk '{print $2}')
for id in $ID
do
kill -9 $id
echo "killed $id"
done
