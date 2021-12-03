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
cd ../compiler/
mvn clean install -DskipTests
nohup java -cp .:gremlin-server-plugin/target/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar:gremlin-server-plugin/target/classes com.alibaba.graphscope.gaia.GremlinServiceMain &
sleep 10
echo "Generate binary plans for benchmark, and the temp binary plan path is $1"
cd benchmark-tool/
mkdir -p $1
java -cp .:./target/benchmark-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GeneratePlanBinaryTool ../../gremlin_core/resource/benchmark/queries ".txt" $1 "" 0.0.0.0 8182
# kill gremlin server
ID=$(ps -ef | grep "GremlinServiceMain" | grep -v "$0" | grep -v "grep" | awk '{print $2}')
for id in $ID
do
kill -9 $id
echo "killed $id"
done
