#!/bin/bash

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

worker_num="pegasus.worker.num: $WORKER_NUM";

timeout="pegasus.timeout: $TIMEOUT"

batch_size="pegasus.batch.size: $BATCH_SIZE";

output_capacity="pegasus.output.capacity: $OUTPUT_CAPACITY";

hosts="pegasus.hosts: $DNS_NAME_PREFIX_STORE:$GAIA_RPC_PORT";

hosts="${hosts/"{}"/0}";

gremlin_server_port="gremlin.server.port: $GREMLIN_SERVER_PORT";

cypher_server_port="neo4j.bolt.server.port: $CYPHER_SERVER_PORT";

frontend_query_per_second_limit="frontend.query.per.second.limit: $FRONTEND_QUERY_PER_SECOND_LIMIT"

gremlin_script_language_name="gremlin.script.language.name: $GREMLIN_SCRIPT_LANGUAGE_NAME"

count=1;
while (($count<$SERVERSSIZE))
do
    host=",$DNS_NAME_PREFIX_STORE:$GAIA_RPC_PORT"
    host="${host/"{}"/$count}";
    hosts+=$host
    (( count++ ))
done

graph_schema="graph.schema: $GRAPH_SCHEMA"

properties="$worker_num\n$timeout\n$batch_size\n$output_capacity\n$hosts\n$server_num\n$graph_schema\n$gremlin_server_port\n$cypher_server_port\n$frontend_query_per_second_limit\n$gremlin_script_language_name"

echo -e $properties > ./conf/ir.compiler.properties
