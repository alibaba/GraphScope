#!/bin/bash
base_dir=$(cd $(dirname $0); pwd)
# clean service first
ps -ef | grep "com.alibaba.graphscope.gremlin.service.GraphServiceMain" | awk '{print $2}' | xargs kill -9 || true
ps -ef | grep "start_rpc_server" |  awk '{print $2}' | xargs kill -9
sleep 3
# start engine service and load ldbc graph
cd ${base_dir}/../executor/ir/target/release &&
RUST_LOG=info DATA_PATH=${base_dir}/../../gstest/ldbc_graph_exp_bin PARTITION_ID=0 ./start_rpc_server --config ${base_dir}/../executor/ir/integrated/config/distributed/server_0 &
cd ${base_dir}/../executor/ir/target/release &&
RUST_LOG=info DATA_PATH=${base_dir}/../../gstest/ldbc_graph_exp_bin PARTITION_ID=1 ./start_rpc_server --config ${base_dir}/../executor/ir/integrated/config/distributed/server_1 &
sleep 10
# start compiler service
cd ${base_dir} && make run graph.schema:=../executor/ir/core/resource/ldbc_schema.json pegasus.hosts:=127.0.0.1:1234,127.0.0.1:1235 pegasus.server.num:=2 &
sleep 5
# run gremlin standard tests
cd ${base_dir} && make pattern_test
exit_code=$?
# clean service
ps -ef | grep "com.alibaba.graphscope.gremlin.service.GraphServiceMain" | awk '{print $2}' | xargs kill -9 || true
ps -ef | grep "start_rpc_server" | awk '{print $2}' | xargs kill -9
# report test result
if [ $exit_code -ne 0 ]; then
    echo "ir integration pattern test on experimental store fail"
    exit 1
fi
