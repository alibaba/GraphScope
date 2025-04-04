#!/bin/bash
base_dir=$(cd $(dirname $0); pwd)
# clean service first
ps -ef | grep "com.alibaba.graphscope.GraphServer" | awk '{print $2}' | xargs kill -9 || true
ps -ef | grep "start_rpc_server" |  awk '{print $2}' | xargs kill -9
sleep 3
# Test1: run advanced tests (pattern & ldbc) on experimental store via ir-core
# start engine service and load ldbc graph with sf=0.1
cd ${base_dir}/../executor/ir/target/release &&
RUST_LOG=info DATA_PATH=/tmp/gstest/ldbc_graph_exp_bin PARTITION_ID=0 ./start_rpc_server --config ${base_dir}/../executor/ir/integrated/config/distributed/server_0 &
cd ${base_dir}/../executor/ir/target/release &&
RUST_LOG=info DATA_PATH=/tmp/gstest/ldbc_graph_exp_bin PARTITION_ID=1 ./start_rpc_server --config ${base_dir}/../executor/ir/integrated/config/distributed/server_1 &
sleep 10
# start compiler service
cd ${base_dir} && make run graph.schema:=../executor/ir/core/resource/ldbc_schema.json pegasus.hosts:=127.0.0.1:1234,127.0.0.1:1235 &
sleep 5
# run pattern tests
cd ${base_dir} && make pattern_test
exit_code=$?
# clean compiler service
ps -ef | grep "com.alibaba.graphscope.GraphServer" | awk '{print $2}' | xargs kill -9 || true
ps -ef | grep "start_rpc_server" |  awk '{print $2}' | xargs kill -9
sleep 3
# report test result
if [ $exit_code -ne 0 ]; then
    echo "ir integration pattern test on experimental store fail"
    exit 1
fi

# Test2: run advanced tests (pattern & ldbc & simple match) on experimental store via calcite-based ir
# start service
export ENGINE_TYPE=pegasus
cd ${base_dir}/../executor/ir/target/release &&
RUST_LOG=info DATA_PATH=/tmp/gstest/ldbc_graph_exp_bin PARTITION_ID=0 ./start_rpc_server --config ${base_dir}/../executor/ir/integrated/config/distributed/server_0 &
cd ${base_dir}/../executor/ir/target/release &&
RUST_LOG=info DATA_PATH=/tmp/gstest/ldbc_graph_exp_bin PARTITION_ID=1 ./start_rpc_server --config ${base_dir}/../executor/ir/integrated/config/distributed/server_1 &
sleep 10
cd ${base_dir} && make run graph.schema:=../executor/ir/core/resource/ldbc_schema.json gremlin.script.language.name=antlr_gremlin_calcite graph.physical.opt=proto graph.planner.opt=CBO graph.statistics=src/test/resources/statistics/ldbc1_statistics.json pegasus.hosts:=127.0.0.1:1234,127.0.0.1:1235 graph.planner.rules=FilterIntoJoinRule,FilterMatchRule,ExtendIntersectRule,ExpandGetVFusionRule &
sleep 5s
cd ${base_dir} && make pattern_test && make simple_test
exit_code=$?
# clean service
ps -ef | grep "com.alibaba.graphscope.GraphServer" | awk '{print $2}' | xargs kill -9 || true
ps -ef | grep "start_rpc_server" | awk '{print $2}' | xargs kill -9
# report test result
if [ $exit_code -ne 0 ]; then
    echo "ir\(calcite-based\) integration pattern test on experimental store fail"
    exit 1
fi
