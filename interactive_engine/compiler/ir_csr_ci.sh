#!/bin/bash
base_dir=$(cd $(dirname $0); pwd)
# start engine service and load modern graph
cd ${base_dir}/../executor/ir/target/release &&
RUST_LOG=info CSR_PATH=/tmp/gstest/modern_graph_csr_bin PARTITION_ID=0 ./start_rpc_server_csr --config ${base_dir}/../executor/ir/integrated/config &
sleep 5s
# start compiler service
cd ${base_dir} && make run graph.store=csr &
sleep 5s
# run gremlin standard tests
cd ${base_dir} && make gremlin_test
exit_code=$?
# clean service
ps -ef | grep "com.alibaba.graphscope.GraphServer" | grep -v grep | awk '{print $2}' | xargs kill -9 || true
ps -ef | grep "start_rpc_server" | grep -v grep | awk '{print $2}' | xargs kill -9
# report test result
if [ $exit_code -ne 0 ]; then
    echo "ir integration test on experimental store fail"
    exit 1
fi
