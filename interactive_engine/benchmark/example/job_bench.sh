#!/bin/bash

set -x
BASE_DIR=$(cd "$(dirname "$0")"; pwd)
CURR_DIR=$(pwd)
declare -r CONFIG_FILE="${CURR_DIR}/job_benchmark.properties"
declare -r DATA_DIR="/tmp/bench_data"
declare -r GIE_COMPILER_DIR=${CURR_DIR}/../../compiler
declare -r GIE_ENGINE_DIR=${CURR_DIR}/../../executor/ir
declare -r KUZU_DB_DIR=${CURR_DIR}/../dbs/kuzu


# download binary data for GIE
mkdir -p ${DATA_DIR}
curl -o ${DATA_DIR}/gie_imdb_bin.tar.gz -L "https://graphscope.oss-cn-beijing.aliyuncs.com/gopt_data/imdb_bin.tar.gz"
cd ${DATA_DIR} && tar xvzf gie_imdb_bin.tar.gz
# start engine for GIE
cd ${GIE_COMPILER_DIR} && make build && make run gremlin.script.language.name=antlr_gremlin_calcite graph.physical.opt=proto graph.planner.opt=CBO graph.statistics=./src/test/resources/statistics/imdb_statistics.json &
cd ${GIE_ENGINE_DIR}/target/release && RUST_LOG=info DATA_PATH=/tmp/bench_data/gie_imdb_bin ./start_rpc_server --config ${GIE_ENGINE_DIR}/integrated/config &


# download raw csv data for kuzu
curl -o ${DATA_DIR}/imdb_csv.tar.gz -L "https://graphscope.oss-accelerate-overseas.aliyuncs.com/dataset/imdb.tar.gz"
cd ${DATA_DIR} && tar xvzf imdb_csv.tar.gz

# start engine for kuzu
cd ${KUZU_DB_DIR} && python job_server.py

# run the benchmark
cd ${BASE_DIR} && make build && make config.path=${CONFIG_FILE} run && make config.path=${CONFIG_FILE} collect






