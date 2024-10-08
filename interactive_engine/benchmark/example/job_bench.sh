#!/bin/bash

set -x
CURR_DIR=$(cd "$(dirname "$0")"; pwd)
BENCHMARK_DIR=${CURR_DIR}/..
declare -r CONFIG_FILE="${CURR_DIR}/job_benchmark.properties"
declare -r DATA_DIR="/tmp/bench_data"
declare -r GIE_COMPILER_DIR=${CURR_DIR}/../../compiler
declare -r GIE_ENGINE_DIR=${CURR_DIR}/../../executor/ir
declare -r GIE_EXP_STORE_DIR=${CURR_DIR}/../../executor/store/exp_store
declare -r KUZU_DB_DIR=${CURR_DIR}/../dbs/kuzu
declare -r KUZU_JOB_DIR="/tmp/bench_data/kuzu_job"

# if --skip-gie-compile is set, skip the compilation of GIE
SKIP_GIE_COMPILE=false
for arg in "$@"; do
    case $arg in
        --skip-gie-compile)
            SKIP_GIE_COMPILE=true
            shift
            ;;
    esac
done

# download binary data for GIE
mkdir -p ${DATA_DIR}
if [ ! -d ${DATA_DIR}/imdb_bin ]; then
    echo "Downloading binary data for GIE..."
    curl -o ${DATA_DIR}/imdb_bin.tar.gz -L "https://graphscope.oss-cn-beijing.aliyuncs.com/gopt_data/imdb_bin.tar.gz"
    cd ${DATA_DIR} && tar xvzf imdb_bin.tar.gz
else
    echo "Binary data for GIE already exists."
fi

# download raw data for kuzu
if [ ! -d ${DATA_DIR}/imdb ]; then
    echo "Downloading raw csv data for kuzu..."
    curl -o ${DATA_DIR}/imdb.tar.gz -L "https://graphscope.oss-accelerate-overseas.aliyuncs.com/dataset/imdb.tar.gz"
    cd ${DATA_DIR} && tar xvzf imdb.tar.gz
else
    echo "Raw csv data for kuzu already exists."
fi

# clean service
ps -ef | grep "com.alibaba.graphscope.GraphServer" | grep -v grep | awk '{print $2}' | xargs kill -9 || true
ps -ef | grep "start_rpc_server" | grep -v grep | awk '{print $2}' | xargs kill -9

# start engine for GIE
sed -i "s/SingleValueTable/PropertyTable/g" ${GIE_EXP_STORE_DIR}/src/graph_db/graph_db_impl.rs # This is a temporary fix for exp store, as imdb data is encoded as PropertyTable
echo "Starting GIE engine..."
if [ "$SKIP_GIE_COMPILE" = false ]; then
    echo "Compiling GIE..."
    cd ${GIE_COMPILER_DIR} && make build 
fi
cd ${GIE_COMPILER_DIR} && make run graph.schema:=../executor/ir/core/resource/imdb_schema.yaml gremlin.script.language.name=antlr_gremlin_calcite graph.physical.opt=proto graph.planner.opt=CBO graph.statistics=./src/test/resources/statistics/imdb_statistics.json &
cd ${GIE_ENGINE_DIR}/target/release && DATA_PATH=/tmp/bench_data/imdb_bin ./start_rpc_server --config ${GIE_ENGINE_DIR}/integrated/config &
# waiting for loading data
sleep 60

# prepare kuzudb
echo "Starting kuzu..."
if ! python3 -c "import kuzu" &> /dev/null; then
    echo "Installing kuzu..."
    python3 -m pip install kuzu
else
    echo "kuzu is already installed."
fi
if [ ! -d "${KUZU_JOB_DIR}" ]; then
    echo "Building kuzu database..."
    cd ${KUZU_DB_DIR} && python3 job_server.py ${KUZU_JOB_DIR} ${KUZU_DB_DIR}/resource/job_schema.cypher ${KUZU_DB_DIR}/resource/job_dataloading.cypher &
    sleep 60
fi

# run the benchmark
echo "Running the benchmark..."
cd ${BENCHMARK_DIR} && make build && make config.path=${CONFIG_FILE} run && make config.path=${CONFIG_FILE} collect
