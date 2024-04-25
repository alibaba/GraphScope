#!/bin/bash
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
set -e
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
FLEX_HOME=${SCRIPT_DIR}/../../
SERVER_BIN=${FLEX_HOME}/build/bin/interactive_server
GIE_HOME=${FLEX_HOME}/../interactive_engine/
ADMIN_PORT=7777
QUERY_PORT=10000

# 
if [ ! $# -eq 3 ]; then
  echo "only receives: $# args, need 3"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE> <ENGINE_CONFIG> <GS_TEST_DIR>"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1
ENGINE_CONFIG_PATH=$2
GS_TEST_DIR=$3
if [ ! -d ${INTERACTIVE_WORKSPACE} ]; then
  echo "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE} not exists"
  exit 1
fi
if [ ! -f ${ENGINE_CONFIG_PATH} ]; then
  echo "ENGINE_CONFIG: ${ENGINE_CONFIG_PATH} not exists"
  exit 1
fi
if [ ! -d ${GS_TEST_DIR} ]; then
  echo "GS_TEST_DIR: ${GS_TEST_DIR} not exists"
  exit 1
fi

GRAPH_SCHEMA_YAML=${FLEX_HOME}/interactive/examples/movies/graph.yaml
GRAPH_BULK_LOAD_YAML=${FLEX_HOME}/interactive/examples/movies/import.yaml
RAW_CSV_FILES=${FLEX_HOME}/interactive/examples/movies/
GRAPH_CSR_DATA_DIR=${HOME}/csr-data-dir/
TEST_CYPHER_QUERIES="${FLEX_HOME}/interactive/examples/movies/0_get_user.cypher ${FLEX_HOME}/interactive/examples/movies/5_recommend_rule.cypher"



RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
err() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] -ERROR- $* ${NC}" >&2
}

info() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}


kill_service(){
    info "Kill Service first"
    ps -ef | grep "interactive_server" |  awk '{print $2}' | xargs kill -9  || true
    sleep 3
    # check if service is killed
    info "Kill Service success"
}

# kill service when exit
trap kill_service EXIT

# start engine service and load ldbc graph
start_engine_service(){
    #check SERVER_BIN exists
    if [ ! -f ${SERVER_BIN} ]; then
        err "SERVER_BIN not found"
        exit 1
    fi

    cmd="${SERVER_BIN} -c ${ENGINE_CONFIG_PATH} --enable-admin-service true "
    cmd="${cmd}  -w ${INTERACTIVE_WORKSPACE} &"

    echo "Start engine service with command: ${cmd}"
    eval ${cmd} 
    sleep 5
    #check interactive_server is running, if not, exit
    ps -ef | grep "interactive_server" | grep -v grep

    info "Start engine service success"
}

run_admin_test(){
  echo "run admin test"
  pushd ${FLEX_HOME}/build/
  cmd="GLOG_v=10 ./tests/hqps/admin_http_test ${ADMIN_PORT} ${QUERY_PORT} ${GRAPH_SCHEMA_YAML} ${GRAPH_BULK_LOAD_YAML} ${RAW_CSV_FILES} ${TEST_CYPHER_QUERIES}"
  echo "Start admin test: ${cmd}"
  eval ${cmd} || (err "admin test failed" &&  exit 1)
  info "Finish admin test"
  popd
}

kill_service
start_engine_service
run_admin_test
kill_service




