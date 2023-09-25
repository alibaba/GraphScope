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
SERVER_BIN=${FLEX_HOME}/build/bin/sync_server
GIE_HOME=${FLEX_HOME}/../interactive_engine/

# 
if [ $# -lt 2 ]; then
  echo "only receives: $# args, need 2"
  echo "Usage: $0 <GS_TEST_DIR> <INTERACTIVE_WORKSPACE>"
  exit 1
fi

GS_TEST_DIR=$1
INTERACTIVE_WORKSPACE=$2
if [ ! -d ${GS_TEST_DIR} ]; then
  echo "GS_TEST_DIR: ${GS_TEST_DIR} not exists"
  exit 1
fi
if [ ! -d ${INTERACTIVE_WORKSPACE} ]; then
  echo "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE} not exists"
  exit 1
fi

ENGINE_CONFIG_PATH=${GS_TEST_DIR}/flex/ldbc-sf01-long-date/engine_config.yaml
ORI_GRAPH_SCHEMA_YAML=${GS_TEST_DIR}/flex/ldbc-sf01-long-date/audit_graph_schema.yaml
GRAPH_SCHEMA_YAML=${INTERACTIVE_WORKSPACE}/data/ldbc/graph.yaml
GRAPH_BULK_LOAD_YAML=${GS_TEST_DIR}/flex/ldbc-sf01-long-date/audit_bulk_load.yaml
COMPILER_GRAPH_SCHEMA=${GS_TEST_DIR}/flex/ldbc-sf01-long-date/audit_graph_schema.yaml
GRAPH_CSR_DATA_DIR=${HOME}/csr-data-dir/
# check if GRAPH_SCHEMA_YAML exists
if [ ! -f ${GRAPH_SCHEMA_YAML} ]; then
  echo "GRAPH_SCHEMA_YAML: ${GRAPH_SCHEMA_YAML} not found"
  exit 1
fi

# check if GRAPH_BULK_LOAD_YAML exists
if [ ! -f ${GRAPH_BULK_LOAD_YAML} ]; then
  echo "GRAPH_BULK_LOAD_YAML: ${GRAPH_BULK_LOAD_YAML} not found"
  exit 1
fi

# check if COMPILER_GRAPH_SCHEMA exists
if [ ! -f ${COMPILER_GRAPH_SCHEMA} ]; then
  echo "COMPILER_GRAPH_SCHEMA: ${COMPILER_GRAPH_SCHEMA} not found"
  exit 1
fi

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
    ps -ef | grep "com.alibaba.graphscope.GraphServer" | awk '{print $2}' | xargs kill -9 || true
    ps -ef | grep "sync_server" |  awk '{print $2}' | xargs kill -9  || true
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
    # export FLEX_DATA_DIR
    export FLEX_DATA_DIR=${GS_TEST_DIR}/flex/ldbc-sf01-long-date/

    cmd="${SERVER_BIN} -c ${ENGINE_CONFIG_PATH} -g ${GRAPH_SCHEMA_YAML} "
    cmd="${cmd} --data-path ${GRAPH_CSR_DATA_DIR} -l ${GRAPH_BULK_LOAD_YAML} "

    echo "Start engine service with command: ${cmd}"
    ${cmd} &
    sleep 5
    #check sync_server is running, if not, exit
    ps -ef | grep "sync_server" | grep -v grep

    info "Start engine service success"
}


start_compiler_service(){
  echo "try to start compiler service"
  pushd ${GIE_HOME}/compiler
  cmd="make run graph.schema=${COMPILER_GRAPH_SCHEMA} config.path=${ENGINE_CONFIG_PATH}"
  echo "Start compiler service with command: ${cmd}"
  ${cmd} &
  sleep 5
  # check if Graph Server is running, if not exist
  ps -ef | grep "com.alibaba.graphscope.GraphServer" | grep -v grep
  info "Start compiler service success"
  popd
}

run_ldbc_test() {
  echo "run ldbc test"
  pushd ${GIE_HOME}/compiler
  cmd="mvn test -Dtest=com.alibaba.graphscope.cypher.integration.ldbc.IrLdbcTest"
  echo "Start ldbc test: ${cmd}"
  ${cmd}
  info "Finish ldbc test"
  popd
}

run_simple_test(){
  echo "run simple test"
  pushd ${GIE_HOME}/compiler
  cmd="mvn test -Dtest=com.alibaba.graphscope.cypher.integration.ldbc.SimpleMatchTest"
  echo "Start simple test: ${cmd}"
  ${cmd}
  info "Finish simple test"
  popd
}

kill_service
start_engine_service
start_compiler_service
run_ldbc_test
run_simple_test
kill_service




