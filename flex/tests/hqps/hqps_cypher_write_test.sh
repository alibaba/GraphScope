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
BULK_LOADER=${FLEX_HOME}/build/bin/bulk_loader
SERVER_BIN=${FLEX_HOME}/build/bin/interactive_server
GIE_HOME=${FLEX_HOME}/../interactive_engine/

#
if [ $# -ne 3 ]; then
  echo "Receives: $# args, need 3 args"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE> <GRAPH_NAME> <ENGINE_CONFIG>"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1
GRAPH_NAME=$2
ENGINE_CONFIG_PATH=$3
# if ENGINE_CONFIG_PATH is not absolute path, convert it to absolute path
if [[ ! ${ENGINE_CONFIG_PATH} == /* ]]; then
  ENGINE_CONFIG_PATH=$(cd $(dirname ${ENGINE_CONFIG_PATH}) && pwd)/$(basename ${ENGINE_CONFIG_PATH})
fi


if [ ! -d ${INTERACTIVE_WORKSPACE} ]; then
  echo "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE} not exists"
  exit 1
fi

if [ ! -d ${INTERACTIVE_WORKSPACE}/data/${GRAPH_NAME} ]; then
  echo "GRAPH: ${GRAPH_NAME} not exists"
  exit 1
fi
if [ ! -f ${INTERACTIVE_WORKSPACE}/data/${GRAPH_NAME}/graph.yaml ]; then
  echo "GRAPH_SCHEMA_FILE: ${BULK_LOAD_FILE} not exists"
  exit 1
fi
if [ ! -f ${ENGINE_CONFIG_PATH} ]; then
  echo "ENGINE_CONFIG: ${ENGINE_CONFIG_PATH} not exists"
  exit 1
fi

GRAPH_SCHEMA_YAML=${INTERACTIVE_WORKSPACE}/data/${GRAPH_NAME}/graph.yaml
GRAPH_CSR_DATA_DIR=${INTERACTIVE_WORKSPACE}/data/${GRAPH_NAME}/indices
rm -rf ${GRAPH_CSR_DATA_DIR}/wal

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
    ps -ef | grep "interactive_server" |  awk '{print $2}' | xargs kill -9  || true
    sleep 3
    sed -i "s/default_graph: .*/default_graph: modern_graph/g" ${ENGINE_CONFIG_PATH}
    rm -rf ${INTERACTIVE_WORKSPACE}/METADATA || (err "rm METADATA failed")
    rm ${INTERACTIVE_WORKSPACE}/data/1 || (err "rm builtin graph failed")
    info "Kill Service success"
    rm -rf /tmp/neo4j-* || true
    # clean the wal
    rm -rf ${GRAPH_CSR_DATA_DIR}/wal || true
    rm -rf ${GRAPH_CSR_DATA_DIR}/runtime || true
}

# kill service when exit
trap kill_service EXIT


# start engine service and load ldbc graph
start_engine_service(){
    # suppose graph has been loaded, check ${GRAPH_CSR_DATA_DIR} exists
    rm -rf ${INTERACTIVE_WORKSPACE}/metadata/
    rm ${INTERACTIVE_WORKSPACE}/data/1 || true
    if [ ! -d ${GRAPH_CSR_DATA_DIR} ]; then
        err "GRAPH_CSR_DATA_DIR not found"
        exit 1
    fi

    #check SERVER_BIN exists
    if [ ! -f ${SERVER_BIN} ]; then
        err "SERVER_BIN not found"
        exit 1
    fi
    sed -i "s/default_graph: .*/default_graph: ${GRAPH_NAME}/g" ${ENGINE_CONFIG_PATH}

    cmd="${SERVER_BIN} -c ${ENGINE_CONFIG_PATH} "
    cmd="${cmd} -w ${INTERACTIVE_WORKSPACE} --enable-admin-service true --start-compiler true"

    info "Start engine service with command: ${cmd}"
    ${cmd} &
    sleep 5
    #check interactive_server is running, if not, exit
    ps -ef | grep "interactive_server" | grep -v grep

    info "Start engine service success"
}


start_compiler_service(){
  echo "try to start compiler service"
  pushd ${GIE_HOME}/compiler
  cmd="make run graph.schema=${GRAPH_SCHEMA_YAML} config.path=${ENGINE_CONFIG_PATH}"
  echo "Start compiler service with command: ${cmd}"
  ${cmd} &
  sleep 5
  # check if Graph Server is running, if not exist
  ps -ef | grep "com.alibaba.graphscope.GraphServer" | grep -v grep
  info "Start compiler service success"
  popd
}

run_cypher_write_test(){
    echo "run cypher write test"
    pushd ${GIE_HOME}/compiler
    cmd="mvn test -Dskip.ir.core=true -Dtest=com.alibaba.graphscope.cypher.integration.modern.ModernGraphWriteTest"
    echo "Run cypher write test with command: ${cmd}"
    ${cmd}
    info "Run cypher write test success"
    popd
}


kill_service
start_engine_service
start_compiler_service
run_cypher_write_test



kill_service
