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
if [ ! $# -eq 4 ]; then
  echo "only receives: $# args, need 4"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE> <GRAPH_NAME> <BULK_LOAD_FILE> <ENGINE_CONFIG>"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1
GRAPH_NAME=$2
GRAPH_BULK_LOAD_YAML=$3
ENGINE_CONFIG_PATH=$4
if [ ! -d ${INTERACTIVE_WORKSPACE} ]; then
  echo "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE} not exists"
  exit 1
fi
# check graph is ldbc or movies
if [ ${GRAPH_NAME} != "ldbc" ] && [ ${GRAPH_NAME} != "movies" ]; then
  echo "GRAPH_NAME: ${GRAPH_NAME} not supported, use movies or ldbc"
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
if [ ! -f ${GRAPH_BULK_LOAD_YAML} ]; then
  echo "GRAPH_BULK_LOAD_YAML: ${GRAPH_BULK_LOAD_YAML} not exists"
  exit 1
fi
if [ ! -f ${ENGINE_CONFIG_PATH} ]; then
  echo "ENGINE_CONFIG: ${ENGINE_CONFIG_PATH} not exists"
  exit 1
fi

GRAPH_SCHEMA_YAML=${INTERACTIVE_WORKSPACE}/data/${GRAPH_NAME}/graph.yaml
GRAPH_CSR_DATA_DIR=${HOME}/csr-data-dir/
# rm data dir if exists
if [ -d ${GRAPH_CSR_DATA_DIR} ]; then
  rm -rf ${GRAPH_CSR_DATA_DIR}
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
  cmd="make run graph.schema=${GRAPH_SCHEMA_YAML} config.path=${ENGINE_CONFIG_PATH}"
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

run_movie_test(){
  echo "run movie test"
  pushd ${GIE_HOME}/compiler
  cmd="mvn test -Dtest=com.alibaba.graphscope.cypher.integration.movie.MovieTest"
  echo "Start movie test: ${cmd}"
  ${cmd}
  info "Finish movie test"
  popd
}

kill_service
start_engine_service
start_compiler_service
# if GRAPH_NAME equals ldbc
if [ "${GRAPH_NAME}" == "ldbc" ]; then
  run_ldbc_test
  run_simple_test
else
  run_movie_test
fi

kill_service




