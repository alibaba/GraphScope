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
if [ $# -lt 3 ] || [ $# -gt 4 ]; then
  echo "Receives: $# args, need 3 or 4 args"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE> <GRAPH_NAME> <ENGINE_CONFIG> [TEST_TYPE(cypher/gremlin/all)]"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1
GRAPH_NAME=$2
ENGINE_CONFIG_PATH=$3
if [ $# -eq 4 ]; then
  TEST_TYPE=$4
else
  TEST_TYPE="cypher" # default run cypher tests
fi

# check TEST_TYPE is valid
if [ ${TEST_TYPE} != "cypher" ] && [ ${TEST_TYPE} != "gremlin" ] && [ ${TEST_TYPE} != "all" ]; then
  echo "TEST_TYPE: ${TEST_TYPE} not supported, use cypher or gremlin"
  exit 1
else
  echo "TEST_TYPE: ${TEST_TYPE}"
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
    # check if service is killed
    info "Kill Service success"
}

# kill service when exit
trap kill_service EXIT


# start engine service and load ldbc graph
start_engine_service(){
    # suppose graph has been loaded, check ${GRAPH_CSR_DATA_DIR} exists
    if [ ! -d ${GRAPH_CSR_DATA_DIR} ]; then
        err "GRAPH_CSR_DATA_DIR not found"
        exit 1
    fi

    #check SERVER_BIN exists
    if [ ! -f ${SERVER_BIN} ]; then
        err "SERVER_BIN not found"
        exit 1
    fi
    cmd="${SERVER_BIN} -c ${ENGINE_CONFIG_PATH} -g ${GRAPH_SCHEMA_YAML} "
    cmd="${cmd} --data-path ${GRAPH_CSR_DATA_DIR} "

    if [ "${TEST_TYPE}" == "gremlin" ]; then
      cmd="${cmd} --enable-adhoc-handler=true"
    fi

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

run_graph_algo_test(){
  echo "run graph_algo test"
  pushd ${GIE_HOME}/compiler
  cmd="mvn test -Dtest=com.alibaba.graphscope.cypher.integration.graphAlgo.GraphAlgoTest"
  echo "Start graph_algo test: ${cmd}"
  ${cmd}
  info "Finish graph_algo test"
  popd
}

run_cypher_test(){
  # check graph is ldbc or movies
  if [ ${GRAPH_NAME} != "ldbc" ] && [ ${GRAPH_NAME} != "movies" ] && [ ${GRAPH_NAME} != "graph_algo" ]; then
    echo "GRAPH_NAME: ${GRAPH_NAME} not supported, use movies or ldbc"
    exit 1
  fi
  if [ "${GRAPH_NAME}" == "ldbc" ]; then
    run_ldbc_test
    run_simple_test
  elif [ "${GRAPH_NAME}" == "movies" ]; then
    run_movie_test
  elif [ "${GRAPH_NAME}" == "graph_algo" ]; then
    run_graph_algo_test
  else
    echo "GRAPH_NAME: ${GRAPH_NAME} not supported, use movies, ldbc or graph_algo"
  fi
  rm -rf /tmp/neo4j-* || true
}


run_gremlin_test(){
  if [ "${GRAPH_NAME}" != "modern_graph" ]; then
    echo "GRAPH_NAME: ${GRAPH_NAME} not supported, only support modern_graph"
    exit 1
  fi
  echo "run gremlin test"
  pushd ${GIE_HOME}/compiler
  cmd="mvn test -Dtest=com.alibaba.graphscope.gremlin.integration.standard.calcite.IrGremlinTest"
  echo "Start gremlin test: ${cmd}"
  ${cmd}
  info "Finish gremlin test"
  popd
}

kill_service
start_engine_service
start_compiler_service


if [ "${TEST_TYPE}" == "cypher" ]; then
  run_cypher_test
elif [ "${TEST_TYPE}" == "gremlin" ]; then
  info "run gremlin test"
  run_gremlin_test
elif [ "${TEST_TYPE}" == "all" ]; then
  run_cypher_test
  run_gremlin_test
else
  echo "TEST_TYPE: ${TEST_TYPE} not supported, use cypher/gremlin/all"
  exit 1
fi

kill_service
