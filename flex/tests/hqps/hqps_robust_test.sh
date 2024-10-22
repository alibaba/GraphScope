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
ADMIN_PORT=7777
QUERY_PORT=10000
CYPHER_PORT=7687

if [ ! $# -eq 3 ]; then
  echo "only receives: $# args, need 3"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE> <ENGINE_CONFIG> <CBO_ENGINE_CONFIG_PATH>"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1
ENGINE_CONFIG_PATH=$2
CBO_ENGINE_CONFIG_PATH=$3

if [ ! -d ${INTERACTIVE_WORKSPACE} ]; then
  echo "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE} not exists"
  exit 1
fi
if [ ! -f ${ENGINE_CONFIG_PATH} ]; then
  echo "ENGINE_CONFIG: ${ENGINE_CONFIG_PATH} not exists"
  exit 1
fi

if [ ! -f ${CBO_ENGINE_CONFIG_PATH} ]; then
  echo "CBO_ENGINE_CONFIG_PATH: ${CBO_ENGINE_CONFIG_PATH} not exists"
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
    ps -ef | grep "interactive_server" |  awk '{print $2}' | xargs kill -9  || true
    ps -ef | grep "compiler" |  awk '{print $2}' | xargs kill -9  || true
    sleep 3
    # check if service is killed
    info "Kill Service success"
}

# kill service when exit
trap kill_service EXIT

# start engine service
start_engine_service(){
    # expect one argument
    if [ ! $# -eq 1 ]; then
        err "start_engine_service need one argument"
        exit 1
    fi
    local config_path=$1
    #check SERVER_BIN exists
    if [ ! -f ${SERVER_BIN} ]; then
        err "SERVER_BIN not found"
        exit 1
    fi

    cmd="${SERVER_BIN} -c ${config_path} --enable-admin-service true "
    cmd="${cmd}  -w ${INTERACTIVE_WORKSPACE} --start-compiler true &"

    echo "Start engine service with command: ${cmd}"
    eval ${cmd} 
    sleep 10
    #check interactive_server is running, if not, exit
    ps -ef | grep "interactive_server" | grep -v grep

    info "Start engine service success"
}


run_robust_test(){
    pushd ${FLEX_HOME}/interactive/sdk/python/gs_interactive
    cmd="python3 -m pytest -s tests/test_robustness.py"
    echo "Run robust test with command: ${cmd}"
    eval ${cmd} || (err "Run robust test failed"; exit 1)
    info "Run robust test success"
    popd
}

run_additional_robust_test(){
    pushd ${FLEX_HOME}/interactive/sdk/python/gs_interactive
    export RUN_ON_PROTO=ON
    cmd="python3 -m pytest -s tests/test_robustness.py -k test_call_proc_in_cypher"
    echo "Run additional robust test with command: ${cmd}"
    eval ${cmd} || (err "Run additional robust test failed"; exit 1)
    info "Run additional robust test success"
    popd
}

kill_service
start_engine_service $ENGINE_CONFIG_PATH
export INTERACTIVE_ADMIN_ENDPOINT=http://localhost:${ADMIN_PORT}
export INTERACTIVE_STORED_PROC_ENDPOINT=http://localhost:${QUERY_PORT}
export INTERACTIVE_CYPHER_ENDPOINT=neo4j://localhost:${CYPHER_PORT}
export INTERACTIVE_GREMLIN_ENDPOINT=ws://localhost:${GREMLIN_PORT}/gremlin

run_robust_test
kill_service 
sleep 5
start_engine_service $CBO_ENGINE_CONFIG_PATH
run_additional_robust_test

kill_service