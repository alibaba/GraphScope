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
CYPHER_PORT=7687
GREMLIN_PORT=8182

# 
if [ ! $# -eq 3 ]; then
  echo "only receives: $# args, need 3"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE> <ENGINE_CONFIG> <SDK_TYPE>"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1
ENGINE_CONFIG_PATH=$2
SDK_TYPE=$3
if [ ! -d ${INTERACTIVE_WORKSPACE} ]; then
  echo "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE} not exists"
  exit 1
fi
if [ ! -f ${ENGINE_CONFIG_PATH} ]; then
  echo "ENGINE_CONFIG: ${ENGINE_CONFIG_PATH} not exists"
  exit 1
fi

# if SDK_TYPE != java or python, exit
if [ ${SDK_TYPE} != "java" ] && [ ${SDK_TYPE} != "python" ]; then
  echo "SDK_TYPE: ${SDK_TYPE} not supported, only support java or python"
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

# start engine service and load ldbc graph
start_engine_service(){
    #check SERVER_BIN exists
    if [ ! -f ${SERVER_BIN} ]; then
        err "SERVER_BIN not found"
        exit 1
    fi

    cmd="${SERVER_BIN} -c ${ENGINE_CONFIG_PATH} --enable-admin-service true "
    cmd="${cmd}  -w ${INTERACTIVE_WORKSPACE} --start-compiler true &"

    echo "Start engine service with command: ${cmd}"
    eval ${cmd} 
    sleep 10
    #check interactive_server is running, if not, exit
    ps -ef | grep "interactive_server" | grep -v grep

    info "Start engine service success"
}

run_java_sdk_test(){
  echo "run java sdk test"
  pushd ${FLEX_HOME}/interactive/sdk/java/
  cmd="mvn test -Dtest=com.alibaba.graphscope.interactive.client.DriverTest"
  echo "Start java sdk test: ${cmd}"
  eval ${cmd} || (err "java sdk test failed" &&  exit 1)
  info "Finish java sdk test"
  popd
}

run_python_sdk_test(){
  echo "run python sdk test"
  pushd ${FLEX_HOME}/interactive/sdk/python/gs_interactive
  cmd="python3 -m pytest -s tests/test_driver.py"
  echo "Run python sdk test: ${cmd}"
  eval ${cmd} || (err "test_driver failed" &&  exit 1)
  cmd="python3 -m pytest -s tests/test_utils.py"
  echo "Run python sdk test: ${cmd}"
  eval ${cmd} || (err "test_utils failed" &&  exit 1)
  info "Finish python sdk test"
  popd
}

kill_service
start_engine_service
export INTERACTIVE_ADMIN_ENDPOINT=http://localhost:${ADMIN_PORT}
export INTERACTIVE_STORED_PROC_ENDPOINT=http://localhost:${QUERY_PORT}
export INTERACTIVE_CYPHER_ENDPOINT=neo4j://localhost:${CYPHER_PORT}
export INTERACTIVE_GREMLIN_ENDPOINT=ws://localhost:${GREMLIN_PORT}/gremlin
if [ ${SDK_TYPE} == "java" ]; then
  run_java_sdk_test
elif [ ${SDK_TYPE} == "python" ]; then
  run_python_sdk_test
else
  err "SDK_TYPE: ${SDK_TYPE} not supported, only support java or python"
  exit 1
fi
kill_service




