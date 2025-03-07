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
if [ $# -ne 2 ]; then
  echo "Receives: $# args, need 2 args"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE> <ENGINE_CONFIG>"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1
ENGINE_CONFIG_PATH=$2


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

    #check SERVER_BIN exists
    if [ ! -f ${SERVER_BIN} ]; then
        err "SERVER_BIN not found"
        exit 1
    fi
    cmd="${SERVER_BIN} -c ${ENGINE_CONFIG_PATH} --start-compiler true "
    cmd="${cmd} -w ${INTERACTIVE_WORKSPACE} --enable-admin-service true > /tmp/engine.log 2>&1 &"

    info "Start engine service with command: ${cmd}"
    ${cmd} &
    sleep 5
    #check interactive_server is running, if not, exit
    ps -ef | grep "interactive_server" | grep -v grep

    info "Start engine service success"
}



run_type_test() {
    echo "run type test"
    pushd ${GIE_HOME}/compiler
    cmd="mvn test -Dtest=com.alibaba.graphscope.cypher.integration.flex.bench.FlexTypeTest"
    info "Run type test with command: ${cmd}"
    ${cmd}
    info "Run type test success"
    popd
}

kill_service
start_engine_service
run_type_test
kill_service
