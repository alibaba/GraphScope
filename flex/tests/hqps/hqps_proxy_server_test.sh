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

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
err() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] -ERROR- $* ${NC}" >&2
}

info() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
FLEX_HOME=${SCRIPT_DIR}/../../
INTERACITIVE_SERVER_BIN=${FLEX_HOME}/build/bin/interactive_server
PROXY_SERVER_BIN=${FLEX_HOME}/build/bin/proxy_server
GIE_HOME=${FLEX_HOME}/../interactive_engine/
ENGINE_CONFIG_PATH_WORKER1=/tmp/interactive_engine_config_worker1.yaml
ENGINE_CONFIG_PATH_WORKER2=/tmp/interactive_engine_config_worker2.yaml

if [ $# -lt 2 ] || [ $# -ge 3 ]; then
    echo "Receives: $# args, need 2 args"
    echo "Usage: $0 <INTERACTIVE_WORKSPACE> <ENGINE_CONFIG>"
    exit 1
fi

INTERACTIVE_WORKSPACE=$1
ENGINE_CONFIG_PATH=$2
info "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE}"
info "ENGINE_CONFIG_PATH: ${ENGINE_CONFIG_PATH}"

kill_service(){
    info "Kill Service first"
    ps -ef | grep "com.alibaba.graphscope.GraphServer" | awk '{print $2}' | xargs kill -9 || true
    ps -ef | grep "interactive_server" |  awk '{print $2}' | xargs kill -9  || true
    ps -ef | grep "/bin/proxy_server" |  awk '{print $2}' | xargs kill -9  || true
    sleep 3
    # check if service is killed
    info "Kill Service success"
}

# kill service when exit
trap kill_service EXIT

# create two copy of engine config, for two workers.
prepare_engine_config() {
    if [ -f ${ENGINE_CONFIG_PATH_WORKER1} ]; then
        rm -f ${ENGINE_CONFIG_PATH_WORKER1}
    fi
    if [ -f ${ENGINE_CONFIG_PATH_WORKER2} ]; then
        rm -f ${ENGINE_CONFIG_PATH_WORKER2}
    fi
    cp ${ENGINE_CONFIG_PATH} ${ENGINE_CONFIG_PATH_WORKER1}
    cp ${ENGINE_CONFIG_PATH} ${ENGINE_CONFIG_PATH_WORKER2}
    # modify the engine config
    sed -i "s/localhost:10000/localhost:10001/g" ${ENGINE_CONFIG_PATH_WORKER2}
    sed -i "s/port: 7687/port: 7688/g" ${ENGINE_CONFIG_PATH_WORKER2}
    sed -i "s/port: 8182/port: 8183/g" ${ENGINE_CONFIG_PATH_WORKER2}
    sed -i "s/admin_port: 7777/admin_port: 7778/g" ${ENGINE_CONFIG_PATH_WORKER2}
    sed -i "s/query_port: 10000/query_port: 10001/g" ${ENGINE_CONFIG_PATH_WORKER2}
}

start_worker() {
    info "start worker1"
    graph_yaml=${INTERACTIVE_WORKSPACE}/data/modern_graph/graph.yaml
    indices_path=${INTERACTIVE_WORKSPACE}/data/modern_graph/indices
    base_cmd="${INTERACITIVE_SERVER_BIN} -g ${graph_yaml}"
    base_cmd=" ${base_cmd} --data-path ${indices_path}"
    cmd1=" ${base_cmd} -c ${ENGINE_CONFIG_PATH_WORKER1}"
    cmd2=" ${base_cmd} -c ${ENGINE_CONFIG_PATH_WORKER2}"
    info "Start worker1 with command: ${cmd1}"
    ${cmd1} &
    sleep 2
    info "Start worker2 with command: ${cmd2}"
    ${cmd2} &
    sleep 2
    # check whether interactive_server has two process running
    cnt=$(ps -ef | grep "bin/interactive_server" | grep -v grep | wc -l)
    if [ ${cnt} -ne 2 ]; then
        err "Start worker failed, expect 2 interactive_server process, but got ${cnt}"
        exit 1
    fi
    info "Start worker success"
}

start_proxy() {
    info "start proxy server"
    cmd="${PROXY_SERVER_BIN} -e localhost:10000,localhost:10001"
    info "Start proxy server with command: ${cmd}"
    ${cmd} &
    sleep 2
    # check whether proxy_server is running
    cnt=$(ps -ef | grep "bin/proxy_server" | grep -v grep | wc -l)
    if [ ${cnt} -ne 1 ]; then
        err "Start proxy server failed, expect 1 proxy_server process, but got ${cnt}"
        exit 1
    fi
    info "Start proxy server success"
}


test_proxy() {
    # First check whether proxy server is running, if not, exit
    cnt=$(ps -ef | grep "bin/proxy_server" | grep -v grep | wc -l)
    if [ ${cnt} -ne 1 ]; then
        err "Proxy server is not running, got cnt ${cnt}, expect 1"
        exit 1
    fi
    # test proxy server
    info "Test proxy server"
    # check heart beat
    res=$(curl -X GET  http://localhost:9999/heartbeat)
    if [ "${res}" != "OK" ]; then
        err "Test proxy server failed, expect OK, but got ${res}"
        exit 1
    fi
    # now kill worker2, and check whether proxy server can still work
    ps -ef | grep "bin/interactive_server" | grep -v grep | grep ${ENGINE_CONFIG_PATH_WORKER2} | awk '{print $2}' | xargs kill -9
    sleep 2
    res=$(curl -X GET  http://localhost:9999/heartbeat)
    # shold still be ok
    if [ "${res}" != "OK" ]; then
        err "Test proxy server failed, expect OK, but got ${res}"
        exit 1
    fi
    # now kill worker1, and check whether proxy server can still work
    ps -ef | grep "bin/interactive_server" | grep -v grep | grep ${ENGINE_CONFIG_PATH_WORKER1} | awk '{print $2}' | xargs kill -9
    sleep 2
    res=$(curl -X GET  http://localhost:9999/heartbeat)
    # shold not contains OK
    if [ "${res}" == "OK" ]; then
        err "Test proxy server failed, expect not OK, but got ${res}"
        exit 1
    fi
    info "Test proxy server success"
}


kill_service

prepare_engine_config
start_worker # start interactive worker first
start_proxy # start the proxy server
test_proxy 

kill_service