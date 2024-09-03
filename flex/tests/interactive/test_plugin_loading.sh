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

kill_service(){
    echo "Kill Service first"
    ps -ef | grep "interactive_server" |  awk '{print $2}' | xargs kill -9  || true
    ps -ef | grep "com.alibaba.graphscope.GraphServer" |  awk '{print $2}' | xargs kill -9  || true
    sleep 3
    # check if service is killed
    echo "Kill Service success"
}

# kill service when exit
trap kill_service EXIT

start_engine_service(){
    # expect one args 
    if [ $# -lt 1 ]; then
        echo "Receives: $# args, need 1 args"
        echo "Usage: $0 <ENABLE_COMPILER>"
        exit 1
    fi
    enable_compiler=$1
    #check SERVER_BIN exists
    if [ ! -f ${SERVER_BIN} ]; then
        err "SERVER_BIN not found"
        exit 1
    fi
    cmd="${SERVER_BIN} -w ${WORKSPACE} -c ${ENGINE_CONFIG_PATH} --enable-admin-service true"
    if [ "${enable_compiler}" == "true" ]; then
        cmd="${cmd} --start-compiler true"
    fi
    
    echo "Start engine service with command: ${cmd}"
    ${cmd} &
    sleep 10
    #check interactive_server is running, if not, exit
    ps -ef | grep "interactive_server" | grep -v grep

    echo "Start engine service success"
}

if [ $# -ne 5 ]; then
  echo "Receives: $# args, need 5 args"
  echo "Usage: $0 <WORKSPACE> <GRAPH_NAME> <ENGINE_CONFIG> <SCHEMA_VERSION_00> <SCHEMA_VERSION_01>"
  exit 1
fi

WORKSPACE=$1
GRAPH_NAME=$2
ENGINE_CONFIG_PATH=$3
SCHEMA_VERSION_00=$4
SCHEMA_VERSION_01=$5

if [ ! -d ${WORKSPACE} ]; then
  echo "WORKSPACE: ${WORKSPACE} not exists"
  exit 1
fi
if [ ! -d ${WORKSPACE}/data/${GRAPH_NAME} ]; then
  echo "GRAPH: ${GRAPH_NAME} not exists"
  exit 1
fi

if [ ! -f ${SCHEMA_VERSION_00} ]; then
  echo "SCHEMA_VERSION_00: ${SCHEMA_VERSION_00} not exists"
  exit 1
fi

if [ ! -f ${SCHEMA_VERSION_01} ]; then
  echo "SCHEMA_VERSION_01: ${SCHEMA_VERSION_01} not exists"
  exit 1
fi

echo "WORKSPACE: ${WORKSPACE}"
echo "ENGINE_CONFIG_PATH: ${ENGINE_CONFIG_PATH}"


# Try to start service with the generated plugins, for both v0.0 schema and v0.1 schema
# and check if the service can be started successfully

check_procedure_loading_and_calling_via_encoder() {
  kill_service
  if [ $# -ne 1 ]; then
    echo "Receives: $# args, need 1 args"
    echo "Usage: $0 <SCHEMA_FILE>"
    exit 1
  fi
  cp $1 ${WORKSPACE}/data/${GRAPH_NAME}/graph.yaml
  start_engine_service false

  python3 test_call_proc.py --endpoint http://localhost:7777 --input-format encoder

  kill_service
}

check_procedure_loading_and_calling_via_cypher_json() {
  kill_service
  if [ $# -ne 1 ]; then
    echo "Receives: $# args, need 1 args"
    echo "Usage: $0 <SCHEMA_FILE>"
    exit 1
  fi
  cp $1 ${WORKSPACE}/data/${GRAPH_NAME}/graph.yaml
  start_engine_service true

  sleep 5
  python3 test_call_proc.py --endpoint http://localhost:7777 --input-format json

  kill_service
}

echo "Testing for schema file: ${SCHEMA_VERSION_00}"
rm -rf ${WORKSPACE}/METADATA/
check_procedure_loading_and_calling_via_encoder ${SCHEMA_VERSION_00}
echo "Testing for schema file: ${SCHEMA_VERSION_01}"
rm -rf ${WORKSPACE}/METADATA/
check_procedure_loading_and_calling_via_cypher_json ${SCHEMA_VERSION_01}

echo "Test passed for plugin loading and calling"