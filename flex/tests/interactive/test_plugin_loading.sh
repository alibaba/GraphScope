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

if [ $# -ne 3 ]; then
  echo "Receives: $# args, need 3 args"
  echo "Usage: $0 <SCHEMA_PATH> <IMPORT_FILE> <ENGINE_CONFIG>"
  exit 1
fi

SCHEMA_PATH=$1
IMPORT_FILE=$2
ENGINE_CONFIG_PATH=$3

echo "SCHEMA_PATH: ${SCHEMA_PATH}"
echo "IMPORT_FILE: ${IMPORT_FILE}"
echo "ENGINE_CONFIG_PATH: ${ENGINE_CONFIG_PATH}"

DATA_PATH=/tmp/test_plugin_loading
if [ -d ${DATA_PATH} ]; then
  rm -rf ${DATA_PATH}
fi
mkdir -p ${DATA_PATH}

# First load the data with the bulk loader
cmd="GLOG_v=10 ${BULK_LOADER} -g ${SCHEMA_PATH} -d ${DATA_PATH} -l ${IMPORT_FILE}"
echo "Loading data with bulk loader"
echo $cmd
eval $cmd || exit 1


# Try to start service with the generated plugins, for both v0.0 schema and v0.1 schema
# and check if the service can be started successfully
cmd="GLOG_v=10 ${SERVER_BIN} -g ${SCHEMA_PATH} --data-path ${DATA_PATH} -c ${ENGINE_CONFIG_PATH}"
echo "Starting service with modern graph schema v0.0"
echo $cmd
eval "$cmd &" 

sleep 10

# check process is running, if not running, exit
if ! ps -p $! > /dev/null
then
    echo "Test failed for modern graph schema v0.0"
    exit 1
fi

# stop the service
kill -9 $!

echo "Test passed for modern graph schema v0.0"