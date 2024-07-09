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

# This script is used to generate the Java SDK from the Flex Interactive API
# It uses the Swagger Codegen tool to generate the SDK

CUR_DIR=$(cd $(dirname $0); pwd)
OPENAPI_SPEC_PATH="${CUR_DIR}/../openapi/openapi_interactive.yaml"
TMP_DIR=/tmp/interactive_api_doc/

#find the generator binary
GENERATOR_BIN=$(which openapi-generator-cli)
if [ -z "$GENERATOR_BIN" ]; then
    # try to find in ~/bin/openapitools/
    GENERATOR_BIN=~/bin/openapitools/openapi-generator-cli
    if [ ! -f "$GENERATOR_BIN" ]; then
        echo "openapi-generator-cli not found, please install it first"
        exit 1
    fi
fi

function usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  -h, --help         Show this help message and exit"
  echo "  -o, --output-path  The output path for the generated resutful API documentation"
}

function do_gen() {
  echo "Generating Interactive RESTful API documentation"
  OUTPUT_PATH="$1"
  echo "Output path: ${OUTPUT_PATH}"

  cmd="${GENERATOR_BIN} generate -i ${OPENAPI_SPEC_PATH} -g html2 -o ${TMP_DIR}"
  echo "Running command: ${cmd}"
  eval $cmd

  # copy to the output path
  cmd="cp -r ${TMP_DIR}/index.html ${OUTPUT_PATH}"
  echo "Running cmd ${cmd}"
  eval $cmd

  echo "Finish generating Interactive RESTful API documentation"
}


while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  -h | --help)
    usage
    exit
    ;;
  -o | --output-path)
    shift
    do_gen "$@"
    exit 0
    ;;
  *) # unknown option
    err "unknown option $1"
    usage
    exit 1
    ;;
  esac
done