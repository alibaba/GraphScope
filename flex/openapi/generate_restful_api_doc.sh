#!/bin/bash
# Copyright 2024 Alibaba Group Holding Limited.
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


# find the generator binary
GENERATOR_BIN=$(which openapi-generator-cli)
if [ -z "$GENERATOR_BIN" ]; then
  GENERATOR_BIN=$(which openapi-generator)
  if [ -z "$GENERATOR_BIN" ]; then
    # try to find in ~/bin/openapitools/
    GENERATOR_BIN=~/bin/openapitools/openapi-generator-cli
    if [ ! -f "$GENERATOR_BIN" ]; then
        echo "openapi-generator-cli not found, please install it first"
        exit 1
    fi
  fi
fi

function usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  -h, --help         Show this help message and exit"
  echo "  -c, --component    Optional values: interactive, coordinator"
  echo "  -s, --style        Optional values: html, html2"
  echo "  -o, --output-path  The output path for the generated resutful API documentation"
}

function do_gen() {
  cmd="${GENERATOR_BIN} generate -i ${OPENAPI_SPEC_PATH} -g ${STYLE} -o ${TMP_DIR}"
  echo "Running command: ${cmd}"
  eval $cmd

  # copy to the output path
  cmd="cp -r ${TMP_DIR}/index.html ${OUTPUT_PATH}"
  echo "Running cmd ${cmd}"
  eval $cmd

  echo "Finish generating RESTful API doc, output path: ${OUTPUT_PATH}"
}

readonly CUR_DIR=$(cd $(dirname $0); pwd)
readonly TMP_DIR="/tmp/openapi_api_doc"
COMPONENT="interactive"
STYLE="html2"
OUTPUT_PATH="/tmp/restful_api_doc.html"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      usage
      exit
      ;;
    -o | --output-path)
      OUTPUT_PATH="$2"
      shift 2
      ;;
    -s | --style)
      STYLE="$2"
      shift 2
      ;;
    -c | --component)
      COMPONENT="$2"
      shift 2
      ;;
    *) # unknown option
      err "unknown option $1"
      usage
      exit 1
      ;;
  esac
done


if [[ "${COMPONENT}" == "interactive" ]]; then
  OPENAPI_SPEC_PATH="${CUR_DIR}/openapi_interactive.yaml"
elif [[ "${COMPONENT}" == "coordinator" ]]; then
  OPENAPI_SPEC_PATH="${CUR_DIR}/openapi_coordinator.yaml"
else
  echo "unknown component: ${COMPONENT}"
  usage
  exit
fi

echo "Generating restful api doc for ${COMPONENT} with ${STYLE} style."

do_gen
