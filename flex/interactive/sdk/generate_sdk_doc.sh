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
set -e

GENERATED_ENDPOINT="https://virtserver.swaggerhub.com/GRAPHSCOPE/InteractiveAPI/1.0.0"
ENDPOINT_PLACE_HOLDER="{INTERACTIVE_ADMIN_ENDPOINT}"

function usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  -h, --help        Show this help message and exit"
  echo "  -o, --output-dir  Generate the SDK for the specified language"
}

function do_gen() {
  echo "Generating SDK documentation"
  if [ "$#" -ne 1 ]; then
    echo "Invalid number of arguments"
    usage
    exit 1
  fi
  OUTPUT_PATH="$1"
  echo "Output path: ${OUTPUT_PATH}"
  # First check whether java/docs and python/docs exist
  if [ ! -d "./java/docs" ]; then
    echo "java/docs not found, please generate the SDK first"
    exit 1
  fi
  if [ ! -d "./python/docs" ]; then
    echo "python/docs not found, please generate the SDK first"
    exit 1
  fi
  # post process the generated docs inplace.
  # replace all occurrence of $GENERATED_ENDPOINT with $ENDPOINT_PLACE_HOLDER
  # in the generated docs, for all files under ./java/docs and ./python/docs
  sed -i 's|${GENERATED_ENDPOINT}|${ENDPOINT_PLACE_HOLDER}|g' ./java/docs/* || (echo "Failed to replace endpoint in java docs" && exit 1)
  sed -i 's|${GENERATED_ENDPOINT}|${ENDPOINT_PLACE_HOLDER}|g' ./python/docs/* || (echo "Failed to replace endpoint in python docs" && exit 1)
  sed -i 's|\.\.\/README|python_sdk|g' ./python/docs/* || (echo "Failed to replace README in python docs" && exit 1)
  sed -i 's|README|python_sdk|g' ./python/docs/* || (echo "Failed to replace README in python docs" && exit 1)
  sed -i 's|documentation-for-models|documentation-for-data-structures|g' ./python/docs/* || (echo "Failed to replace documentation-for-models in python docs" && exit 1)
  sed -i 's|documentation-for-api-endpoints|documentation-for-service-apis|g' ./python/docs/* || (echo "Failed to replace documentation-for-api-endpoints in python docs" && exit 1)

  # # Copy the generated docs to the output path
  # echo_and_run "find ./java/docs/* -type f ! -name "*Api*" -exec cp {} ${OUTPUT_PATH}/java/ \;"
  find ./java/docs/* -type f ! -name "*Api*" -exec cp {} ${OUTPUT_PATH}/java/ \;
  # echo_and_run "find ./python/docs/* -type f ! -name "*Api*" -exec cp {} ${OUTPUT_PATH}/python/ \;"
  find ./python/docs/* -type f ! -name "*Api*" -exec cp {} ${OUTPUT_PATH}/python/ \;

  echo "SDK documentation generated successfully."
}

SOURCE_FILE_OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  -h | --help)
    usage
    exit
    ;;
  -o | --output-dir)
    # do_gen "$@"
    shift
    SOURCE_FILE_OUTPUT_DIR="$1"
    shift
    ;;
  *) # unknown option
    err "unknown option $1"
    usage
    exit 1
    ;;
  esac
done

do_gen "${SOURCE_FILE_OUTPUT_DIR}"