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

PACKAGE_NAME="com.alibaba.graphscope.interactive"
GROUP_ID="com.alibaba.graphscope"
ARTIFACT_ID="interactive-java-sdk"
ARTIFACT_URL="https://github.com/alibaba/GraphScope/tree/main/flex/interactive"
VERSION="0.0.1"
EMAIL="graphscope@alibaba-inc.com"
DESCRIPTION="GraphScope Interactive Java SDK"
ORGANIZATION="Alibaba GraphScope"
developerOrganizationUrl="https://graphscope.io"
DEVELOPER_NAME="GraphScope Team"
LICENSE_NAME="Apache-2.0"
LICENSE_URL="https://www.apache.org/licenses/LICENSE-2.0.html"

addtional_properties="licenseName=${LICENSE_NAME},licenseUrl=${LICENSE_URL},artifactUrl=${ARTIFACT_URL}"
addtional_properties="${addtional_properties},artifactDescription=\"${DESCRIPTION}\",developerEmail=${EMAIL}"
addtional_properties="${addtional_properties},developerName=\"${DEVELOPER_NAME}\",developerOrganization=\"${ORGANIZATION}\""
addtional_properties="${addtional_properties},developerOrganizationUrl=${developerOrganizationUrl}"


#get current bash scrip's directory
CUR_DIR=$(cd `dirname $0`; pwd)
OPENAPI_SPEC_PATH="${CUR_DIR}/../../../openapi/openapi_interactive.yaml"
OUTPUT_PATH="${CUR_DIR}/interactive_sdk/"

cmd="openapi-generator-cli generate -i ${OPENAPI_SPEC_PATH} -g java -o ${OUTPUT_PATH}"
cmd=" ${cmd} --api-package ${PACKAGE_NAME}"
cmd=" ${cmd} --artifact-id ${ARTIFACT_ID}"
cmd=" ${cmd} --artifact-version ${VERSION}"
cmd=" ${cmd} --group-id ${GROUP_ID}"
cmd=" ${cmd} --additional-properties=${addtional_properties}"


echo "Running command: ${cmd}"
eval $cmd