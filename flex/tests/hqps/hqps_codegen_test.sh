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
CODEGEN_SCRIPT=${FLEX_HOME}/bin/load_plan_and_gen.sh
LDBC_STATISTICS=${GIE_HOME}/compiler/src/test/resources/statistics/ldbc1_statistics.json
ADMIN_PORT=7777
QUERY_PORT=10000

if [ ! $# -eq 3 ]; then
  echo "only receives: $# args, need 3"
  echo "Usage: $0 <INTERACTIVE_WORKSPACE> <RBO_ENGINE_CONFIG> <CBO_ENGINE_CONFIG>"
  exit 1
fi

INTERACTIVE_WORKSPACE=$1
RBO_ENGINE_CONFIG_PATH=$2
CBO_ENGINE_CONFIG_PATH=$3
if [ ! -d ${INTERACTIVE_WORKSPACE} ]; then
  echo "INTERACTIVE_WORKSPACE: ${INTERACTIVE_WORKSPACE} not exists"
  exit 1
fi
if [ ! -f ${RBO_ENGINE_CONFIG_PATH} ]; then
  echo "ENGINE_CONFIG: ${RBO_ENGINE_CONFIG_PATH} not exists"
  exit 1
fi
if [ ! -f ${CBO_ENGINE_CONFIG_PATH} ]; then
  echo "ENGINE_CONFIG: ${CBO_ENGINE_CONFIG_PATH} not exists"
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


test_codegen_on_ldbc_cbo(){
    # we need to start engine service first for cbo test, since statistics is needed
    # failed and reason: 
    # 1. PathExpand output Path with Both Vertex and Edges
    for i in 2 3 4 5 6 8 9 10 11 12;
    # 7 is not supported now
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${FLEX_HOME}/resources/queries/ic/stored_procedure/ic${i}.cypher -w=/tmp/codegen/"
        cmd=${cmd}" -o=/tmp/plugin --ir_conf=${CBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${INTERACTIVE_WORKSPACE}/data/ldbc/graph.yaml"
        cmd=${cmd}" --statistic_path=${LDBC_STATISTICS}"
        echo $cmd
        eval ${cmd} || exit 1
    done
}

test_codegen_on_ldbc_rbo(){
    sed -i 's/default_graph: modern_graph/default_graph: ldbc/g' ${RBO_ENGINE_CONFIG_PATH}
    for i in 1 2 3 4 5 6 7 8 9 10 11 12;
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${FLEX_HOME}/resources/queries/ic/adhoc/ic${i}_adhoc.cypher -w=/tmp/codegen/"
        cmd=${cmd}" -o=/tmp/plugin --ir_conf=${RBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${INTERACTIVE_WORKSPACE}/data/ldbc/graph.yaml"
        echo $cmd
        eval ${cmd} || exit 1
    done
    for i in 1 2 3 4 5 6 7 8 9 11 12; # 10 is not supported now
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${FLEX_HOME}/resources/queries/ic/adhoc/simple_match_${i}.cypher -w=/tmp/codegen/"
        cmd=${cmd}" -o=/tmp/plugin --ir_conf=${RBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${INTERACTIVE_WORKSPACE}/data/ldbc/graph.yaml"
        echo $cmd
        eval ${cmd} || exit 1
    done
    sed -i 's/default_graph: ldbc/default_graph: modern_graph/g' ${RBO_ENGINE_CONFIG_PATH}
}

test_codegen_on_movie_rbo(){
    # test movie graph, 8,9,10 are not supported now
    # change the default_graph config in ../tests/hqps/interactive_config_test.yaml to movies
    sed -i 's/default_graph: modern_graph/default_graph: movies/g' ${RBO_ENGINE_CONFIG_PATH}
    for i in 1 2 3 4 5 6 7 11 12 13 14 15;
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${FLEX_HOME}/tests/hqps/queries/movie/query${i}.cypher -w=/tmp/codegen/"
        cmd=${cmd}" -o=/tmp/plugin --ir_conf=${RBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${INTERACTIVE_WORKSPACE}/data/movies/graph.yaml"
        echo $cmd
        eval ${cmd} || exit 1
    done
    sed -i 's/default_graph: movies/default_graph: modern_graph/g' ${RBO_ENGINE_CONFIG_PATH}
}

test_codegen_on_graph_algo(){
    sed -i 's/default_graph: modern_graph/default_graph: graph_algo/g' ${RBO_ENGINE_CONFIG_PATH}
    cypher_files=$(ls ${GITHUB_WORKSPACE}/flex/interactive/examples/graph_algo/*.cypher)
    for cypher_file in ${cypher_files};
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${cypher_file} -w=/tmp/codegen/"
        cmd=${cmd}" -o=/tmp/plugin --ir_conf=${RBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${INTERACTIVE_WORKSPACE}/data/graph_algo/graph.yaml"
        cmd=${cmd}" --procedure_name=$(basename ${cypher_file} .cypher)"
        cmd=${cmd}" --procedure_desc=\"This is test procedure, change the description if needed.\""
        echo $cmd
        eval ${cmd} || exit 1
    done
    sed -i 's/default_graph: graph_algo/default_graph: modern_graph/g' ${RBO_ENGINE_CONFIG_PATH}
}

test_codegen_on_ldbc_cbo
test_codegen_on_ldbc_rbo
test_codegen_on_movie_rbo
test_codegen_on_graph_algo

rm -rf /tmp/codegen
rm -rf /tmp/plugin