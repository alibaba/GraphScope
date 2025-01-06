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

LDBC_GRAPH_SCHEMA_PATH=${INTERACTIVE_WORKSPACE}/data/ldbc/graph.yaml
MOVIE_GRAPH_SCHEMA_PATH=${INTERACTIVE_WORKSPACE}/data/movies/graph.yaml
GRAPH_ALGO_GRAPH_SCHEMA_PATH=${INTERACTIVE_WORKSPACE}/data/graph_algo/graph.yaml


test_codegen_on_ldbc_cbo(){
    # Expect two args, first is the work_dir, second is the plugin_dir
    if [ ! $# -eq 2 ]; then
        echo "only receives: $# args, need 2"
        echo "Usage: $0 <work_dir> <plugin_dir>"
        exit 1
    fi
    local work_dir=$1
    local plugin_dir=$2
    # we need to start engine service first for cbo test, since statistics is needed
    # failed and reason: 
    # 1. PathExpand output Path with Both Vertex and Edges
    for i in 2 3 4 5 6 8 9 10 11 12;
    # 7 is not supported now
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${FLEX_HOME}/resources/queries/ic/stored_procedure/ic${i}.cypher -w=${work_dir}"
        cmd=${cmd}" -o=${plugin_dir} --ir_conf=${CBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${LDBC_GRAPH_SCHEMA_PATH}"
        cmd=${cmd}" --statistic_path=${LDBC_STATISTICS}"
        echo $cmd
        eval ${cmd} || exit 1
    done
}

test_codegen_on_ldbc_rbo(){
    if [ ! $# -eq 2 ]; then
      echo "only receives: $# args, need 2"
      echo "Usage: $0 <work_dir> <plugin_dir>"
      exit 1
    fi
    local work_dir=$1
    local plugin_dir=$2
    for i in 1 2 3 4 5 6 7 8 9 10 11 12;
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${FLEX_HOME}/resources/queries/ic/adhoc/ic${i}_adhoc.cypher -w=${work_dir}"
        cmd=${cmd}" -o=${plugin_dir} --ir_conf=${RBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${LDBC_GRAPH_SCHEMA_PATH}"
        echo $cmd
        eval ${cmd} || exit 1
    done
}

test_codegen_on_ldbc_rbo_simple_match(){
    if [ ! $# -eq 2 ]; then
      echo "only receives: $# args, need 2"
      echo "Usage: $0 <work_dir> <plugin_dir>"
      exit 1
    fi
    local work_dir=$1
    local plugin_dir=$2
    for i in 1 2 3 4 5 6 7 8 9 11 12; # 10 is not supported now
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${FLEX_HOME}/resources/queries/ic/adhoc/simple_match_${i}.cypher -w=${work_dir}"
        cmd=${cmd}" -o=${plugin_dir} --ir_conf=${RBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${LDBC_GRAPH_SCHEMA_PATH}"
        echo $cmd
        eval ${cmd} || exit 1
    done
}

test_codegen_on_movie_rbo(){
    if [ ! $# -eq 2 ]; then
      echo "only receives: $# args, need 2"
      echo "Usage: $0 <work_dir> <plugin_dir>"
      exit 1
    fi
    local work_dir=$1
    local plugin_dir=$2
    # test movie graph, 8,9,10 are not supported now
    for i in 1 2 3 4 5 6 7 11 12 13 14 15;
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${FLEX_HOME}/tests/hqps/queries/movie/query${i}.cypher -w=${work_dir}"
        cmd=${cmd}" -o=${plugin_dir} --ir_conf=${RBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${MOVIE_GRAPH_SCHEMA_PATH}"
        echo $cmd
        eval ${cmd} || exit 1
    done
}

test_codegen_on_graph_algo(){
    if [ ! $# -eq 2 ]; then
      echo "only receives: $# args, need 2"
      echo "Usage: $0 <work_dir> <plugin_dir>"
      exit 1
    fi
    local work_dir=$1
    local plugin_dir=$2
    cypher_files=$(ls ${GITHUB_WORKSPACE}/flex/interactive/examples/graph_algo/*.cypher)
    for cypher_file in ${cypher_files};
    do
        cmd="${CODEGEN_SCRIPT} -e=hqps -i=${cypher_file} -w=${work_dir}"
        cmd=${cmd}" -o=${plugin_dir} --ir_conf=${RBO_ENGINE_CONFIG_PATH} "
        cmd=${cmd}" --graph_schema_path=${GRAPH_ALGO_GRAPH_SCHEMA_PATH}"
        cmd=${cmd}" --procedure_name=$(basename ${cypher_file} .cypher)"
        cmd=${cmd}" --procedure_desc=\"This is test procedure, change the description if needed.\""
        echo $cmd
        eval ${cmd} || exit 1
    done
}

task1() {
  echo "task1"
  sleep 11
}

task2() {
  echo "task2"
  sleep 12
}

task3() {
  echo "task3"
  sleep 13
}

task4() {
  echo "task4"
  sleep 14
}

task5() {
  echo "task5"
  sleep 15
}

test_cases='test_codegen_on_ldbc_cbo test_codegen_on_ldbc_rbo test_codegen_on_ldbc_rbo_simple_match test_codegen_on_movie_rbo test_codegen_on_graph_algo'
#test_cases='task1 task2 task3 task4 task5'
work_dirs='/tmp/codegen1 /tmp/codegen2 /tmp/codegen3 /tmp/codegen4 /tmp/codegen5'
plugin_dirs='/tmp/plugin1 /tmp/plugin2 /tmp/plugin3 /tmp/plugin4 /tmp/plugin5'

pids=()

launch_test_parallel(){
  # get length of test_cases
  num_task=$(echo $test_cases | wc -w)
  # launch num_task parallel tasks in background, wait for all tasks to finish
  # If one task failed, exit
  for i in $(seq 1 $num_task); do
    test_case=$(echo $test_cases | cut -d ' ' -f $i)
    work_dir=$(echo $work_dirs | cut -d ' ' -f $i)
    plugin_dir=$(echo $plugin_dirs | cut -d ' ' -f $i)
    graph_schema_path=$(echo $graph_schema_paths | cut -d ' ' -f $i)
    # call function $test_case
    info "launching $test_case, work_dir: $work_dir, plugin_dir: $plugin_dir, graph_schema_path: $graph_schema_path"
    eval $test_case $work_dir $plugin_dir $graph_schema_path || exit 1 &
    pid=$!
    pids+=($pid)
  done

  for i in $(seq 1 $num_task); do
    wait ${pids[$i-1]} || (err "task ${pids[$i-1]} failed"; exit 1)
    info "task ${pids[$i-1]} finished"
  done
}

cleanup(){
  for i in $(seq 1 $num_task); do
    work_dir=$(echo $work_dirs | cut -d ' ' -f $i)
    plugin_dir=$(echo $plugin_dirs | cut -d ' ' -f $i)
    rm -rf $work_dir
    rm -rf $plugin_dir
  done
}

launch_test_parallel

cleanup
