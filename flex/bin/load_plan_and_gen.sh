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
YELLOW='\033[0;33m'
NC='\033[0m' # No Color
err() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] -ERROR- $* ${NC}" >&2
}

info() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

emph(){
  echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

function check_file_exists(){
  if [ ! -f $1 ]; then
    err "File $1 not exists."
    exit 1
  fi
}

function find_resources(){
  # get parent directory as flex_home
  flex_home=$(dirname ${SCRIPT_DIR})
  # check whether flex_home is install directory or source directory
  if [ -d ${flex_home}/lib ]; then
    info "FLEX_HOME ${flex_home} is install directory."
    CODEGEN_RUNNER=${flex_home}/bin/gen_code_from_plan
    CMAKE_TEMPLATE_PATH=${flex_home}/lib/flex/CMakeLists.txt.template
    PEGASUS_COMPILE_PATH=${flex_home}lib/flex/pegasus/benchmark
    COMPILER_JAR=${flex_home}/lib/compiler-0.0.1-SNAPSHOT.jar
    COMPILER_LIB_DIR=${flex_home}/lib
    IR_CORE_LIB_DIR=${flex_home}/lib
    FLEX_INCLUDE_PREFIX=${flex_home}/include
  else 
    info "FLEX_HOME ${flex_home} is source directory."
    CODEGEN_RUNNER=${flex_home}/build/codegen/gen_code_from_plan
    CMAKE_TEMPLATE_PATH=${flex_home}/resources/hqps/CMakeLists.txt.template
    PEGASUS_COMPILE_PATH=${flex_home}resources/pegasus/benchmark
    COMPILER_JAR=${flex_home}/../interactive_engine/compiler/target/compiler-0.0.1-SNAPSHOT.jar
    COMPILER_LIB_DIR=${flex_home}/../interactive_engine/compiler/target/libs
    IR_CORE_LIB_DIR=${flex_home}/../interactive_engine/executor/ir/target/release/
    FLEX_INCLUDE_PREFIX=${flex_home}/../
  fi

  check_file_exists ${CODEGEN_RUNNER} || (err "Fail to find codegen_runner." && exit 1)
  check_file_exists ${CMAKE_TEMPLATE_PATH} || (err "Fail to find CMakeLists.txt.template." && exit 1)
#  check_file_exists ${PEGASUS_COMPILE_PATH} || (err "Fail to find pegasus compile path." && exit 1)

  info "Codegen runner = ${CODEGEN_RUNNER}"
  info "Cmake template path = ${CMAKE_TEMPLATE_PATH}"
  #if [ ! -f ${PEGASUS_COMPILE_PATH} ]; then
  #  echo "Pegasus compile path = ${PEGASUS_COMPILE_PATH} not exists."
  #  exit 1
  #fi
}


cypher_to_plan() {
  if [ $# -ne 8 ]; then
    echo "Usage: cypher_to_plan <query_name> <input_file> <output_plan file>"
    echo "                      <output_yaml_file> <ir_compiler_properties> <graph_schema_path>"
    echo "                      <procedure_name> <procedure_description>" 
    echo " but receive: "$#
    exit 1
  fi
  query_name=$1
  input_path=$2
  output_path=$3
  output_yaml_file=$4
  ir_compiler_properties=$5
  graph_schema_path=$6

  # get procedure_name and procedure_description
  procedure_name=$7
  procedure_description=$8

  # find java executable
  info "IR compiler properties = ${ir_compiler_properties}"
  #check file exists
  if [ ! -f ${ir_compiler_properties} ]; then
    err "IR compiler properties = ${ir_compiler_properties} not exists."
    err "Fail to find IR compiler properties."
    exit 1
  fi
  JAVA_EXECUTABLE=$(which java)
  if [ -z ${JAVA_EXECUTABLE} ]; then
    # try find from JAVA_HOME
    if [ -z ${JAVA_HOME} ]; then
      err "JAVA_HOME not set."
      exit 1
    else
      JAVA_EXECUTABLE=${JAVA_HOME}/bin/java
    fi
    exit 1
  fi
  info "Java executable = ${JAVA_EXECUTABLE}"
  info "---------------------------"
  # read from file ${input_path}
  cypher_query=$(cat ${input_path})
  info "Find cypher query:"
  info "---------------------------"
  emph ${cypher_query}
  info "---------------------------"

  #get abs path of input_path
  real_input_path=$(realpath ${input_path})
  real_output_path=$(realpath ${output_path})
  real_output_yaml=$(realpath ${output_yaml_file})

  if [ ! -f ${COMPILER_JAR} ]; then
    err "Compiler jar = ${COMPILER_JAR} not exists."
    exit 1
  fi
  # add extra_key_value_config
  extra_config="name:${procedure_name}"
  extra_config="${extra_config},description:${procedure_description}"

  cmd="java -cp ${COMPILER_LIB_DIR}/*:${COMPILER_JAR}"
  cmd="${cmd} -Dgraph.schema=${graph_schema_path}"
  cmd="${cmd} -Djna.library.path=${IR_CORE_LIB_DIR}"
  cmd="${cmd} com.alibaba.graphscope.common.ir.tools.GraphPlanner ${ir_compiler_properties} ${real_input_path} ${real_output_path} ${real_output_yaml} '${extra_config}'"
  info "running physical plan generation with ${cmd}"
  eval ${cmd}

  info "---------------------------"
  #check output
  if [ ! -f ${real_output_path} ]; then
    err "Output file = ${output_path} not exists, fail to generate physical plan."
    exit 1
  fi

  #check output yaml file
  if [ ! -f ${real_output_yaml} ]; then
    err "Output yaml file = ${output_yaml_file} not exists, fail to generate stored procedure config yaml."
    exit 1
  fi
}

compile_hqps_so() {
  #check input params size eq 2 or 3
  if [ $# -gt 7 ] || [ $# -lt 4 ]; then
    echo "Usage: $0 <input_file> <work_dir> <ir_compiler_properties_file>  <graph_schema_file> "
    echo "          [output_dir] [stored_procedure_name] [stored_procedure_description]"
    exit 1
  fi
  input_path=$1
  work_dir=$2
  ir_compiler_properties=$3
  graph_schema_path=$4
  if [ $# -ge 5 ]; then
    output_dir=$5
  else
    output_dir=${work_dir}
  fi

  if [ $# -ge 6 ]; then
    procedure_name=$6
  else
    procedure_name=""
  fi

  if [ $# -ge 7 ]; then
    procedure_description=$7
  else
    procedure_description=""
  fi

  info "Input path = ${input_path}"
  info "Work dir = ${work_dir}"
  info "ir compiler properties = ${ir_compiler_properties}"
  info "graph schema path = ${graph_schema_path}"
  info "Output dir = ${output_dir}"
  info "Procedure name = ${procedure_name}"
  info "Procedure description = ${procedure_description}"

  last_file_name=$(basename ${input_path})

  # request last_file_name suffix is .pb
  if [[ $last_file_name == *.pb ]]; then
    query_name="${last_file_name%.pb}"
  elif [[ $last_file_name == *.cc ]]; then
    query_name="${last_file_name%.cc}"
  elif [[ $last_file_name == *.cypher ]]; then
    query_name="${last_file_name%.cypher}"
  else
    err "Expect a .pb or .cc file"
    exit 1
  fi
  # if procedure_name is not set, use query_name
  if [ -z "${procedure_name}" ]; then
    procedure_name=${query_name}
  fi
  # if procedure_description is not set, use query_name
  if [ -z "${procedure_description}" ]; then
    procedure_description="Stored procedure for ${procedure_name}"
  fi
  cur_dir=${work_dir}
  mkdir -p ${cur_dir}
  output_cc_path="${cur_dir}/${procedure_name}.cc"
  dst_yaml_path="${output_dir}/${procedure_name}.yaml"
  if [[ $(uname) == "Linux" ]]; then
    output_so_path="${cur_dir}/lib${procedure_name}.so"
    dst_so_path="${output_dir}/lib${procedure_name}.so"
  elif [[ $(uname) == "Darwin" ]]; then
    output_so_path="${cur_dir}/lib${procedure_name}.dylib"
    dst_so_path="${output_dir}/lib${procedure_name}.dylib"
  else
    err "Not support OS."
    exit 1
  fi

  #only do codegen when receives a .pb file.
  if [[ $last_file_name == *.pb ]]; then
    cmd="${CODEGEN_RUNNER} -e hqps -i ${input_path} -o ${output_cc_path}"
    info "Codegen command = ${cmd}"
    eval ${cmd}
    info "----------------------------"
  elif [[ $last_file_name == *.cypher ]]; then
    info "Generating code from cypher query, procedure name: ${procedure_name}, description: ${procedure_description}"
    # first do .cypher to .pb
    output_pb_path="${cur_dir}/${procedure_name}.pb"
    output_yaml_path="${cur_dir}/${procedure_name}.yaml"
    cypher_to_plan ${procedure_name} ${input_path} ${output_pb_path} \
      ${output_yaml_path} ${ir_compiler_properties} ${graph_schema_path} \
      ${procedure_name} "${procedure_description}"

    info "----------------------------"
    info "Codegen from cypher query done."
    info "----------------------------"
    cmd="${CODEGEN_RUNNER} -e hqps -i ${output_pb_path} -o ${output_cc_path}"
    info "Codegen command = ${cmd}"
    eval ${cmd}
    # then. do .pb to .cc
  elif [[ $last_file_name == *.cc ]]; then
    cp $input_path ${output_cc_path}
  fi
  info "Start running cmake and make"
  #check output_cc_path exists
  if [ ! -f ${output_cc_path} ]; then
    err "Codegen failed, ${output_cc_path} not exists."
    exit 1
  fi

  # copy cmakelist.txt to output path.
  cp ${CMAKE_TEMPLATE_PATH} ${cur_dir}/CMakeLists.txt
  # run cmake and make in output path.
  pushd ${cur_dir}
  cmd="cmake . -DQUERY_NAME=${query_name} -DFLEX_INCLUDE_PREFIX=${FLEX_INCLUDE_PREFIX}"
  # if CMAKE_CXX_COMPILER is set, use it.
  if [ ! -z ${CMAKE_CXX_COMPILER} ]; then
    cmd="${cmd} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
  fi
  # if CMAKE_C_COMPILER is set, use it.
  if [ ! -z ${CMAKE_C_COMPILER} ]; then
    cmd="${cmd} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
  fi
  info "CMake command = ${cmd}"
  info "---------------------------"
  eval ${cmd}

  ################### now build ##########################
  # get number of cores
  #n_cores=`nproc`
  n_cores=1
  cmd="make -j"${n_cores}
  info "Make command = ${cmd}"
  eval ${cmd}
  info "---------------------------"
  #check if build success
  if [ $? -ne 0 ]; then
    err "Build failed."
    exit 1
  fi
  # check output_so_name exists
  if [ ! -f ${output_so_path} ]; then
    err "Build failed, ${output_so_path} not exists."
    exit 1
  fi
  info "Finish building, output to ${output_so_path}"
  popd

  ################### now copy ##########################
  # if dst_so_path eq output_so_path, skip copying.
  if [ ${dst_so_path} == ${output_so_path} ]; then
    info "Output dir is same as work dir, skip copying."
    exit 0
  fi
  # copy output to output_dir
  if [ ! -z ${output_dir} ]; then
    mkdir -p ${output_dir}
  else
    info "Output dir not set, skip copying."
    exit 0
  fi
  # check output_dir doesn't contains output_so_name
  if [ -f ${dst_so_path} ]; then
    emph "Output dir ${output_dir} already contains ${procedure_name}.so,overriding it."
  fi
  cp ${output_so_path} ${output_dir}
  #check dst_so_path exists
  if [ ! -f ${dst_so_path} ]; then
    err "Copy failed, ${dst_so_path} not exists."
    exit 1
  fi
  # copy the generated yaml
  cp ${output_yaml_path} ${output_dir}
  if [ ! -f ${dst_yaml_path} ]; then
    err "Copy failed, ${dst_yaml_path} not exists."
    exit 1
  fi
  info "Finish copying, output to ${dst_so_path}"
}

compile_pegasus_so() {
  info "Start compiling pegasus so"
  #check input params size eq 2 or 3
  if [ $# -ne 4 ] && [ $# -ne 5 ]; then
    err "Usage: $0 <input_file> <work_dir> <ir_compiler_properties_file> <graph_schema_file> [output_dir]"
    exit 1
  fi
  input_path=$1
  work_dir=$2
  ir_compiler_properties=$3
  graph_schema_path=$4
  if [ $# -eq 5 ]; then
    output_dir=$5
  else
    output_dir=${work_dir}
  fi
  info "Input path = ${input_path}"
  info "Work dir = ${work_dir}"
  info "ir compiler properties = ${ir_compiler_properties}"
  info "graph schema path = ${graph_schema_path}"
  info "Output dir = ${output_dir}"

  last_file_name=$(basename ${input_path})

  info "last file name: ${last_file_name}"
  # request last_file_name suffix is .pb
  if [[ $last_file_name == *.pb ]]; then
    query_name="${last_file_name%.pb}"
    info "File has .pb suffix."
  elif [[ $last_file_name == *.rs ]]; then
    info "File has .rs suffix."
    query_name="${last_file_name%.rs}"
  elif [[ $last_file_name == *.cypher ]]; then
    info "File has .cypher suffix."
    query_name="${last_file_name%.cypher}"
  elif [[ $last_file_name == *.json ]]; then
    info "File has .json suffix."
    query_name="${last_file_name%.json}"
  else
    err "Expect a .pb or .cc file"
    exit 1
  fi
  cur_dir=${work_dir}
  mkdir -p ${cur_dir}
  output_rs_path=${cur_dir}/${query_name}.rs
  if [[ $(uname) == "Linux" ]]; then
    output_so_path=${PEGASUS_COMPILE_PATH}/target/release/lib.so
    dst_so_path=${output_dir}/lib${query_name}.so
  elif [[ $(uname) == "Darwin" ]]; then
    output_so_path=${PEGASUS_COMPILE_PATH}/target/release/lib.dylib
    dst_so_path=${output_dir}/lib${query_name}.dylib
  else
    err "Not support OS."
    exit 1
  fi

  #only do codegen when receives a .pb file.
  if [[ $last_file_name == *.json ]]; then
    cmd="${CODEGEN_RUNNER} ${input_path} ${output_rs_path}"
    info "Codegen command = ${cmd}"
    eval ${cmd}
    info "----------------------------"
  elif [[ $last_file_name == *.rs ]]; then
    cp $input_path ${output_rs_path}
  fi
  info "Start running cmake and make"
  #check output_cc_path exists
  if [ ! -f ${output_rs_path} ]; then
    err "Codegen failed, ${output_rs_path} not exists."
    exit 1
  fi

  # copy cmakelist.txt to output path.
  rm ${PEGASUS_COMPILE_PATH}/query/src/queries/*.rs
  cp ${output_rs_path} ${PEGASUS_COMPILE_PATH}/query/src/queries
  >${PEGASUS_COMPILE_PATH}/query/src/queries/mod.rs
  echo "pub mod ${query_name};" >> ${PEGASUS_COMPILE_PATH}/query/src/queries/mod.rs
  # build dynamic lib
  pushd ${PEGASUS_COMPILE_PATH}/query
  cmd="cargo build --release"
  eval ${cmd}
  #check if build success
  if [ $? -ne 0 ]; then
    err "Build failed."
    exit 1
  fi
  # check output_so_name exists
  if [ ! -f ${output_so_path} ]; then
    err "Build failed, ${output_so_path} not exists."
    exit 1
  fi
  info "Finish building, output to "${output_so_path}
  popd

  ################### now copy ##########################
  # copy output to output_dir
  if [ ! -z ${output_dir} ]; then
    mkdir -p ${output_dir}
  else
    info "Output dir not set, skip copying."
    exit 0
  fi
  # check output_dir doesn't contains output_so_name
  if [ -f ${dst_so_path} ]; then
    err "Output dir ${output_dir} already contains ${query_name}.so, overriding it."
  fi
  cp ${output_so_path} ${output_dir}
  #check dst_so_path exists
  if [ ! -f ${dst_so_path} ]; then
    err "Copy failed, ${dst_so_path} not exists."
    exit 1
  fi
  info "Finish copying, output to ${dst_so_path}"
}

function usage(){
  cat << EOF
  Usage: $0 [options]
  Options:
    -e, --engine_type=ENGINE_TYPE
    -i, --input=INPUT
    -w, --work_dir=WORK_DIR
    --ir_conf=IR_CONF
    --graph_schema_path=GRAPH_SCHEMA_PATH
    [-o, --output_dir=OUTPUT_DIR]
EOF
}

# input path
# output dir
run() {
  for i in "$@"; do
    case $i in
    -e=* | --engine_type=*)
      ENGINE_TYPE="${i#*=}"
      shift # past argument=value
      ;;
    -i=* | --input=*)
      INPUT="${i#*=}"
      shift # past argument=value
      ;;
    -w=* | --work_dir=*)
      WORK_DIR="${i#*=}"
      shift # past argument=value
      ;;
    --ir_conf=*)
      IR_CONF="${i#*=}"
      shift # past argument=value
      ;;
    --graph_schema_path=*)
      GRAPH_SCHEMA_PATH="${i#*=}"
      shift # past argument=value
      ;;
    -o=* | --output_dir=*)
      OUTPUT_DIR="${i#*=}"
      shift # past argument=value
      ;;
    --procedure_name=*)
      PROCEDURE_NAME="${i#*=}"
      shift # past argument=value
      ;;
    --procedure_desc=*)
      PROCEDURE_DESCRIPTION="${i#*=}"
      shift # past argument=value
      ;;
    -* | --*)
      err "Unknown option $i"
      exit 1
      ;;
    *) ;;

    esac
  done

  echo "Engine type            ="${ENGINE_TYPE}
  echo "Input                  ="${INPUT}
  echo "Work dir               ="${WORK_DIR}
  echo "ir conf                ="${IR_CONF}
  echo "graph_schema_path      ="${GRAPH_SCHEMA_PATH}
  echo "Output path            ="${OUTPUT_DIR}
  echo "Procedure name         ="${PROCEDURE_NAME}
  echo "Procedure description  ="${PROCEDURE_DESCRIPTION}

  find_resources

  # check input exist
  if [ ! -f ${INPUT} ]; then
    err "Input file ${INPUT} not exists."
    exit 1
  fi

  if [ -z "${OUTPUT_DIR}" ]; then
    OUTPUT_DIR=${WORK_DIR}
  fi

  # if engine_type equals hqps
  if [ ${ENGINE_TYPE} == "hqps" ]; then
    echo "Engine type is hqps, generating dynamic library for hqps engine."
    # if PROCEDURE_DESCRIPTION is not set, use empty string
    if [ -z ${PROCEDURE_DESCRIPTION} ]; then
      PROCEDURE_DESCRIPTION="Automatic generated description for stored procedure ${PROCEDURE_NAME}."
    fi
    # if PROCEDURE_NAME is not set, use input file name
    if [ -z "${PROCEDURE_NAME}" ]; then
      #remove the suffix of input file, the suffix is .cc or .cypher
      PROCEDURE_NAME=$(basename ${INPUT})
      PROCEDURE_NAME="${PROCEDURE_NAME%.cc}"
      PROCEDURE_NAME="${PROCEDURE_NAME%.pb}"
    fi
    compile_hqps_so ${INPUT} ${WORK_DIR} ${IR_CONF} ${GRAPH_SCHEMA_PATH} ${OUTPUT_DIR} ${PROCEDURE_NAME} "${PROCEDURE_DESCRIPTION}"

  # else if engine_type equals pegasus
  elif [ ${ENGINE_TYPE} == "pegasus" ]; then
    info "Engine type is pegasus, generating dynamic library for pegasus engine."
    compile_pegasus_so ${INPUT} ${WORK_DIR} ${IR_CONF} ${GRAPH_SCHEMA_PATH} ${OUTPUT_DIR}
  else
    err "Unknown engine type "${ENGINE_TYPE}
    exit 1
  fi
  exit 0
}

if [ $# -lt 5 ]; then
  err "only receives: $# args"
  usage
  exit 1
fi

run "$@"
