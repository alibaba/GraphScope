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
FLEX_HOME=${SCRIPT_DIR}/../
echo "FLEX_HOME root =         ${FLEX_HOME}"
FLEX_INCLUDE=${FLEX_HOME}/include/flex/
echo "FLEX_INCLUDE directory = ${CODE_GEN_ROOT}"

# first try to resolve as if we are installed. the try to resolve locally.

if [ ! -d ${FLEX_INCLUDE} ]; then
  echo "try FLEX_HOME ${FLEX_INCLUDE} not exists."
  echo "try to resolve locally"
else
  echo "try FLEX_HOME ${FLEX_INCLUDE} exists."
  CODEGEN_RUNNER=${FLEX_HOME}/bin/gen_code_from_plan
  CMAKE_TEMPLATE_PATH=${FLEX_HOME}/lib/flex/CMakeLists.txt.template
  FLEX_LIB_DIR=${FLEX_HOME}/lib/
  PEGASUS_COMPILE_PATH=${FLEX_HOME}resources/pegasus/benchmark
fi

FLEX_INCLUDE=${FLEX_HOME}/../
echo "try to find flex with FLEX_INCLUDE directory = ${FLEX_INCLUDE}"
if [ ! -d ${FLEX_INCLUDE} ]; then
  echo "FLEX_INCLUDE directory = ${FLEX_INCLUDE} not exists."
  echo "Fail to find flex."
  exit 1
else
  CODEGEN_RUNNER=${FLEX_HOME}/build/codegen/gen_code_from_plan
  CMAKE_TEMPLATE_PATH=${FLEX_HOME}/resources/hqps/CMakeLists.txt.template
  FLEX_LIB_DIR=${FLEX_HOME}/build/lib/
  PEGASUS_COMPILE_PATH=${FLEX_HOME}resources/pegasus/benchmark
fi

echo "Codegen runner = ${CODEGEN_RUNNER}"
echo "Cmake template path = ${CMAKE_TEMPLATE_PATH}"
#check these files exist
if [ ! -f ${CODEGEN_RUNNER} ]; then
 echo "Codegen runner = ${CODEGEN_RUNNER} not exists."
 echo "Fail to find codegen_runner."
 exit 1
fi

if [ ! -f ${CMAKE_TEMPLATE_PATH} ]; then
  echo "Cmake template path = ${CMAKE_TEMPLATE_PATH} not exists."
  echo "Fail to find CMakeLists.txt.template."
  exit 1
fi

#if [ ! -f ${PEGASUS_COMPILE_PATH} ]; then
#  echo "Pegasus compile path = ${PEGASUS_COMPILE_PATH} not exists."
#  exit 1
#fi

cypher_to_plan() {
  if [ $# -ne 5 ]; then
    echo "Usage: $0 <input_file> <output_file> <ir_compiler_properties> <graph_schema_path> <gie_home>, but receive: "$#
    exit 1
  fi
  input_path=$1
  output_path=$2
  ir_compiler_properties=$3
  graph_schema_path=$4
  GIE_HOME=$5
  # find java executable
  echo "IR compiler properties = ${ir_compiler_properties}"
  #check file exists
  if [ ! -f ${ir_compiler_properties} ]; then
    echo "IR compiler properties = ${ir_compiler_properties} not exists."
    echo "Fail to find IR compiler properties."
    exit 1
  fi
  JAVA_EXECUTABLE=$(which java)
  if [ -z ${JAVA_EXECUTABLE} ]; then
    # try find from JAVA_HOME
    if [ -z ${JAVA_HOME} ]; then
      echo "JAVA_HOME not set."
      exit 1
    else
      JAVA_EXECUTABLE=${JAVA_HOME}/bin/java
    fi
    exit 1
  fi
  echo "Java executable = ${JAVA_EXECUTABLE}"
  echo "---------------------------"
  echo "Find compiler exists"
  # read from file ${input_path}
  cypher_query=$(cat ${input_path})
  echo "Find cypher query:"
  echo "---------------------------"
  echo ${cypher_query}
  echo "---------------------------"

  #get abs path of input_path
  real_input_path=$(realpath ${input_path})
  real_output_path=$(realpath ${output_path})

  compiler_jar=${GIE_HOME}/compiler/target/compiler-0.0.1-SNAPSHOT.jar
  if [ ! -f ${compiler_jar} ]; then
    echo "Compiler jar = ${compiler_jar} not exists."
    echo "Fail to find compiler jar."
    exit 1
  fi
  cmd="java -cp ${GIE_HOME}/compiler/target/libs/*:${compiler_jar}"
  cmd="${cmd} -Dgraph.schema=${graph_schema_path}"
  cmd="${cmd} -Djna.library.path=${GIE_HOME}/executor/ir/target/release/"
  cmd="${cmd} com.alibaba.graphscope.common.ir.tools.GraphPlanner ${ir_compiler_properties} ${real_input_path} ${real_output_path}"
  echo "running physical plan genration with "${cmd}
  eval ${cmd}

  echo "---------------------------"
  #check output
  if [ ! -f ${output_path} ]; then
    echo "Output file = ${output_path} not exists."
    echo "Fail to generate physical plan."
    exit 1
  fi
}

compile_hqps_so() {
  #check input params size eq 2 or 3
  if [ $# -ne 5 ] && [ $# -ne 6 ]; then
    echo "Usage: $0 <input_file> <work_dir> <ir_compiler_properties_file> <graph_schema_file> <GIE_HOME>[output_dir]"
    exit 1
  fi
  input_path=$1
  work_dir=$2
  ir_compiler_properties=$3
  graph_schema_path=$4
  gie_home=$5
  if [ $# -eq 6 ]; then
    output_dir=$6
  else
    output_dir=${work_dir}
  fi
  echo "Input path = ${input_path}"
  echo "Work dir = ${work_dir}"
  echo "ir compiler properties = ${ir_compiler_properties}"
  echo "graph schema path = ${graph_schema_path}"
  echo "GIE_HOME = ${gie_home}"
  echo "Output dir = ${output_dir}"

  last_file_name=$(basename ${input_path})

  echo "last file name: ${last_file_name}"
  # requiest last_file_name suffix is .pb
  if [[ $last_file_name == *.pb ]]; then
    query_name="${last_file_name%.pb}"
    echo "File has .pb suffix."
  elif [[ $last_file_name == *.cc ]]; then
    echo "File havs .cc suffix."
    query_name="${last_file_name%.cc}"
  elif [[ $last_file_name == *.cypher ]]; then
    echo "File has .cypher suffix."
    query_name="${last_file_name%.cypher}"
  else
    echo "Expect a .pb or .cc file"
    exit 1
  fi
  cur_dir=${work_dir}
  mkdir -p ${cur_dir}
  output_cc_path="${cur_dir}/${query_name}.cc"
  if [[ $(uname) == "Linux" ]]; then
    output_so_path="${cur_dir}/lib${query_name}.so"
    dst_so_path="${output_dir}/lib${query_name}.so"
  elif [[ $(uname) == "Darwin" ]]; then
    output_so_path="${cur_dir}/lib${query_name}.dylib"
    dst_so_path="${output_dir}/lib${query_name}.dylib"
  else
    echo "Not support OS."
    exit 1
  fi

  #only do codegen when receives a .pb file.
  if [[ $last_file_name == *.pb ]]; then
    cmd="${CODEGEN_RUNNER} -e hqps -i ${input_path} -o ${output_cc_path}"
    echo "Codegen command = ${cmd}"
    eval ${cmd}
    echo "----------------------------"
  elif [[ $last_file_name == *.cypher ]]; then
    echo "Generating code from cypher query"
    # first do .cypher to .pb
    output_pb_path="${cur_dir}/${query_name}.pb"
    cypher_to_plan ${input_path} ${output_pb_path} ${ir_compiler_properties} ${graph_schema_path} ${gie_home}
    echo "----------------------------"
    echo "Codegen from cypher query done."
    echo "----------------------------"
    cmd="${CODEGEN_RUNNER} -e hqps -i ${output_pb_path} -o ${output_cc_path}"
    echo "Codegen command = ${cmd}"
    eval ${cmd}
    # then. do .pb to .cc
  elif [[ $last_file_name == *.cc ]]; then
    cp $input_path ${output_cc_path}
  fi
  echo "Start running cmake and make"
  #check output_cc_path exists
  if [ ! -f ${output_cc_path} ]; then
    echo "Codegen failed, ${output_cc_path} not exists."
    exit 1
  fi

  # copy cmakelist.txt to output path.
  cp ${CMAKE_TEMPLATE_PATH} ${cur_dir}/CMakeLists.txt
  # run cmake and make in output path.
  pushd ${cur_dir}
  cmd="cmake . -DQUERY_NAME=${query_name} -DFLEX_INCLUDE_PREFIX=${FLEX_INCLUDE} -DFLEX_LIB_DIR=${FLEX_LIB_DIR}"
  # if CMAKE_CXX_COMPILER is set, use it.
  if [ ! -z ${CMAKE_CXX_COMPILER} ]; then
    cmd="${cmd} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
  fi
  # if CMAKE_C_COMPILER is set, use it.
  if [ ! -z ${CMAKE_C_COMPILER} ]; then
    cmd="${cmd} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
  fi
  echo "Cmake command = ${cmd}"
  echo "---------------------------"
  eval ${cmd}

  ################### now build ##########################
  # get number of cores
  #n_cores=`nproc`
  n_cores=1
  cmd="make -j"${n_cores}
  echo "Make command = ${cmd}"
  eval ${cmd}
  echo "---------------------------"
  #check if build success
  if [ $? -ne 0 ]; then
    echo "Build failed."
    exit 1
  fi
  # check output_so_name exists
  if [ ! -f ${output_so_path} ]; then
    echo "Build failed, ${output_so_path} not exists."
    exit 1
  fi
  echo "Finish building, output to ${output_so_path}"
  popd

  ################### now copy ##########################
  # if dst_so_path eq output_so_path, skip copying.
  if [ ${dst_so_path} == ${output_so_path} ]; then
    echo "Output dir is same as work dir, skip copying."
    exit 0
  fi
  # copy output to output_dir
  if [ ! -z ${output_dir} ]; then
    mkdir -p ${output_dir}
  else
    echo "Output dir not set, skip copying."
    exit 0
  fi
  # check output_dir doesn't contains output_so_name
  if [ -f ${dst_so_path} ]; then
    echo "Output dir ${output_dir} already contains ${query_name}.so"
    echo "Please remove it first."
    exit 1
  fi
  cp ${output_so_path} ${output_dir}
  #check dst_so_path exists
  if [ ! -f ${dst_so_path} ]; then
    echo "Copy failed, ${dst_so_path} not exists."
    exit 1
  fi
  echo "Finish copying, output to ${dst_so_path}"
}

compile_pegasus_so() {
  echo "Start compiling pegasus so"
  #check input params size eq 2 or 3
  if [ $# -ne 4 ] && [ $# -ne 5 ]; then
    echo "Usage: $0 <input_file> <work_dir> <ir_compiler_properties_file> <graph_schema_file> [output_dir]"
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
  echo "Input path = ${input_path}"
  echo "Work dir = ${work_dir}"
  echo "ir compiler properties = ${ir_compiler_properties}"
  echo "graph schema path = ${graph_schema_path}"
  echo "Output dir = ${output_dir}"

  last_file_name=$(basename ${input_path})

  echo "last file name: ${last_file_name}"
  # requiest last_file_name suffix is .pb
  if [[ $last_file_name == *.pb ]]; then
    query_name="${last_file_name%.pb}"
    echo "File has .pb suffix."
  elif [[ $last_file_name == *.rs ]]; then
    echo "File has .rs suffix."
    query_name="${last_file_name%.rs}"
  elif [[ $last_file_name == *.cypher ]]; then
    echo "File has .cypher suffix."
    query_name="${last_file_name%.cypher}"
  elif [[ $last_file_name == *.json ]]; then
    echo "File has .json suffix."
    query_name="${last_file_name%.json}"
  else
    echo "Expect a .pb or .cc file"
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
    echo "Not support OS."
    exit 1
  fi

  #only do codegen when receives a .pb file.
  if [[ $last_file_name == *.json ]]; then
    cmd="${CODEGEN_RUNNER} ${input_path} ${output_rs_path}"
    echo "Codegen command = ${cmd}"
    eval ${cmd}
    echo "----------------------------"
  elif [[ $last_file_name == *.rs ]]; then
    cp $input_path ${output_rs_path}
  fi
  echo "Start running cmake and make"
  #check output_cc_path exists
  if [ ! -f ${output_rs_path} ]; then
    echo "Codegen failed, ${output_rs_path} not exists."
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
    echo "Build failed."
    exit 1
  fi
  # check output_so_name exists
  if [ ! -f ${output_so_path} ]; then
    echo "Build failed, ${output_so_path} not exists."
    exit 1
  fi
  echo "Finish building, output to "${output_so_path}
  popd

  ################### now copy ##########################
  # copy output to output_dir
  if [ ! -z ${output_dir} ]; then
    mkdir -p ${output_dir}
  else
    echo "Output dir not set, skip copying."
    exit 0
  fi
  # check output_dir doesn't contains output_so_name
  if [ -f ${dst_so_path} ]; then
    echo "Output dir ${output_dir} already contains ${query_name}.so"
    echo "Please remove it first."
    exit 1
  fi
  cp ${output_so_path} ${output_dir}
  #check dst_so_path exists
  if [ ! -f ${dst_so_path} ]; then
    echo "Copy failed, ${dst_so_path} not exists."
    exit 1
  fi
  echo "Finish copying, output to ${dst_so_path}"
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
    --gie_home=GIE_HOME
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
    --gie_home=*)
      GIE_HOME="${i#*=}"
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
    -* | --*)
      echo "Unknown option $i"
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
  echo "GIE_HOME               ="${GIE_HOME}
  echo "Output path            ="${OUTPUT_DIR}


  # check input exist
  if [ ! -f ${INPUT} ]; then
    echo "Input file ${INPUT} not exists."
    exit 1
  fi

  # if engine_type equals hqps
  if [ ${ENGINE_TYPE} == "hqps" ]; then
    echo "Engine type is hqps, generating dynamic library for hqps engine."
    compile_hqps_so ${INPUT} ${WORK_DIR} ${IR_CONF} ${GRAPH_SCHEMA_PATH} ${GIE_HOME} ${OUTPUT_DIR} 

  # else if engine_type equals pegasus
  elif [ ${ENGINE_TYPE} == "pegasus" ]; then
    echo "Engine type is pegasus, generating dynamic library for pegasus engine."
    compile_pegasus_so ${INPUT} ${WORK_DIR} ${IR_CONF} ${GRAPH_SCHEMA_PATH} ${OUTPUT_DIR}
  else
    echo "Unknown engine type "${ENGINE_TYPE}
    exit 1
  fi
  exit 0
}

if [ $# -lt 6 ]; then
  echo "only receives: $# args"
  usage
  exit 1
fi

run "$@"
