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
  if [ $# -ne 9 ]; then
    echo "Usage: cypher_to_plan <query_name> <input_file> <output_plan file> <output_yaml_file>"
    echo "          <ir_compiler_properties> <graph_schema_path> <gie_home>"
    echo "           <procedure_name> <procedure_description>"    
    echo " but receive: "$#
    exit 1
  fi
  query_name=$1
  input_path=$2
  output_path=$3
  output_yaml_file=$4
  ir_compiler_properties=$5
  graph_schema_path=$6
  GIE_HOME=$7

  # get procedure_name and procedure_description
  procedure_name=$8
  procedure_description=$9

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
  real_output_yaml=$(realpath ${output_yaml_file})

  compiler_jar=${GIE_HOME}/compiler/target/compiler-0.0.1-SNAPSHOT.jar
  if [ ! -f ${compiler_jar} ]; then
    echo "Compiler jar = ${compiler_jar} not exists."
    echo "Fail to find compiler jar."
    exit 1
  fi
  # add extrac_key_value_config
  extra_config="name:${procedure_name}"
  extra_config="${extra_config},description:${procedure_description}"

  cmd="java -cp ${GIE_HOME}/compiler/target/libs/*:${compiler_jar}"
  cmd="${cmd} -Dgraph.schema=${graph_schema_path}"
  cmd="${cmd} -Djna.library.path=${GIE_HOME}/executor/ir/target/release/"
  cmd="${cmd} com.alibaba.graphscope.common.ir.tools.GraphPlanner ${ir_compiler_properties} ${real_input_path} ${real_output_path} ${real_output_yaml} '${extra_config}'"
  echo "running physical plan genration with ${cmd}"
  eval ${cmd}

  echo "---------------------------"
  #check output
  if [ ! -f ${real_output_path} ]; then
    echo "Output file = ${output_path} not exists."
    echo "Fail to generate physical plan."
    exit 1
  fi

  #check output yaml file
  if [ ! -f ${real_output_yaml} ]; then
    echo "Output yaml file = ${output_yaml_file} not exists."
    echo "Fail to generate physical plan."
    exit 1
  fi
}

compile_hqps_so() {
  #check input params size eq 2 or 3
  if [ $# -gt 8 ] || [ $# -lt 5 ]; then
    echo "Usage: $0 <input_file> <work_dir> <ir_compiler_properties_file>"
    echo "           <graph_schema_file> <GIE_HOME> "
    echo "          [output_dir] [stored_procedure_name] [stored_procedure_description]"
    exit 1
  fi
  input_path=$1
  work_dir=$2
  ir_compiler_properties=$3
  graph_schema_path=$4
  gie_home=$5
  if [ $# -ge 6 ]; then
    output_dir=$6
  else
    output_dir=${work_dir}
  fi

  if [ $# -ge 7 ]; then
    procedure_name=$7
  else
    procedure_name=""
  fi

  if [ $# -ge 8 ]; then
    procedure_description=$8
  else
    procedure_description=""
  fi

  echo "Input path = ${input_path}"
  echo "Work dir = ${work_dir}"
  echo "ir compiler properties = ${ir_compiler_properties}"
  echo "graph schema path = ${graph_schema_path}"
  echo "GIE_HOME = ${gie_home}"
  echo "Output dir = ${output_dir}"
  echo "Procedure name = ${procedure_name}"
  echo "Procedure description = ${procedure_description}"

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
    echo "Generating code from cypher query, procedure name: ${procedure_name}, description: ${procedure_description}"
    # first do .cypher to .pb
    output_pb_path="${cur_dir}/${procedure_name}.pb"
    output_yaml_path="${cur_dir}/${procedure_name}.yaml"
    cypher_to_plan ${procedure_name} ${input_path} ${output_pb_path} \
      ${output_yaml_path} ${ir_compiler_properties} ${graph_schema_path} ${gie_home} \
      ${procedure_name} "${procedure_description}"

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
  cmd="cmake . -DQUERY_NAME=${procedure_name} -DFLEX_INCLUDE_PREFIX=${FLEX_INCLUDE} -DFLEX_LIB_DIR=${FLEX_LIB_DIR}"
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
    echo "Output dir ${output_dir} already contains ${procedure_name}.so"
    echo "Please remove it first."
    exit 1
  fi
  cp ${output_so_path} ${output_dir}
  #check dst_so_path exists
  if [ ! -f ${dst_so_path} ]; then
    echo "Copy failed, ${dst_so_path} not exists."
    exit 1
  fi
  # copy the generated yaml
  cp ${output_yaml_path} ${output_dir}
  if [ ! -f ${dst_yaml_path} ]; then
    echo "Copy failed, ${dst_yaml_path} not exists."
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
    --procedure_name=*)
      PROCEDURE_NAME="${i#*=}"
      shift # past argument=value
      ;;
    --procedure_desc=*)
      PROCEDURE_DESCRIPTION="${i#*=}"
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
  echo "Procedure name         ="${PROCEDURE_NAME}
  echo "Procedure description  ="${PROCEDURE_DESCRIPTION}

  # check input exist
  if [ ! -f ${INPUT} ]; then
    echo "Input file ${INPUT} not exists."
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
    compile_hqps_so ${INPUT} ${WORK_DIR} ${IR_CONF} ${GRAPH_SCHEMA_PATH} ${GIE_HOME} ${OUTPUT_DIR} ${PROCEDURE_NAME} "${PROCEDURE_DESCRIPTION}"

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

if [ $# -lt 5 ]; then
  echo "only receives: $# args"
  usage
  exit 1
fi

run "$@"
