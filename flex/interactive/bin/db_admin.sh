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


# The product name
DB_PROD_NAME="interactive"

# colored error and info functions to wrap messages.
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
err() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] -ERROR- $* ${NC}" >&2
}

info() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

################## Some Util Functions ##################
function parse_yaml {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}
# check if the file exists, if not, exit.
function check_file_exists(){
  if [ ! -f "$1" ]; then
    err "file $1 not exists"
    exit 1
  fi
}

################## Get Docker Workspace ##################
function get_docker_workspace_from_yaml(){
  local yaml_file=$1
  check_file_exists "${yaml_file}"
  local tmp=$(parse_yaml "${yaml_file}")
  # info "tmp = ${tmp}"
  local docker_workspace=$(parse_yaml "${yaml_file}" | grep "directories_workspace" | awk -F "=" '{print $2}')
  if [ -z "${docker_workspace}" ]; then
    err "Can not find docker workspace from yaml file: ${yaml_file}"
    exit 1
  fi
  # trim docker_workspace,
  docker_workspace=$(echo "${docker_workspace}" | sed 's/^[[:space:]]*//g' | sed 's/[[:space:]]*$//g')
  # remove the quotes if presents
  docker_workspace=$(echo "${docker_workspace}" | sed 's/^"//' | sed 's/"$//')
  info "Found docker db home: ${docker_workspace}"
  export DOCKER_DB_HOME="${docker_workspace}"
}

HOST_DB_HOME="$(
  cd "$(dirname "$0")/../" >/dev/null 2>&1
  pwd -P
)"
info "HOST_DB_HOME = ${HOST_DB_HOME}"

####################  DEFINE CONSTANTS ####################
GRAPHSCOPE_GROUP_ID=1001

# the configuration directory
HOST_DB_CONF_DIR="${HOST_DB_HOME}/conf"
# the data directory
HOST_DB_DATA_DIR="${HOST_DB_HOME}/data"
# the log directory
HOST_DB_LOG_DIR="${HOST_DB_HOME}/logs"
HOST_DB_SERVER_OUTPUT_LOG="${HOST_DB_LOG_DIR}/server.log"
HOST_DB_COMPILER_OUTPUT_LOG="${HOST_DB_LOG_DIR}/compiler.log"
HOST_DB_INTERACTIVE_YAML="${HOST_DB_CONF_DIR}/interactive.yaml"
HOST_DB_EXAMPLE_DATASET_DIR=${HOST_DB_HOME}/"examples/sf0.1-raw/"
HOST_DB_RUNNING_FILE="${HOST_DB_HOME}/.running"
# will export DOCKER_DB_HOME, if not set, exist
get_docker_workspace_from_yaml "${HOST_DB_INTERACTIVE_YAML}"

DOCKER_DB_GRAPHSCOPE_HOME="/home/graphscope/GraphScope"
DOCKER_DB_DATA_DIR="${DOCKER_DB_HOME}/data"
DOCKER_DB_LOG_DIR="${DOCKER_DB_HOME}/logs"
DOCKER_DB_CONF_DIR="${DOCKER_DB_HOME}/conf"
DOCKER_DB_IR_CONF_FILE="${DOCKER_DB_HOME}/conf/interactive.properties"
DOCKER_DB_GIE_HOME="${DOCKER_DB_GRAPHSCOPE_HOME}/interactive_engine/"
DOCKER_DB_INTERACTIVE_YAML="${DOCKER_DB_HOME}/conf/interactive.yaml"
DOCKER_DB_SERVER_BIN="${DOCKER_DB_GRAPHSCOPE_HOME}/flex/build/bin/sync_server"
DOCKER_DB_COMPILER_BIN="com.alibaba.graphscope.GraphServer"
DOCKER_DB_GEN_BIN="${DOCKER_DB_GRAPHSCOPE_HOME}/flex/bin/load_plan_and_gen.sh"
DOCKER_DB_SERVER_OUTPUT_LOG=${DOCKER_DB_LOG_DIR}/server.log
DOCKER_DB_COMPILER_OUTPUT_LOG=${DOCKER_DB_LOG_DIR}/compiler.log
export DOCKER_DB_CONNECTOR_PORT=7687
DB_CONNECT_DEFAULT_PORT=7687
# update the port by parsing the yaml file
DOCKER_DB_CONNECTOR_PORT=$(parse_yaml "${HOST_DB_INTERACTIVE_YAML}" | grep "compiler_endpoint_boltConnector_port" | awk -F "=" '{print $2}')
#remove "" and space
DOCKER_DB_CONNECTOR_PORT=$(echo "${DOCKER_DB_CONNECTOR_PORT}" | sed 's/^"//' | sed 's/"$//')

EXAMPLE_DATA_SET_URL="https://github.com/GraphScope/gstest.git"

################### IMAGE VERSION ###################
GIE_DB_IMAGE_VERSION="v0.0.1"
GIE_DB_IMAGE_NAME="registry.cn-hongkong.aliyuncs.com/graphscope/${DB_PROD_NAME}"
GIE_DB_CONTAINER_NAME="${DB_PROD_NAME}-server"


####################  DEFINE FUNCTIONS ####################
function check_running_containers_and_exit(){
  # check if there is any running containers
  info "Check running containers and exit"
  running_containers=$(docker ps -a --format "{{.Names}}" | grep "${GIE_DB_CONTAINER_NAME}")
  if [ -n "${running_containers}" ]; then
    err "There are running containers: ${running_containers}, please stop them first."
    exit 1
  fi
  info "finish check"
}

function check_container_running(){
  if [ "$(docker inspect -f '{{.State.Running}}' "${GIE_DB_CONTAINER_NAME}")" = "true" ]; then
    info "container ${GIE_DB_CONTAINER_NAME} is running"
  else
    info "container ${GIE_DB_CONTAINER_NAME} is not running"
    # start the container
    docker start "${GIE_DB_CONTAINER_NAME}"
  fi
}

function ensure_container_running(){
  if [ "$(docker inspect -f '{{.State.Running}}' "${GIE_DB_CONTAINER_NAME}")" = "true" ]; then
    info "container ${GIE_DB_CONTAINER_NAME} is running"
  else
    info "container ${GIE_DB_CONTAINER_NAME} is not running"
    # start the container
    docker start "${GIE_DB_CONTAINER_NAME}"
  fi
}

function check_process_running_in_container(){
  local container_name=$1
  local process_name=$2
  local error_msg=$3
  local process_id=$(docker top  "${container_name}" | grep "${process_name}" | awk '{print $2}\')
  if [ -z "${process_id}" ]; then
    err "process ${process_name} is not running in container ${container_name}"
    err "${error_msg}"
    exit 1 
  fi
  info "process ${process_name} is running in container ${container_name}, process id is ${process_id}"
}


####################  DEFINE USAGE ####################
# parse the args and set the variables.
function usage() {
  init_usage
  start_usage
  stop_usage
  restart_usage
  compile_usage
  show_stored_procedure_usage
  download_dataset_usage
  destroy_usage
}

function init_usage() {
  cat << EOF
    db_admin.sh init -p[---publish] <host_port1:container_port1> 
                      -v[--volume] <host_dir:container_dir> 
                      --version <image_version(can skip to use default)>
                Init the database, create the containers. --publish and --volume can be used multiple times.
EOF
}

function start_usage() {
  cat << EOF
    db_admin.sh start -n [--name] <graph_name> -b [--bulk-load] <bulk load yaml file> -r[--root-data-dir] <path to root data dir>
                Start the database with the given graph. graph schema file should be placed at ./data/{graph_name}/graph.yaml.
                If mode is override, we need to clear the data directory first.
EOF
}

function stop_usage() {
  cat << EOF
    db_admin.sh stop
                Stop the database with the given graph.
EOF
}

function restart_usage() {
  cat << EOF
    db_admin.sh restart
                Restart the database with current running graph.
EOF
}

function compile_usage(){
  cat << EOF
    db_admin.sh compile -g[--graph] <graph_name> -i <sourcefile[.cc, .cypher]
                compile cypher/.cc to dynamic library, according to the schema of graph. The output library will be placed at ./data/{graph_name}/lib.
EOF
}

function show_stored_procedure_usage(){
  cat << EOF
    db_admin.sh show_stored_procedure -n[--name] graph_name
                show all stored procedure
EOF
}

function download_dataset_usage(){
  cat << EOF
    db_admin.sh download_dataset 
                download the example dataset.
EOF
}

function destroy_usage() {
  cat << EOF
    db_admin.sh destroy
                Destroy the current database, remove the container.
EOF
}



####################  Init database ####################
# Init the current data base.
# create a user with same user id in container
function do_init(){
  # check running containers and exit
  check_running_containers_and_exit
  info "No containers running, start init database..."
  # if no containers running, procede to init
  mount_cmd=""
  port_cmd=""

  # now parse args
  while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    -p | --publish)
      publish="$2"
      shift # past argument
      shift
      port_cmd="${port_cmd} -p ${publish}"
      ;;
    -v | --volume)
      volume="$2"
      shift
      shift
      mount_cmd="${mount_cmd} -v ${volume}"
      ;;
    --version)
      version="$2"
      shift
      shift
      ;;
    *)
      err "unknown option $1"
      init_usage
      exit 1
      ;;
    esac
  done
  # we need to mount db_home to workspace additionaly 
  mount_cmd="${mount_cmd} -v ${HOST_DB_HOME}:${DOCKER_DB_HOME}"
  port_cmd="${port_cmd} -p ${DB_CONNECT_DEFAULT_PORT}:${DOCKER_DB_CONNECTOR_PORT}"

  GIE_DB_IMAGE_NAME_TAG="${GIE_DB_IMAGE_NAME}:${GIE_DB_IMAGE_VERSION}"
  cmd="docker run -it -d --privileged --name ${GIE_DB_CONTAINER_NAME} ${port_cmd} ${mount_cmd} ${GIE_DB_IMAGE_NAME_TAG} bash"
  
  info "Running cmd: ${cmd}"
  eval ${cmd} || docker rm "${GIE_DB_CONTAINER_NAME}"
  info "Finish init database"
  # get uid 
  local uid=$(id -u)
  local gid=$(id -g)
  # get group name
  local group_name=$(id -gn)
  # get username
  local username=$(id -un)
  # create the group_name in container, if not exists
  docker exec "${GIE_DB_CONTAINER_NAME}" bash -c "sudo groupadd -g ${gid} ${group_name}" || docker rm "${GIE_DB_CONTAINER_NAME}"
  # add graphscope to the group
  docker exec "${GIE_DB_CONTAINER_NAME}" bash -c "sudo usermod -a -G ${group_name} graphscope" || docker rm "${GIE_DB_CONTAINER_NAME}"
  info "Finish setting up database."
}

####################  Destroy ####################
function do_destroy() {
  info "Destroy database"
  docker stop "${GIE_DB_CONTAINER_NAME}"
  docker rm "${GIE_DB_CONTAINER_NAME}"
  info "Finish destroy database"
}

####################  Start database ####################
function do_start(){
  do_stop
  ensure_container_running
  GRAPH_NAME=""
  BULK_LOAD_FILE=""
  ROOT_DATA_DIR=""
  # these path should be relative to ${DOCKER_DB_HOME}
  while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    -n | --name)
      graph_name="$2"
      shift # past argument
      shift
      GRAPH_NAME="${graph_name}"
      ;;
    -b | --bulk-load)
      bulk_load="$2"
      shift
      shift
      BULK_LOAD_FILE="${bulk_load}"
      ;;
    -r | --root-data-dir)
      root_data_dir="$2"
      shift
      shift
      ROOT_DATA_DIR="${root_data_dir}"
      ;;
    *)
      err "unknown option $1"
      start_usage
      exit 1
      ;;
    esac
  done
  if [ -z "${GRAPH_NAME}" ]; then
    err "graph name is not specified"
    start_usage
    exit 1
  fi
  if [ -z "${BULK_LOAD_FILE}" ]; then
    err "bulk load file is not specified"
    start_usage
    exit 1
  fi
  # the bulk_load_file shoud place inside ${DOCKER_DB_HOME}. and should use relative path
  info "In start datebase, received graph_name = ${GRAPH_NAME}, bulk_load_file = ${BULK_LOAD_FILE}, root_data_dir = ${ROOT_DATA_DIR}"
  graph_schema_file="${DOCKER_DB_DATA_DIR}/${GRAPH_NAME}/graph.yaml"
  graph_json_file="${DOCKER_DB_DATA_DIR}/${GRAPH_NAME}/graph.json"
  full_bulk_load_file="${DOCKER_DB_HOME}/${BULK_LOAD_FILE}"
  csr_data_dir="${DOCKER_DB_DATA_DIR}/${GRAPH_NAME}/indices"
  docker_graph_plugin_dir=${DOCKER_DB_DATA_DIR}/${GRAPH_NAME}/plugins/
  host_graph_plugin_dir=${HOST_DB_DATA_DIR}/${GRAPH_NAME}/plugins/
  cmd="docker exec ${GIE_DB_CONTAINER_NAME} bash -c \""
  cmd="${cmd} ${DOCKER_DB_SERVER_BIN} -c ${DOCKER_DB_INTERACTIVE_YAML}"
  # if BULK_LOAD_FILE is not given, we will use existing graph indices
  if [ -n "${BULK_LOAD_FILE}" ]; then
    cmd="${cmd}  -l ${full_bulk_load_file}"
  fi
  cmd="${cmd} -g ${graph_schema_file}  --data-path ${csr_data_dir}"
  cmd="${cmd} -i ${DOCKER_DB_IR_CONF_FILE} -z ${graph_json_file} --gie-home ${DOCKER_DB_GIE_HOME}"
  # if graph_plugin_dir exits
  if [ -d "${host_graph_plugin_dir}" ]; then
    cmd="${cmd} --plugin-dir ${docker_graph_plugin_dir}"
  else 
    info "No plugin dir found: ${host_graph_plugin_dir}"
  fi

  if [ -n "${ROOT_DATA_DIR}" ]; then
    # ROOT_DATA_DIR is a relative path, prepend DOCKER_DB_HOME
    ROOT_DATA_DIR="${DOCKER_DB_HOME}/${ROOT_DATA_DIR}"
    export FLEX_DATA_DIR="${ROOT_DATA_DIR}"
  fi
  cmd="${cmd} > ${DOCKER_DB_SERVER_OUTPUT_LOG} 2>&1  & \""
  echo "Running cmd: ${cmd}"
  # eval command, if fails exist
  eval ${cmd} || exit 1
  sleep 4
  # check whether the process is running
  check_process_running_in_container ${GIE_DB_CONTAINER_NAME} ${DOCKER_DB_SERVER_BIN} "check ${HOST_DB_SERVER_OUTPUT_LOG} to see more details"
  info "Successfuly start server"

  # start compiler
  cmd="docker exec ${GIE_DB_CONTAINER_NAME} bash -c \""
  cmd=${cmd}"java -cp \"${DOCKER_DB_GIE_HOME}/compiler/target/libs/*:${DOCKER_DB_GIE_HOME}/compiler/target/compiler-0.0.1-SNAPSHOT.jar\" "
  cmd=${cmd}" -Djna.library.path=${DOCKER_DB_GIE_HOME}/executor/ir/target/release"
  cmd=${cmd}" -Dgraph.schema=${graph_json_file}"
  #graph.stored.procedures.uri
  if [ -d "${host_graph_plugin_dir}" ]; then
    cmd=${cmd}" -Dgraph.stored.procedures.uri=file:${docker_graph_plugin_dir}"
  fi
  cmd=${cmd}" ${DOCKER_DB_COMPILER_BIN} ${DOCKER_DB_IR_CONF_FILE} > ${DOCKER_DB_COMPILER_OUTPUT_LOG} 2>&1 &"
  cmd=${cmd}"\""
  info "Running cmd: ${cmd}"
  eval ${cmd}
  sleep 6
  check_process_running_in_container ${GIE_DB_CONTAINER_NAME} ${DOCKER_DB_COMPILER_BIN} "check ${HOST_DB_COMPILER_OUTPUT_LOG} to see more details"
  info "Successfuly start compiler"
  info "DataBase service is running..., port is open on :${DOCKER_DB_CONNECTOR_PORT}"

  # if do_start success, we should write current args to ${HOST_DB_RUNNING_FILE}
  echo "GRAPH_NAME=${GRAPH_NAME}" > ${HOST_DB_RUNNING_FILE}
  echo "BULK_LOAD_FILE=${BULK_LOAD_FILE}" >> ${HOST_DB_RUNNING_FILE}
  echo "ROOT_DATA_DIR=${root_data_dir}" >> ${HOST_DB_RUNNING_FILE}
#  info "Successfuly write running args to ${HOST_DB_RUNNING_FILE}"
}


####################  Stop database ####################
function do_stop(){
  # stop the container 
  docker stop ${GIE_DB_CONTAINER_NAME}
  info "Successfuly stop database"
}


####################  Get database status ####################
function do_status() {
  if [ "$(docker inspect -f '{{.State.Running}}' "${GIE_DB_CONTAINER_NAME}")" = "true" ]; then
    info "container ${GIE_DB_CONTAINER_NAME} is running"
  else
    info "container ${GIE_DB_CONTAINER_NAME} is not running"
    info "Please start database first"
  fi
  # the container is running but the process is not running
  check_process_running_in_container ${GIE_DB_CONTAINER_NAME} ${DOCKER_DB_SERVER_BIN}  "check ${HOST_DB_SERVER_OUTPUT_LOG} to see more details"
  check_process_running_in_container ${GIE_DB_CONTAINER_NAME} ${DOCKER_DB_COMPILER_BIN} "check ${HOST_DB_COMPILER_OUTPUT_LOG} to see more details"
  info "Database service is running..., port is open on :${DOCKER_DB_CONNECTOR_PORT}"
}


####################  Download dataset ####################
function do_download_dataset(){
  git clone ${EXAMPLE_DATA_SET_URL} ${HOST_DB_EXAMPLE_DATASET_DIR}
  info "Successfuly download dataset to: ${HOST_DB_EXAMPLE_DATASET_DIR}"
}


####################  Restart ####################
function do_restart() {
  # if the container is not running, exit
  if [ "$(docker inspect -f '{{.State.Running}}' "${GIE_DB_CONTAINER_NAME}")" = "false" ]; then
    info "container ${GIE_DB_CONTAINER_NAME} is not running"
    info "Please start database first"
    exit 1
  fi
  info "Stopping database first..."
  do_stop
  info "Successfuly stop database"
  # read args from cached file.
  # get num lines in file ${HOST_DB_RUNNING_FILE}
  num_lines=$(wc -l < ${HOST_DB_RUNNING_FILE})
  if [ ${num_lines} -ne 3 ]; then
    err "Error: ${HOST_DB_RUNNING_FILE} should have 3 lines, but got ${num_lines}, something wrong with the file ${HOST_DB_RUNNING_FILE}"
    exit 1
  fi
  # read args from file
  GRAPH_NAME=$(sed -n '1p' ${HOST_DB_RUNNING_FILE} | cut -d '=' -f 2)
  BULK_LOAD_FILE=$(sed -n '2p' ${HOST_DB_RUNNING_FILE} | cut -d '=' -f 2)
  ROOT_DATA_DIR=$(sed -n '3p' ${HOST_DB_RUNNING_FILE} | cut -d '=' -f 2)
  do_start -n ${GRAPH_NAME} -b ${BULK_LOAD_FILE} -r ${ROOT_DATA_DIR}
  info "Finish restart database"
}

# the compiled dynamic libs will be placed at data/${graph_name}/plugins/
# after compilation, the user need to write the cooresponding yaml, telling the compiler about 
# the input and output of the stored procedure
function do_compile() {
    # check args num == 4
  # start container
  ensure_container_running
  if [ $# -ne 4 ]; then
    err "stored_procedure command need 2 args, but got $#"
    compile_usage
    exit 1
  fi
  graph_name=""
  file_path="" # file path
  output_dir=""

  while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    -g | --graph)
      graph_name="$2"
      info "graph_name = ${graph_name}"
      shift # past argument
      shift
      ;;
    -i | --input)
      file_path="$2"
      shift # past argument
      shift
      ;;
    *)
      err "unknown option $1"
      compile_usage
      exit 1
      ;;
    esac
  done

  # check graph_name 
  if [ -z "${graph_name}" ]; then
    err "graph_name is empty"
    compile_usage
    exit 1
  fi

  # check file_path 
  if [ -z "${file_path}" ]; then
    err "file_path is empty"
    compile_usage
    exit 1
  fi

  # get real file_path
  file_name=$(basename "${file_path}")
  real_file_path=$(realpath "${file_path}")
  # check exists
  if [ ! -f "${real_file_path}" ]; then
    err "file ${real_file_path} not exist"
    exit 1
  fi
  # check graph dir exists
  graph_dir="${HOST_DB_HOME}/data/${graph_name}"
  if [ ! -d "${graph_dir}" ]; then
    err "graph ${graph_name} not exist"
    exit 1
  fi
  mkdir -p "${graph_dir}/plugins"

  DOCKER_OUTPUT_DIR="${DOCKER_DB_HOME}/data/${graph_name}/plugins"
  HOST_OUTPUT_DIR="${HOST_DB_HOME}/data/${graph_name}/plugins"
  DOCKER_DB_GRAPH_SCHEMA="${DOCKER_DB_HOME}/data/${graph_name}/graph.json"
  DOCKER_REAL_FILE_PATH="/tmp/${file_name}"
  # docker cp file to container
  cmd="docker cp ${real_file_path} ${GIE_DB_CONTAINER_NAME}:${DOCKER_REAL_FILE_PATH}"
  eval ${cmd} || exit 1

  cmd="docker exec ${GIE_DB_CONTAINER_NAME} bash -c \""
  cmd=${cmd}" ${DOCKER_DB_GEN_BIN}"
  cmd=${cmd}" --engine_type=hqps"
  cmd=${cmd}" --input=${DOCKER_REAL_FILE_PATH}"
  cmd=${cmd}" --work_dir=/tmp/codegen/"
  cmd=${cmd}" --ir_conf=${DOCKER_DB_IR_CONF_FILE}"
  cmd=${cmd}" --graph_schema_path=${DOCKER_DB_GRAPH_SCHEMA}"
  cmd=${cmd}" --gie_home=${DOCKER_DB_GIE_HOME}"
  cmd=${cmd}" --output_dir=${DOCKER_OUTPUT_DIR}"
  cmd=${cmd}" \""

  echo "Running cmd: ${cmd}"
  eval ${cmd} || exit 1
  # check output exists
  # get the file_name of file_path
  file_name="${file_name%.*}"
  output_file="${HOST_OUTPUT_DIR}/lib${file_name}.so"

  if [ ! -f "${output_file}" ]; then
    err "output file ${output_file} not exist, compilation failed"
    exit 1
  fi
  info "success generate dynamic lib ${output_file}, please create the cooresponding yaml file ${HOST_OUTPUT_DIR}/${file_name}.yaml."
}

####################  Entry ####################
if [ $# -eq 0 ]; then
  usage
  exit 1
fi

while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  -h | --help)
    usage
    exit
    ;;
  init)
    shift
    info "Start initiating database..."
    do_init "$@"
    exit 0
    ;;
  start)
    shift
    info "Start database service..."
    do_start "$@"
    exit 0
    ;;
  status)
    shift
    do_status "$@"
    exit 0
    ;;
  stop)
    shift
    do_stop "$@"
    exit 0
    ;;
  restart)
    shift 
    do_restart # restart current graph
    exit 0
    ;;
  compile)
    shift
    do_compile "$@"
    exit 0
    ;;
  show_stored_procedure)
    shift
    do_show_stored_procedure "$@"
    exit 0
    ;;
  destroy)
    shift
    do_destroy "$@"
    exit 0
    ;;
  download_dataset)
    shift
    do_download_dataset
    exit 0
    ;;
  *) # unknown option
    err "unknown option $1"
    usage
    exit 1
    ;;
  esac
done




