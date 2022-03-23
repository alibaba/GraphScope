#!/bin/bash

#
# Copyright 2022 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
set -e

readonly RED="\033[0;31m"
readonly YELLOW="\033[1;33m"
readonly GREEN="\033[0;32m"
readonly NC="\033[0m" # No Color

err() {
  echo -e "${RED}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [ERROR] $*${NC}" >&2
}

warning() {
  echo -e "${YELLOW}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [WARNING] $*${NC}" >&1
}

log() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&1
}

succ() {
  echo -e "${GREEN}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*${NC}" >&1
}

get_mpi(){
  if ! command -v mpirun &> /dev/null; then
    export MPIRUN_EXECUTABLE=${GRAPHSCOPE_HOME}/openmpi/bin/mpirun
  else
    export MPIRUN_EXECUTABLE=mpirun
  fi
}

generate_mpi_cmd_with_param(){
  ifconfig_output=$(ifconfig)
  if echo ${ifconfig_output} | grep -q "eth0"; then
    tcp_include="-mca btl_tcp_if_include eth0"
  fi
  if echo ${ifconfig_output} | grep -q "bond0"; then
    tcp_include="-mca btl_tcp_if_include bond0"
  fi
  export MPIRUN_EXECUTABLE="GLOG_v=10 ${MPIRUN_EXECUTABLE} ${tcp_include} "
}

distribute_serial_file(){
  array=($(echo "${HOST_SLOT}" | tr ',' '\n'))
  for host in "${array[@]}";
  do
      echo ${host}
      splited=($(echo "$host" | tr ':' '\n'))
      echo "scp to host "${splited[0]}
      scp ${SERIAL_PATH} ${splited[0]}:${SERIAL_PATH}
  done
}

prepare_enviroment() {
  if [ -z "${GRAPHSCOPE_HOME}" ]; then
    SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    export GRAPHSCOPE_HOME=${SCRIPT_DIR}/../
    echo "using probed GRAPHSCOPE_HOME: "${GRAPHSCOPE_HOME}
  else
    echo "using GRAPHSCOPE_HOME: "${GRAPHSCOPE_HOME}
  fi
  # Check MPI installation
  get_mpi
  source  ${GRAPHSCOPE_HOME}/conf/grape_jvm_opts
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${GRAPHSCOPE_HOME}/lib/:${GRAPHSCOPE_HOME}/lib64
  GRAPHX_RUNNER=${GRAPHSCOPE_HOME}/bin/graphx_runner
}

load_fragment() {
  log "Loading Fragment....."
  prepare_enviroment

  generate_mpi_cmd_with_param
  cmd="${MPIRUN_EXECUTABLE} -n ${NUM_WORKERS} --host ${HOST_SLOT} \
  -x GLOG_v ${GRAPHX_RUNNER} --task ${TASK} --raw_data_ids ${RAW_DATA_IDS} --ipc_socket ${IPC_SOCKET} \
  -vd_class ${VD_CLASS} --ed_class ${ED_CLASS}"
  echo "running cmd: "$cmd >&2
  eval ${cmd}
}

run_pregel() {
  log "Running pregel"
  prepare_enviroment

  if [ -z "${USER_JAR_PATH}" ]; then
    err "USER_JAR_PATH need to be set"
    exit 1;
  fi
  export USER_JAR_PATH=${USER_JAR_PATH}

  if [ -f "${GRAPHX_RUNNER}" ]; then
    echo "graphx_runner exists."
  else
    echo "graphx_runner doesn't exist"
    exit 1;
  fi

  distribute_serial_file
  generate_mpi_cmd_with_param
  cmd="${MPIRUN_EXECUTABLE} -n ${NUM_WORKERS} -host ${HOST_SLOT} -x GLOG_v \
  -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} --task ${TASK} \
  --vd_class ${VD_CLASS} --ed_class ${ED_CLASS} --msg_class ${MSG_CLASS} \
  --serial_path ${SERIAL_PATH} --frag_ids ${FRAG_IDS} \
  --max_iterations ${MAX_ITERATION} --num_part ${NUM_PART} --ipc_socket ${IPC_SOCKET}"
  echo "running cmd: "$cmd >&2
  eval $cmd
}

##########################
# Output useage information.
# Globals:
#   None
# Arguments:
#   None
##########################
usage() {
cat <<END

  A script to run graphx related tasks, load fragment, and graphx pregel.

  Usage: run_graphx options

  Options:
    -h, --help           Print help information
    --task               Specify the task to run this time, load_fragment or run_pregel
    --num-workers        Number of mpi workers to run.
    --host-slot          Host and slot num specification.
    --vd-class           Vertex data class name, only used in run_pregel.
    --ed-class           Edge data class name, only used in run_pregel.
    --msg-class          Msg data class name, only used in run_pregel.
    --serial-path        Serialization path for pregel program, only used in run_pregel.
    --frag-ids           Input fragment ids, only used in run_pregel.
    --raw-data-ids       RawDataIds, only used in load_fragment.
    --max-iteration      Number of max iterations.
    --num-part           Number of graphx rdd partitions.
    --ipc-socket         Vineyard IPC socket to use.
    --user-jar-path      Path to user jar, only used in run_pregel.
END
}

if test $# -eq 0; then
  usage; exit 1;
fi

# parse argv
while test $# -ne 0; do
  arg=$1; 
  shift
  case ${arg} in
    -h|--help)        usage; exit ;;
    --task) 
        TASK="$1"
        shift
        if [[ "${TASK}" == *"load_fragment"* || "${TASK}" == *"run_pregel"* ]];
        then
            log "Runing task:            "${TASK}
        else
            err "Received unrecognized task ${TASK}"
            exit 1
        fi
        ;;
    --num-workers)
        NUM_WORKERS="$1"
        log "Num workers:            "${NUM_WORKERS}
        shift
        ;;
    --host-slot)
        HOST_SLOT="$1"
        shift
        ;;
    --vd-class)
        VD_CLASS="$1"
        log "VD class :              "${VD_CLASS}
        shift
        ;;
    --ed-class)
        ED_CLASS="$1"
        log "ED class :              "${ED_CLASS}
        shift
        ;;
    --msg-class)
        MSG_CLASS="$1"
        log "MSG class :             "${MSG_CLASS}
        shift
        ;;
    --serial-path)
        SERIAL_PATH="$1"
        log "serial path :           "${SERIAL_PATH}
        shift
        ;;
    --frag-ids)
        FRAG_IDS="$1"
        log "frag ids :              "${FRAG_IDS}
        shift
        ;;
    --raw-data-ids)
        RAW_DATA_IDS="$1"
        log "raw dataids :           "${RAW_DATA_IDS}
        shift
        ;;
    --max-iteration)
        MAX_ITERATION="$1"
        log "max iteration :         "${MAX_ITERATION}
        shift
        ;;
    --num-part)
        NUM_PART="$1"
        log "num part :              "${NUM_PART}
        shift
        ;;
    --ipc-socket)
        IPC_SOCKET="$1"
        log "ipc socket :            "${IPC_SOCKET}
        shift
        ;;
    --user-jar-path)
        USER_JAR_PATH="$1"
        log "user jar path :          "${USER_JAR_PATH}
        shift
        ;;
    *)
        echo "unrecognized option '${arg}'"
        usage; 
        exit 1
        ;;
  esac
done

if [[ "${TASK}" == *"load_fragment"* ]];
then
  load_fragment
elif  [[ "${TASK}" == *"run_pregel"* ]];
then
  run_pregel
else
  err "Not possible taks "${TASK}
fi
