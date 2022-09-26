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

TASK=$1
shift
NUM_WORKERS=$1
shift
HOSTS=$1
shift
LOCAL_VM_IDS=$1
shift
IPC_SOCKET=$1
shift

echo "task                    "${TASK}
echo "num workers:            "${NUM_WORKERS}
echo "hosts :                 "${HOSTS}
echo "local vm ids            "${LOCAL_VM_IDS}
echo "ipc socket              "${IPC_SOCKET}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )


if [ -z "${GRAPHSCOPE_HOME}" ]; then
    export GRAPHSCOPE_HOME=${SCRIPT_DIR}/../
    echo "using probed GRAPHSCOPE_HOME: "${GRAPHSCOPE_HOME}
else
    echo "using GRAPHSCOPE_HOME: "${GRAPHSCOPE_HOME}
fi

GRAPHX_RUNNER=${GRAPHSCOPE_HOME}/bin/graphx_runner
MPIRUN_EXECUTABLE=${GRAPHSCOPE_HOME}/openmpi/bin/mpirun
source  ${GRAPHSCOPE_HOME}/conf/grape_jvm_opts
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${GRAPHSCOPE_HOME}/lib/:${GRAPHSCOPE_HOME}/lib64

if [ -f "${GRAPHX_RUNNER}" ]; then
    echo "graphx runner exists."
else
    echo "graphx runner doesn't exist"
    exit 1;
fi

cmd="GLOG_v=10 ${MPIRUN_EXECUTABLE} -n ${NUM_WORKERS} --host ${HOSTS} \
-x GLOG_v ${GRAPHX_RUNNER} --task ${TASK} --local_vm_ids ${LOCAL_VM_IDS} --ipc_socket ${IPC_SOCKET}"
echo "running cmd: "$cmd >&2

eval $cmd
