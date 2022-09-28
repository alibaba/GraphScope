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
HOST_SLOT=$1
shift
VD_CLASS=$1
shift
ED_CLASS=$1
shift
MSG_CLASS=$1
shift
SERIAL_PATH=$1
shift
FRAG_IDS=$1
shift
MAX_ITERATION=$1
shift
NUM_PART=$1
shift
IPC_SOCKET=$1
shift
USER_JAR_PATH=$1
shift

echo "task                "${TASK}
echo "num workers:        "${NUM_WORKERS}
echo "host file           "${HOST_SLOT} # host name with slots
echo "vd class            "${VD_CLASS}
echo "ed class            "${ED_CLASS}
echo "msg class           "${MSG_CLASS}
echo "serial path         "${SERIAL_PATH}
echo "frag ids:           "${FRAG_IDS}
echo "max iter            "${MAX_ITERATION}
echo "num part:           "${NUM_PART}
echo "ipc socket          "${IPC_SOCKET}
echo "user jar path       "${USER_JAR_PATH}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ -z "${USER_JAR_PATH}" ]; then
    echo "USER_JAR_PATH need to be set"
    exit 1;
fi
export USER_JAR_PATH=${USER_JAR_PATH}

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

# 1.first distribute serialization file
array=($(echo "${HOST_SLOT}" | tr ',' '\n'))
for host in ${array};
do
    echo ${host}
    splited=($(echo "$host" | tr ':' '\n'))
    echo "scp to host "${splited[0]}
    scp ${SERIAL_PATH} ${splited[0]}:${SERIAL_PATH}
done

cmd="GLOG_v=10 ${MPIRUN_EXECUTABLE} -n ${NUM_WORKERS} -host ${HOST_SLOT} -x GLOG_v \
-x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} \
--task ${TASK} \
--vd_class ${VD_CLASS} --ed_class ${ED_CLASS} --msg_class ${MSG_CLASS} \
--serial_path ${SERIAL_PATH} --frag_ids ${FRAG_IDS} \
--max_iterations ${MAX_ITERATION} --num_part ${NUM_PART} --ipc_socket ${IPC_SOCKET}"
echo "running cmd: "$cmd >&2
eval $cmd
