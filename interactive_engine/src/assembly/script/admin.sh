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

# set envs
osname=`uname`
if [ "$osname" = "Linux" ]; then
    BIN_DIR=$(cd `dirname $(readlink $0||echo $0)`; pwd)
    [ ! -z $JAVA_HOME ] || JAVA_HOME="/opt/taobao/java"
elif [ "$osname" = "Darwin" ]; then
    BIN_DIR=$(cd `dirname $(readlink $0||echo $0)`; pwd)
    [ ! -z $JAVA_HOME ] || JAVA_HOME=`/usr/libexec/java_home`
else
    [ ! -z $JAVA_HOME ] || JAVA_HOME="/opt/taobao/java"
    BIN_DIR=$( cd "$( dirname "$0" )" && pwd )
fi

show_usage() {
    echo "admin tool shell usage:  ./admin.sh shell [option]"
    echo "show help: "
    echo "    ./admin.sh shell help"
    echo "clean maxgraph data:"
    echo "    ./admin.sh shell --clean_log_dir /maxgraph_path/to/clean "
    echo "clean pangu data:"
    echo "    ./admin.sh shell --clean_pangu /maxgraph  --studio http://daily.maxgraph.alibaba.net/openApi/sys/instances/"
    exit 0
}

shell_func_clean_data() {
    # # clean logs
    if [[ ! -d ${1} ]]; then
        echo "[ERROR] CLEAN_LOG_DIR path not exist: " "$1"
        show_usage
    fi
    echo "[INFO] clean log under path : data_path: " "$1"

    # # delete files as this pattern
    # example :  binary_launcher.stderr.out.NUMBER
    # find modify time option : -mtime +1
    files_to_delete=( $(find "$1" "*.out.[0-9]*" -or -name "*.log.[0-9]*" -or -name "*.LOG.[0-9]*" -or -name "*debug_env.*" -or -name "*.err.[0-9]*" -mtime +1) )

    echo "[INFO] find following file to delete: "
    for j in ${!files_to_delete[@]}; do
        echo "[INFO] "${files_to_delete[$j]}
    done

    echo "[INFO] delete file: "
    for j in ${!files_to_delete[@]}; do
        echo "[INFO] "${files_to_delete[$j]}
        rm -f ${files_to_delete[$j]}
    done

    exit 0
}

shell_func_clean_pangu() {
    # # clean pangu data DIR at AG machine:
    # [DIR PATTERN]:  /PANGU_PATH/GRAPH_NAME_INSTANCEID
    # for example:
    # $/apsara/deploy/pu ls /maxgraph
    #     /maxgraph/graph_demo1_E9056737C97611E9B56800163E007015/
    # [NOTE] check MaxGraphStatus::InstanceStatus


    PU_BINARY="/apsara/deploy/pu"
    MAXGRAPH_PANGU_ROOT=$1
    MAXGRAPH_STUDIO_URL=$2
    if [ ! -f $PU_BINARY ]; then
        echo "[ERROR] $PU_BINARY not exist"
        exit -1
    fi

    if ! type curl > /dev/null; then
        echo "[ERROR] curl command not exist"
        exit -1
    fi

    echo "[INFO] clean pangu maxgraph under data_path : " $1

    maxgraph_dir_array=($($PU_BINARY ls $MAXGRAPH_PANGU_ROOT | awk '{print $1}'))

    echo "[INFO] find under data_path graph instance #:" ${#maxgraph_dir_array[@]}

    for elem in "${maxgraph_dir_array[@]}"; do
        if [ ! -z "$elem" ]; then
            instance_id=`echo "$elem" | tr -d "\/" | rev | cut -d'_' -f 1 | rev`
            echo "[INFO] >>> check studio for magraph:" $elem", instance_id: " $instance_id

            if [[ ! "$instance_id" =~ [a-zA-Z0-9]{32} ]]; then
                echo "skip graph, graph instance_id pattern not match:" $instance_id
                continue
            fi

            studio_check_url=$MAXGRAPH_STUDIO_URL"/"$instance_id"/exist"
            if [[ $MAXGRAPH_STUDIO_URL == */ ]]; then
              studio_check_url=$MAXGRAPH_STUDIO_URL$instance_id"/exist"
            fi
            echo "[INFO] studo url:" $studio_check_url

            set -x
            status_result=`curl -o /dev/null -s -w '%{http_code}\n' -s  $studio_check_url`
            set +x
        else
            continue
        fi

        echo "[INFO] check content status:" $status_result

        this_dir=${MAXGRAPH_PANGU_ROOT}"/"$elem
        if [[ $MAXGRAPH_PANGU_ROOT == */ ]]; then
            this_dir=${MAXGRAPH_PANGU_ROOT}$elem
        fi
        echo "[INFO] check instance dir" $this_dir

        if  [ "200" == $status_result ]; then
            status_content=`curl -s $studio_check_url`
            if [[ $status_content == *"\"errCode\":7002"* ]] && [[ $status_content == *"\"success\":false"* ]] && [[ $status_content == *"\"errMsg\":\"instance not found\""* ]] ; then
                echo "[INFO] studio result, instance content:"$status_content", prepare to [DELETE] this dir:" $this_dir
                $PU_BINARY rmdir -f $this_dir
                if [ $? -eq 0 ]; then
                    echo "[INFO] pangu remove dir ok:" $this_dir
                fi
            else
                echo "[INFO] status result, status_content:" $status_result ", [SKIP]:"$this_dir
            fi
        else
            echo "[INFO] status result, instance_status:" $status_result ", [SKIP]:"$this_dir
        fi

        # sleep some time
        sleep 1

    done

    exit 0
}

action_java() {
    BASE_DIR=`cd "${BIN_DIR}/.."; pwd`
    LIB_DIR=`cd "${BIN_DIR}/../lib"; pwd`
    CONF_DIR=`cd "${BIN_DIR}/../conf"; pwd`
    mkdir -p ${BASE_DIR}/logs
    LOG_DIR=`cd "${BASE_DIR}/logs"; pwd`


    if [[ ! -d ${JAVA_HOME} ]]; then
        echo "[ERROR] JAVA_HOME path not exist"
        exit 1
    fi
    echo "[INFO] execute admin java tool, argument: " "$@"
    # set JAVA_OPTS
    JAVA_OPTS="${JAVA_OPTS} -client -Dlog4j.configurationFile=file:${CONF_DIR}/log4j2.xml -classpath ./:${LIB_DIR}/* -Dlogfilename=${LOG_DIR}/maxgraph-tool.log -Dlogbasedir=${LOG_DIR}"

    exec ${JAVA_HOME}/bin/java ${JAVA_OPTS} com.alibaba.maxgraph.tools.admin.AdminCli "$@"
}


action_shell() {
    # find & clean log file for maxgraph
    echo "[INFO] execute shell, argument: " "$@"

    # parse argument
    while true ; do
      case "$1" in
        --clean_log_dir)
          CLEAN_DATA="$2"
          shift 2
          ;;
        --clean_pangu)
          CLEAN_PANGU="$2"
          shift 2
          ;;
        --studio)
          STUDIO_URL="$2"
          shift 2
          ;;
        --help|-h)
          show_usage
          ;;
        shell)
          shift
          continue
          ;;
        *) break ;;
      esac
    done

    if [ ! -z $CLEAN_DATA ]; then
        shell_func_clean_data "$CLEAN_DATA"
    elif [ ! -z $CLEAN_PANGU ] && [ ! -z $STUDIO_URL ]; then
        shell_func_clean_pangu "$CLEAN_PANGU" "$STUDIO_URL"
    else
        show_usage
    fi

}

# main entrance
case "$1" in
    shell)
        # run shell tool
        action_shell "$@"
        ;;
    *)
        # run java tool
        action_java "$@"
        exit 1;
        ;;
esac
