#!/usr/bin/env bash
#
# graphscope-store command tool

set -x
set -e
set -o pipefail

usage() {
cat <<END
  A script to launch graphscope-store service.

  Usage: store_ctl.sh [options] [command] [parameters]

  Options:

    -h, --help           output help information

  Commands:

    max_node_gaia                       start max_node of gaia
    max_node_maxgraph                   start max_node of maxgraph
    maxgraph                            start maxgraph with v2
    load_tools                          start load_tools of maxgraph
END
}

# a function to setup common variable and env
_setup_maxgraph_env() {
  declare script="$0"
  if [ -z "${MAXGRAPH_HOME}" ]; then
    # set maxgraph_home base location of the script
    while [ -h "${script}" ] ; do
      ls=$(ls -ld "${script}")
      # Drop everything prior to ->
      link=$(expr "${ls}" : '.*-> \(.*\)$')
      if expr "${link}" : '/.*' > /dev/null; then
        script="${link}"
      else
        script="$(dirname ${script})/${link}"
      fi
      done
      MAXGRAPH_HOME=$(dirname "${script}")
      MAXGRAPH_HOME=$(cd "${MAXGRAPH_HOME}"; pwd)
      readonly MAXGRAPH_HOME=$(dirname ${MAXGRAPH_HOME})
  fi

  if [ -z "${MAXGRAPH_CONF_DIR}" ]; then
    readonly MAXGRAPH_CONF_DIR="${MAXGRAPH_HOME}/conf"
  fi

  if [ -z "${MAXGRAPH_LOGBACK_FILE}" ]; then
    readonly MAXGRAPH_LOGBACK_FILE="${MAXGRAPH_CONF_DIR}/logback.xml"
  fi

  if [ -z "${MAXGRAPH_CONF_FILE}" ]; then
    readonly MAXGRAPH_CONF_FILE="${MAXGRAPH_CONF_DIR}/maxgraph.config"
  fi

  if [ -z "${LOG_NAME}" ]; then
    readonly LOG_NAME="maxgraph"
  fi

  export LD_LIBRARY_PATH=${MAXGRAPH_HOME}/native:${MAXGRAPH_HOME}/native/lib:${LD_LIBRARY_PATH}:/usr/local/lib

  if [ -z "${LOG_DIR}" ]; then
    GS_LOG="/var/log/graphscope"
    if [[ ! -d "${GS_LOG}" || ! -w "${GS_LOG}" ]]; then
      # /var/log/graphscope is not existed/writable, switch to ${HOME}/.local/log/graphscope
      GS_LOG=${HOME}/.local/log/graphscope
    fi
    readonly GS_LOG
    mkdir -p ${GS_LOG}
    export LOG_DIR=${GS_LOG}/store
  fi

  mkdir -p ${LOG_DIR}

  libpath="$(echo "${MAXGRAPH_HOME}"/lib/*.jar | tr ' ' ':')"
}

# start max_node of {gaia, v2}
max_node() {
  type=$1; shift
  _setup_maxgraph_env

  java -server \
       -Dlogback.configurationFile="${MAXGRAPH_LOGBACK_FILE}" \
       -Dconfig.file="${MAXGRAPH_CONF_FILE}" \
       -Dlog.dir="${LOG_DIR}" \
       -Dlog.name="${LOG_NAME}" \
       -cp "${libpath}" com.alibaba.maxgraph.servers.MaxNode \
       "$@" > >(tee -a "${LOG_DIR}/${LOG_NAME}.out") 2> >(tee -a "${LOG_DIR}/${LOG_NAME}.err" >&2)
}

load_tools() {
  _setup_maxgraph_env
  java -cp "${MAXGRAPH_HOME}/lib/data_load_tools-0.0.1-SNAPSHOT.jar" \
       com.alibaba.maxgraph.dataload.LoadTool "$@"
}

# start maxgraph with v2
maxgraph() {
  _setup_maxgraph_env

  java_opt="-server
            -Djava.awt.headless=true
            -Dfile.encoding=UTF-8
            -Dsun.jnu.encoding=UTF-8
            -XX:+UseG1GC
            -XX:ConcGCThreads=2
            -XX:ParallelGCThreads=5
            -XX:MaxGCPauseMillis=50
            -XX:InitiatingHeapOccupancyPercent=20
            -XX:-OmitStackTraceInFastThrow
            -XX:+HeapDumpOnOutOfMemoryError
            -XX:HeapDumpPath=${LOG_DIR}/${LOG_NAME}.hprof
            -XX:+PrintGCDetails
            -XX:+PrintGCDateStamps
            -XX:+PrintTenuringDistribution
            -XX:+PrintGCApplicationStoppedTime
            -Xloggc:${LOG_DIR}/${LOG_NAME}.gc.log
            -XX:+UseGCLogFileRotation
            -XX:NumberOfGCLogFiles=32
            -XX:GCLogFileSize=64m"

  java ${java_opt} \
      -Dlogback.configurationFile="${MAXGRAPH_LOGBACK_FILE}" \
      -Dconfig.file="${MAXGRAPH_CONF_FILE}" \
      -Dlog.dir="${LOG_DIR}" \
      -Dlog.name="${LOG_NAME}" \
      -cp "${libpath}" com.alibaba.maxgraph.servers.MaxGraph \
      "$@" > >(tee -a "${LOG_DIR}/${LOG_NAME}.out") 2> >(tee -a "${LOG_DIR}/${LOG_NAME}.err" >&2)
}

# parse argv
while test $# -ne 0; do
  arg=$1; shift
  case $arg in
    -h|--help) usage; exit ;;
    max_node_gaia) max_node "gaia" "$@"; exit;;
    max_node_maxgraph) max_node "maxgraph" "$@"; exit;;
    maxgraph) maxgraph "$@"; exit;;
    load_tools) load_tools "$@"; exit;;
    *)
      echo "unrecognized option or command '${arg}'"
      usage; exit;;
  esac
done

set +e
set +o pipefail
set +x
