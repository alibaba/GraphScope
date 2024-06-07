#!/usr/bin/env bash
#
# groot command tool

set -eo pipefail

usage() {
	cat <<END
  A script to launch groot service.

  Usage: store_ctl.sh [options] [parameters]

  Options:

    -h, --help       Output help information

  Commands:

    start            Start individual groot server. If no arguments given, start all servers as local deployment
END
}

# a function to setup common variable and env
_setup_env() {
	SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
	if [ -z "${GROOT_HOME}" ]; then
		GROOT_HOME=$(dirname "$SCRIPT_DIR")
	fi

	if [ -z "${GROOT_LOGBACK_FILE}" ]; then
		readonly GROOT_LOGBACK_FILE="${GROOT_HOME}/conf/logback.xml"
	fi

	if [ -z "${GROOT_CONF_FILE}" ]; then
		readonly GROOT_CONF_FILE="${GROOT_HOME}/conf/groot.config"
	fi

	if [ -z "${LOG_NAME}" ]; then
		readonly LOG_NAME="graphscope-store"
	fi

	if [ -z "${LOG_DIR}" ]; then
		export LOG_DIR="/var/log/graphscope"
	fi

	mkdir -p ${LOG_DIR}

	export LD_LIBRARY_PATH=${GROOT_HOME}/native:${GROOT_HOME}/native/lib:${LD_LIBRARY_PATH}:/usr/local/lib
	libpath="$(echo "${GROOT_HOME}"/lib/*.jar | tr ' ' ':')"
}

# start groot server
start_server() {
	_setup_env
	java_opt="-server
            -Djava.awt.headless=true
            -Dfile.encoding=UTF-8
            -Dsun.jnu.encoding=UTF-8
            -XX:+UseG1GC
            -XX:ConcGCThreads=2
            -XX:+IgnoreUnrecognizedVMOptions
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
            -XX:NumberOfGCLogFiles=4
            -XX:GCLogFileSize=64m"
	export RUST_BACKTRACE=full
	java ${java_opt} \
		-Dlogback.configurationFile="${GROOT_LOGBACK_FILE}" \
		-Dconfig.file="${GROOT_CONF_FILE}" \
		-Dlog.dir="${LOG_DIR}" \
		-Dlog.name="${LOG_NAME}" \
		-cp "${libpath}" com.alibaba.graphscope.groot.servers.GrootGraph \
		"$@" # > >(tee -a "${LOG_DIR}/${LOG_NAME}.out") 2> >(tee -a "${LOG_DIR}/${LOG_NAME}.err" >&2)
}

# parse argv
while test $# -ne 0; do
	arg=$1
	shift
	case $arg in
	-h | --help)
		usage
		exit
		;;
	start)
		start_server "$@"
		exit
		;;
	*)
		echo "unrecognized option or command '${arg}'"
		usage
		exit
		;;
	esac
done

set +xeo pipefail
