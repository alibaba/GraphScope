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

	start_http       Start groot http server
END
}

# a function to setup common variable and env
_setup_env() {
	SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
	if [ -z "${GROOT_HOME}" ]; then
		GROOT_HOME=$(dirname "$SCRIPT_DIR")
	fi

	export GROOT_LOGBACK_FILE=${GROOT_LOGBACK_FILE:-${GROOT_HOME}/conf/logback.xml}
	export GROOT_CONF_FILE=${GROOT_CONF_FILE:-${GROOT_HOME}/conf/groot.config}
	export LOG_NAME=${LOG_NAME:-graphscope-store}
	export LOG_DIR=${LOG_DIR:-/var/log/graphscope}
	export LOG_MAX_FILE_SIZE=${LOG_MAX_FILE_SIZE:-100MB}
	export LOG_MAX_HISTORY=${LOG_MAX_HISTORY:-10}
	export LOG_TOTAL_SIZE_CAP=${LOG_TOTAL_SIZE_CAP:-1GB}

	mkdir -p ${LOG_DIR}

	export OTEL_SDK_DISABLED="${OTEL_SDK_DISABLED:-true}"

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
	java -cp ${libpath} com.alibaba.graphscope.groot.servers.GrootGraphDaemon &
	java ${java_opt} \
		-Dlogback.configurationFile="${GROOT_LOGBACK_FILE}" \
		-Dconfig.file="${GROOT_CONF_FILE}" \
		-Dlog.dir="${LOG_DIR}" \
		-Dlog.name="${LOG_NAME}" \
		-cp "${libpath}" com.alibaba.graphscope.groot.servers.GrootGraph \
		"$@" # > >(tee -a "${LOG_DIR}/${LOG_NAME}.out") 2> >(tee -a "${LOG_DIR}/${LOG_NAME}.err" >&2)
}

# start groot http server
start_http_server() {
	_setup_env
	# spring boot config file only support .properties or .yml files
	echo "GROOT_CONF_FILE: ${GROOT_CONF_FILE}"
	echo "GROOT_LOGBACK_FILE: ${GROOT_LOGBACK_FILE}"
	GROOT_HTTP_CONF_FILE="${GROOT_CONF_FILE}.properties"
	HTTP_LOG_NAME="${LOG_NAME}-http"
	if [ -f "${GROOT_CONF_FILE}" ] && [ ! -f "${GROOT_HTTP_CONF_FILE}" ]; then
  		cp "${GROOT_CONF_FILE}" "${GROOT_HTTP_CONF_FILE}"
	fi
	java -Dlogging.config="${GROOT_LOGBACK_FILE}" \
     -Dspring.config.location="${GROOT_HTTP_CONF_FILE}" \
     -jar "${GROOT_HOME}/lib/groot-http-0.0.1-SNAPSHOT.jar" \
	 "$@" # > >(tee -a "${LOG_DIR}/${HTTP_LOG_NAME}.out") 2> >(tee -a "${LOG_DIR}/${HTTP_LOG_NAME}.err" >&2)
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
	start_http)
		start_http_server "$@"
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
