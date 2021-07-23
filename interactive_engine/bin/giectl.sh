#!/usr/bin/env bash
#
# interactive_engine command tool

readonly SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
readonly WORKSPACE=${SCRIPT_DIR}/../

err() {
  echo -e "${RED}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [ERROR]${NC} $*" >&2
}

warning() {
  echo -e "${YELLOW}[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [WARNING]${NC} $*" >&1
}

log() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&1
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
  A script to install dependencies of GraphScope or deploy GraphScope locally.

  Usage: deploy_local [options] [command]

  Options:

    --prefix <path>      install prefix of GraphScope, default is /usr/local
    --verbose            ouput the debug logging information
    -h, --help           output help information

  Commands:

    install_deps         install dependencies of GraphScope
    deploy               deploy GraphScope locally
END
}



set_common_envs() {
  object_id=$1
  if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
    export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
  fi

  if [ -n "$object_id" ]; then
    export LOG_DIR=$GRAPHSCOPE_RUNTIME/logs/$object_id
    export CONFIG_DIR=$GRAPHSCOPE_RUNTIME/config/$object_id
    export PID_DIR=$GRAPHSCOPE_RUNTIME/pid/$object_id
  fi
}

start_coordinator() {
  readonly JAVA_OPT="-server -Xmx1024m -Xms1024m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./java.hprof -verbose:gc -Xloggc:${LOG_DIR}/maxgraph-coordinator.gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -Djava.awt.headless=true -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000 -XX:+DisableExplicitGC -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=75 -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Dlogfilename=${LOG_DIR}/maxgraph-coordinator.log -Dlogbasedir=${LOG_DIR}/coordinator -Dlog4j.configurationFile=file:${WORKSPACE}/0.0.1-SNAPSHOT/conf/log4j2.xml -classpath ${WORKSPACE}/0.0.1-SNAPSHOT/conf/*:${WORKSPACE}/0.0.1-SNAPSHOT/lib/*:"
  inner_config=$CONFIG_DIR/coordinator.application.properties
  cp $WORKSPACE/config/coordinator.application.properties $inner_config
  sed -i "s/ZOOKEEPER_PORT/$2/g" $inner_config
  pushd $WORKSPACE/coordinator/target/classes/
  java ${JAVA_OPT} com.alibaba.maxgraph.coordinator.CoordinatorMain $inner_config $1 1>$LOG_DIR/maxgraph-coordinator.out 2>$LOG_DIR/maxgraph-coordinator.err &
  echo $! > $PID_DIR/coordinator.pid
  popd
}

start_frontend() {
  readonly JAVA_OPT="-server -verbose:gc -Xloggc:${LOG_DIR}/maxgraph-frontend.gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -Djava.awt.headless=true -Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=30000 -XX:+DisableExplicitGC -XX:-OmitStackTraceInFastThrow -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=75 -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Dlogfilename=${LOG_DIR}/maxgraph-frontend.log -Dlogbasedir=${LOG_DIR}/frontend -Dlog4j.configurationFile=file:$WORKSPACE/0.0.1-SNAPSHOT/conf/log4j2.xml -classpath $WORKSPACE/0.0.1-SNAPSHOT/conf/*:$WORKSPACE/0.0.1-SNAPSHOT/lib/*:"
  readonly REPLACE_SCHEMA_PATH=`echo ${2//\//\\\/}`

  inner_config=$CONFIG_DIR/frontend.vineyard.properties
  cp $WORKSPACE/config/frontend.vineyard.properties $inner_config
  sed -i "s/VINEYARD_SCHEMA_PATH/$REPLACE_SCHEMA_PATH/g" $inner_config
  sed -i "s/ZOOKEEPER_PORT/$3/g" $inner_config

  cd $WORKSPACE/frontend/frontendservice/target/classes/

  java ${JAVA_OPT} com.alibaba.maxgraph.frontendservice.FrontendServiceMain $inner_config $1 1>$LOG_DIR/maxgraph-frontend.out 2>$LOG_DIR/maxgraph-frontend.err &

  timeout_seconds=60
  wait_period_seconds=0

  while true
  do
    gremlin_server_port=`awk '/frontend host/ { print }' ${LOG_DIR}/maxgraph-frontend.log | awk -F: '{print $6}'`
    if [ -n "$gremlin_server_port" ]; then
      echo "FRONTEND_PORT:127.0.0.1:$gremlin_server_port"
      break
    fi
    wait_period_seconds=$(($wait_period_seconds+5))
    if [ ${wait_period_seconds} -gt ${timeout_seconds} ];then
      echo "Get external ip of ${GREMLIN_EXPOSE} failed."
      break
    fi
    sleep 5
  done

  echo $! > $PID_DIR/frontend.pid
}

start_executor() {
  export VINEYARD_IPC_SOCKET=$3
  inner_config=$CONFIG_DIR/executor.local.vineyard.properties
  cp $WORKSPACE/config/executor.local.vineyard.properties $inner_config
  sed -i "s/VINEYARD_OBJECT_ID/$1/g" $inner_config
  sed -i "s/ZOOKEEPER_PORT/$4/g" $inner_config
  server_id=1

  flag="maxgraph$1executor"
  RUST_BACKTRACE=full $WORKSPACE/bin/executor --config $inner_config ${flag} ${server_id} 1>> $LOG_DIR/maxgraph-executor.out 2>> $LOG_DIR/maxgraph-executor.err &

  echo $! > $PID_DIR/executor.pid
}

create_instance() {
  object_id=$1
  schema_path=$2
  server_id=$3
  vineyard_ipc_socket=$4
  zookeeper_port=$5

  set_common_envs ${object_id}
  mkdir -p $LOG_DIR $CONFIG_DIR $PID_DIR

  start_coordinator ${object_id} ${zookeeper_port}

  start_frontend ${object_id} ${schema_path} ${zookeeper_port}

  start_executor ${object_id} ${server_id} ${vineyard_ipc_socket} \
                 ${zookeeper_port}
}

close_instance() {
  object_id=$1

  set_common_envs ${object_id}

  coordinator_id=`cat $PID_DIR/coordinator.pid`
  frontend_id=`cat $PID_DIR/frontend.pid`
  executor_id=`cat $PID_DIR/executor.pid`

  sudo kill $coordinator_id || true
  sudo kill $frontend_id || true
  sudo kill $executor_id || true
}

# parse argv
# TODO(acezen): when option and command is not illegal, warning and output usage.
# TODO(acezen): now the option need to specify before command, that's not user-friendly.
while test $# -ne 0; do
  arg=$1; shift
  case $arg in
    -h|--help) usage; exit ;;
    start_instance) start_instance $1 $2 $3 $4 $5; exit;;
    close_instance) close_instance; exit;;
    *)
      ;;
  esac
done
