#!/usr/bin/env bash
#
# interactive_engine command tool

set -x
set -e
set -o pipefail
readonly WORKSPACE=${GRAPHSCOPE_HOME}

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

# parallel run for k8s
parallel_run() {
  run_cmd=$1
  pod_hosts=$2
  engine=$3
  _id=1
  for pod in `echo $pod_hosts`
  do
    kubectl --namespace=$ENGINE_NAMESPACE exec $pod -c $engine -- sh -c "export server_id=$_id && $run_cmd"
    let _id+=1
  done
}

random_generator() {
  min=$1
  range=$2
  _port=$(( ((RANDOM<<15)|RANDOM) % $range + $min ))
  echo ${_port}
}

start_coordinator() {
  JAVA_OPT="-server
            -Xmx1024m
            -Xms1024m
            -XX:+HeapDumpOnOutOfMemoryError
            -XX:HeapDumpPath=./java.hprof
            -verbose:gc
            -Xloggc:${LOG_DIR}/maxgraph-coordinator.gc.log
            -XX:+PrintGCDetails
            -XX:+PrintGCDateStamps
            -XX:+PrintHeapAtGC
            -XX:+PrintTenuringDistribution
            -Djava.awt.headless=true
            -Dsun.net.client.defaultConnectTimeout=10000
            -Dsun.net.client.defaultReadTimeout=30000
            -XX:+DisableExplicitGC
            -XX:-OmitStackTraceInFastThrow
            -XX:+UseG1GC
            -XX:InitiatingHeapOccupancyPercent=75
            -Dfile.encoding=UTF-8
            -Dsun.jnu.encoding=UTF-8
            -Dlogfilename=${LOG_DIR}/maxgraph-coordinator.log
            -Dlogbasedir=${LOG_DIR}/coordinator
            -Dlog4j.configurationFile=file:${WORKSPACE}/config/log4j2.xml
            -classpath ${WORKSPACE}/0.0.1-SNAPSHOT/conf/*:${WORKSPACE}/0.0.1-SNAPSHOT/lib/*:"
  inner_config=$CONFIG_DIR/coordinator.application.properties
  cp $WORKSPACE/config/coordinator.application.properties $inner_config
  sed -i "s/ZOOKEEPER_PORT/$2/g" $inner_config
  pushd $WORKSPACE/coordinator/target/classes/
  java ${JAVA_OPT} com.alibaba.maxgraph.coordinator.CoordinatorMain $inner_config $1 1>$LOG_DIR/maxgraph-coordinator.out 2>$LOG_DIR/maxgraph-coordinator.err &
  echo $! > $PID_DIR/coordinator.pid
  popd
}

start_frontend() {
  JAVA_OPT="-server
            -verbose:gc
            -Xloggc:${LOG_DIR}/maxgraph-frontend.gc.log
            -XX:+PrintGCDetails
            -XX:+PrintGCDateStamps
            -XX:+PrintHeapAtGC
            -XX:+PrintTenuringDistribution
            -Djava.awt.headless=true
            -Dsun.net.client.defaultConnectTimeout=10000
            -Dsun.net.client.defaultReadTimeout=30000
            -XX:+DisableExplicitGC
            -XX:-OmitStackTraceInFastThrow
            -XX:+UseG1GC
            -XX:InitiatingHeapOccupancyPercent=75
            -Dfile.encoding=UTF-8
            -Dsun.jnu.encoding=UTF-8
            -Dlogfilename=${LOG_DIR}/maxgraph-frontend.log
            -Dlogbasedir=${LOG_DIR}/frontend
            -Dlog4j.configurationFile=file:$WORKSPACE/config/log4j2.xml
            -classpath $WORKSPACE/0.0.1-SNAPSHOT/conf/*:$WORKSPACE/0.0.1-SNAPSHOT/lib/*:"
  REPLACE_SCHEMA_PATH=`echo ${2//\//\\\/}`

  inner_config=$CONFIG_DIR/frontend.vineyard.properties
  cp $WORKSPACE/config/frontend.vineyard.properties $inner_config
  sed -i "s/VINEYARD_SCHEMA_PATH/$REPLACE_SCHEMA_PATH/g" $inner_config
  sed -i "s/ZOOKEEPER_PORT/$3/g" $inner_config

  pushd $WORKSPACE/frontend/frontendservice/target/classes/

  java ${JAVA_OPT} com.alibaba.maxgraph.frontendservice.FrontendServiceMain $inner_config $1 1>$LOG_DIR/maxgraph-frontend.out 2>$LOG_DIR/maxgraph-frontend.err &

  timeout_seconds=60
  wait_period_seconds=0

  sleep 10
  while true
  do
    gremlin_server_port=$(awk '/frontend host/ { print }' ${LOG_DIR}/maxgraph-frontend.log | awk -F: '{print $6}')
    if [ -n "$gremlin_server_port" ]; then
      log "FRONTEND_PORT:127.0.0.1:$gremlin_server_port"
      break
    fi
    wait_period_seconds=$(($wait_period_seconds+5))
    if [ ${wait_period_seconds} -gt ${timeout_seconds} ];then
      err "Get external ip of ${GREMLIN_EXPOSE} failed."
      break
    fi
    sleep 5
  done

  echo $! > $PID_DIR/frontend.pid
  popd
}

start_executor() {
  export VINEYARD_IPC_SOCKET=$3
  inner_config=$CONFIG_DIR/executor.vineyard.properties
  cp $WORKSPACE/config/executor.vineyard.properties $inner_config
  sed -i "s/VINEYARD_OBJECT_ID/$1/g" $inner_config
  sed -i "s/ZOOKEEPER_PORT/$4/g" $inner_config
  server_id=1

  flag="maxgraph$1executor"
  RUST_BACKTRACE=full $WORKSPACE/bin/executor --config $inner_config ${flag} ${server_id} 1>> $LOG_DIR/maxgraph-executor.out 2>> $LOG_DIR/maxgraph-executor.err &

  echo $! > $PID_DIR/executor.pid
}

create_instance_on_local() {
  cluster_type=$1
  object_id=$2
  schema_path=$3
  server_id=$3
  vineyard_ipc_socket=$4
  zookeeper_port=$5

  set_common_envs ${object_id}
  mkdir -p $LOG_DIR $CONFIG_DIR $PID_DIR

  start_coordinator ${object_id} ${zookeeper_port}
  sleep 1

  start_frontend ${object_id} ${schema_path} ${zookeeper_port}
  sleep 1

  start_executor ${object_id} ${server_id} ${vineyard_ipc_socket} \
                 ${zookeeper_port}
}

create_instance_on_k8s() {
  object_id=$1
  schema_path=$2
  engine_count=`echo $3 | awk -F"," '{print NF}'`
  pod_hosts=`echo $3 | awk -F"," '{for(i=1;i<=NF;++i) {print $i" "}}'`
  engine_container=$4
  preemptive=$5
  gremlin_server_cpu=$6
  gremlin_server_mem=$7
  engine_paras=$8
  launch_engine_cmd="export object_id=${object_id} && /home/maxgraph/executor-entrypoint.sh"

  requests_cpu=0.5
  requests_mem="512Mi"

  # render schema path
  config=/home/maxgraph/config_${object_id}
  mkdir -p $config
  file=${schema_path##*/}
  cp $schema_path $config/$file
  schema_path=/home/maxgraph/config/$file

  # setup config
  update_cmd="/root/maxgraph/set_config.sh $object_id $schema_path $(hostname -i) $engine_count $engine_paras"
  sh -c "$update_cmd"
  parallel_run "$update_cmd" "$pod_hosts" $engine_container

  # create pods
  log "Launch coordinator & frontend in one pod."
  gremlin_image=$GREMLIN_IMAGE
  coordinator_image=$COORDINATOR_IMAGE
  config_name=config-${object_id}
  pod_name=pod-${object_id}
  gremlin_image=$(printf '%s\n' "$gremlin_image" | sed -e 's/[\/&]/\\&/g')
  coordinator_image=$(printf '%s\n' "$coordinator_image" | sed -e 's/[\/&]/\\&/g')
  # node_host=`kubectl describe pods -l "app=manager" | grep "Node:" | head -1 | awk -F '[ /]+' '{print $2}'`
  kubectl create configmap $config_name --from-file /home/maxgraph/config_$object_id

  if [ "$preemptive" = "True" ]; then
    sed -e "s/unique_pod_name/$pod_name/g" -e "s/unique_config_name/$config_name/g" \
        -e "s/gremlin_image/$gremlin_image/g" -e "s/unique_object_id/$object_id/g" \
        -e "s/requests_cpu/$requests_cpu/g" -e "s/requests_mem/$requests_mem/g" \
        -e "s/limits_cpu/$gremlin_server_cpu/g" -e "s/limits_mem/$gremlin_server_mem/g" \
        -e "s/coordinator_image/$coordinator_image/g" \
        /root/maxgraph/pod.yaml > /root/maxgraph/pod_${object_id}.yaml
  else
    sed -e "s/unique_pod_name/$pod_name/g" -e "s/unique_config_name/$config_name/g" \
        -e "s/gremlin_image/$gremlin_image/g" -e "s/unique_object_id/$object_id/g" \
        -e "s/requests_cpu/$gremlin_server_cpu/g" -e "s/requests_mem/$gremlin_server_mem/g" \
        -e "s/limits_cpu/$gremlin_server_cpu/g" -e "s/limits_mem/$gremlin_server_mem/g" \
        -e "s/coordinator_image/$coordinator_image/g" \
        /root/maxgraph/pod.yaml > /root/maxgraph/pod_${object_id}.yaml
  fi
  kubectl apply -f /root/maxgraph/pod_${object_id}.yaml

  log "launch interactive engine per analytical pod."
  parallel_run "$launch_engine_cmd" "$pod_hosts" $engine_container 1>/dev/null 2>&1

  log "expose gremlin server"
  gremlin_pod=`kubectl get pods -l "graph=pod-${object_id}" | grep -v NAME | awk '{print $1}'`
  if [ "$GREMLIN_EXPOSE" = "LoadBalancer" ]; then
    port=8182
    # range [50001, 53000)
    external_port=$( random_generator 50001 3000 )
    kubectl expose pod ${gremlin_pod} --name=gremlin-${object_id} --port=${external_port} \
      --target-port=${port} --type=LoadBalancer 1>/dev/null 2>&1
    [ $? -eq 0 ] || exit 1
    wait_period_seconds=0
    while true
    do
      external_ip=`kubectl describe service gremlin-${object_id} | grep "LoadBalancer Ingress" | awk -F'[ :]+' '{print $3}'`
      if [ -n "${external_ip}" ]; then
        break
        fi
        wait_period_seconds=$(($wait_period_seconds+5))
        if [ ${wait_period_seconds} -gt ${timeout_seconds} ];then
          echo "Get external ip of ${GREMLIN_EXPOSE} failed."
          break
        fi
        sleep 5
    done
  else
    kubectl expose pod ${gremlin_pod} --name=gremlin-${object_id} --port=${port} \
      --target-port=${port} --type=NodePort 1>/dev/null 2>&1
    [ $? -eq 0 ] || exit 1
    external_port=`kubectl describe services gremlin-${object_id} | grep "NodePort" | grep "TCP" | tr -cd "[0-9]"`
    [ $? -eq 0 ] || exit 1
    EXTERNAL_IP=`kubectl describe pods pod-${object_id} | grep "Node:" | head -1 | awk -F '[ /]+' '{print $3}'`
  fi
  log "FEONTEND_PORT: $external_ip:$external_port"
}

close_instance_on_local() {
  object_id=$1

  set_common_envs ${object_id}

  coordinator_id=`cat $PID_DIR/coordinator.pid`
  frontend_id=`cat $PID_DIR/frontend.pid`
  executor_id=`cat $PID_DIR/executor.pid`

  kill $coordinator_id || true
  kill $frontend_id || true
  kill $executor_id || true
}

close_instance_on_k8s() {
  object_id=$1
  pod_hosts=`echo $2 | awk -F"," '{for(i=1;i<=NF;++i) {print $i" "}}'`
  engine_container=$3
  waiting_for_delete=$4

  log "Delete pods"
  kubectl delete -f /root/maxgraph/pod_${object_id}.yaml --wait=${waiting_for_delete}
  kubectl delete configmap config-${object_id}
  kubectl delete service gremlin-${object_id}

  log "Close maxgraph instance of $object_id"
  kill_cmd="/root/maxgraph/kill_process.sh $object_id"
  parallel_run "$kill_cmd" "$pod_hosts" $engine_container
}

start_service() {
  cluster_type=$1
  port=$2
  instance_id=$3
  zookeeper_port=$4

  if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
    export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
  fi

  LIBPATH="."
  for file in `ls ${WORKSPACE}/lib`; do
    LIBPATH=$LIBPATH":"$WORKSPACE/lib/$file
  done

  INSTANCE_DIR=$GRAPHSCOPE_RUNTIME/$instance_id
  mkdir -p $INSTANCE_DIR
  inner_config=$INSTANCE_DIR/application.properties
  cp $WORKSPACE/config/application.properties $inner_config
  sed -i "s#SERVER_PORT#$port#g" $inner_config
  sed -i "s#CREATE_SCRIPT#$WORKSPACE/bin/giectl.sh#g" $inner_config
  sed -i "s#CLOSE_SCRIPT#$WORKSPACE/bin/giectl.sh#g" $inner_config
  sed -i "s#ZOOKEEPER_PORT#$zookeeper_port#g" $inner_config
  if [ "$cluster_type" == "local" ]; then
    java -cp $LIBPATH -Dspring.config.location=$inner_config com.alibaba.maxgraph.admin.InstanceManagerApplication &
    echo $! > $INSTANCE_DIR/graphmanager.pid
  else
    java -cp $LIBPATH -Dspring.config.location=$inner_config com.alibaba.maxgraph.admin.InstanceManagerApplication
  fi
}

stop_service() {
  cluster_type=$1
  instance_id=$2

  if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
    export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
  fi

  if [ "$cluster_type" == "local" ]; then
    instance_dir=$GRAPHSCOPE_RUNTIME/$instance_id
    graphmanager_id=`cat $instance_dir/graphmanager.pid`
    kill $graphmanager_id || true > /dev/null 2>&1
  else
    jps | grep InstanceManagerApplication | awk '{print $1}' | xargs kill -9
  fi
}

# parse argv
# TODO(acezen): when option and command is not illegal, warning and output usage.
# TODO(acezen): now the option need to specify before command, that's not user-friendly.
while test $# -ne 0; do
  arg=$1; shift
  case $arg in
    -h|--help) usage; exit ;;
    create_instance) create_instance "$@"; exit;;
    close_instance) close_instance "$@"; exit;;
    start_service) start_service "$@"; exit;;
    stop_service) stop_service "$@"; exit;;
    *)
      ;;
  esac
done

set +x
set +e
set +o pipefail