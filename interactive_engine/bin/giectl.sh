#!/usr/bin/env bash
#
# interactive_engine command tool

set -x
set -e
set -o pipefail
# color
readonly RED="\033[0;31m"
readonly YELLOW="\033[1;33m"
readonly GREEN="\033[0;32m"
readonly NC="\033[0m" # No Color

readonly WORKSPACE=${GRAPHSCOPE_HOME}

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

random_generator() {
  min=$1
  range=$2
  _port=$(( ((RANDOM<<15)|RANDOM) % $range + $min ))
  echo ${_port}
}

start_coordinator() {
  object_id=$1
  zookeeper_ip=$2
  zookeeper_port=$3
  executor_count=$4

  config_dir=${GRAPHSCOPE_RUNTIME}/config/${object_id}
  log_dir=${GRAPHSCOPE_RUNTIME}/logs/${object_id}
  pid_dir=${GRAPHSCOPE_RUNTIME}/pid/${object_id}
  mkdir -p $log_dir $config_dir $pid_dir
  JAVA_OPT="-server
            -Xmx1024m
            -Xms1024m
            -XX:+HeapDumpOnOutOfMemoryError
            -XX:HeapDumpPath=./java.hprof
            -verbose:gc
            -Xloggc:${log_dir}/maxgraph-coordinator.gc.log
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
            -Dlogfilename=${log_dir}/maxgraph-coordinator.log
            -Dlogbasedir=${log_dir}/coordinator
            -Dlog4j.configurationFile=file:${WORKSPACE}/config/log4j2.xml
            -classpath ${WORKSPACE}/0.0.1-SNAPSHOT/conf/*:${WORKSPACE}/0.0.1-SNAPSHOT/lib/*:"

  sed -e "s@GRAPH_NAME@${object_id}@g" \
      -e "s@ZOOKEEPER_IP:ZOOKEEPER_PORT@${zookeeper_ip}:$zookeeper_port@g" \
      -e "s@RESOURCE_EXECUTOR_COUNT@${executor_count}@g" \
      -e "s@PARTITION_NUM@${executor_count}@g" \
      $WORKSPACE/config/coordinator.application.properties > ${config_dir}/coordinator.application.properties

  # pushd $WORKSPACE/coordinator/target/classes/
  java ${JAVA_OPT} \
       com.alibaba.maxgraph.coordinator.CoordinatorMain \
       ${config_dir}/coordinator.application.properties \
       ${object_id} 1>${log_dir}/maxgraph-coordinator.out 2>${log_dir}/maxgraph-coordinator.err
  echo $! > $pid_dir/coordinator.pid
  # popd
}

start_frontend() {
  object_id=$1
  schema_path=$2
  zookeeper_ip=$3
  zookeeper_port=$4
  executor_count=$5

  config_dir=${GRAPHSCOPE_RUNTIME}/config/${object_id}
  log_dir=${GRAPHSCOPE_RUNTIME}/logs/${object_id}
  pid_dir=${GRAPHSCOPE_RUNTIME}/pid/${object_id}
  mkdir -p $log_dir $config_dir $pid_dir
  JAVA_OPT="-server
            -verbose:gc
            -Xloggc:${log_dir}/maxgraph-frontend.gc.log
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
            -Dlogfilename=${log_dir}/maxgraph-frontend.log
            -Dlogbasedir=${log_dir}/frontend
            -Dlog4j.configurationFile=file:$WORKSPACE/config/log4j2.xml
            -classpath $WORKSPACE/0.0.1-SNAPSHOT/conf/*:$WORKSPACE/0.0.1-SNAPSHOT/lib/*:"

  sed -e "s@GRAPH_NAME@${object_id}@g" \
      -e "s@ZOOKEEPER_IP:ZOOKEEPER_PORT@${zookeeper_ip}:$zookeeper_port@g" \
      -e "s@SCHEMA_PATH@${schema_path}@g" \
      -e "s@RESOURCE_EXECUTOR_COUNT@${executor_count}@g" \
      -e "s@PARTITION_NUM@${executor_count}@g" \
      $WORKSPACE/config/frontend.vineyard.properties > ${config_dir}/frontend.vineyard.properties

  # pushd $WORKSPACE/frontend/frontendservice/target/classes/
  java ${JAVA_OPT} \
       com.alibaba.maxgraph.frontendservice.FrontendServiceMain \
       ${config_dir}/frontend.vineyard.properties \
       ${object_id} 1>${log_dir}/maxgraph-frontend.out 2>${log_dir}/maxgraph-frontend.err

  # TODO: here just for local, need to adapt to k8s (expose gremlin server)
  timeout_seconds=60
  wait_period_seconds=0

  sleep 10
  while true
  do
    gremlin_server_port=$(awk '/frontend host/ { print }' ${log_dir}/maxgraph-frontend.log | awk -F: '{print $6}')
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

  echo $! > $pid_dir/frontend.pid
  # popd
}

start_executor() {
  object_id=$1
  server_id=$2
  zookeeper_ip=$3
  zookeeper_port=$4
  engine_count=$5
  interactive_engine_params=$6

  config_dir=${GRAPHSCOPE_RUNTIME}/config/${object_id}
  log_dir=${GRAPHSCOPE_RUNTIME}/logs/${object_id}
  pid_dir=${GRAPHSCOPE_RUNTIME}/pid/${object_id}
  mkdir -p $log_dir $config_dir $pid_dir

  sed -e "s@GRAPH_NAME@${object_id}@g" \
      -e "s@VINEYARD_OBJECT_ID@${object_id}@g" \
      -e "s@ZOOKEEPER_IP:ZOOKEEPER_PORT@${zookeeper_ip}:$zookeeper_port@g" \
      -e "s@WORKER_NUM@${engine_count}@g" \
      -e "s@PARTITION_NUM@${engine_count}@g" \
      $WORKSPACE/config/executor.vineyard.properties > ${config_dir}/executor.vineyard.properties

  # TODO: add comment to explain what code do for k8s -_-!.
  if [ ! -z "${interactive_engine_params}" ]; then
    params=`echo ${interactive_engine_params} |
      awk -F";" '{for(i=1; i<=NF; ++i){print $i}}'` # trans to key-value pairs
    for p in `echo $params`
    do
      key=`echo $p | awk -F":" '{print $1}'`
      value=`echo $p | awk -F":" '{print $2}'`
      key=$(printf '%s\n' "$key" | sed -e 's/[\/&]/\\&/g')
      value=$(printf '%s\n' "$value" | sed -e 's/[\/&]/\\&/g')
      sed -i "s/^\($key\s*=\s*\).*\$/\1$value/" ${config_dir}/executor.vineyard.properties
    done
  fi


  flag="maxgraph${object_id}executor"
  RUST_BACKTRACE=full $WORKSPACE/bin/executor --config ${config_dir}/executor.vineyard.properties \
    ${flag} ${server_id} 1>> $log_dir/maxgraph-executor.out 2>> $log_dir/maxgraph-executor.err &

  echo $! > $pid_dir/executor.pid
}

create_instance_on_local() {
  object_id=$1
  schema_path=$2
  server_id=$3
  export VINEYARD_IPC_SOCKET=$4
  zookeeper_port=$5

  if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
    export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
  fi

  start_coordinator ${object_id} "localhost" ${zookeeper_port} "1"
  sleep 1

  start_frontend ${object_id} ${schema_path} "localhost" ${zookeeper_port} "1"
  sleep 1

  start_executor ${object_id} ${server_id} "localhost" ${zookeeper_port} "1"
}

create_instance_on_k8s() {
  object_id=$1
  schema_path=$2
  pod_hosts=`echo $3 | awk -F"," '{for(i=1;i<=NF;++i) {print $i" "}}'`
  engine_count=`echo $3 | awk -F"," '{print NF}'`
  engine_container=$4
  preemptive=$5
  gremlin_server_cpu=$6
  gremlin_server_mem=$7
  engine_params=$8
  zookeeper_ip=$(hostname -i)
  requests_cpu=0.5
  requests_mem="512Mi"

  # render schema path
  schema_dir=${GRAPHSCOPE_HOME}/schema_${object_id}
  mkdir -p ${schema_dir}
  schema_file=${schema_path##*/}  # get file name from path
  cp $schema_path ${schema_dir}/$schema_file
  schema_path_in_container=${GRAPHSCOPE_HOME}/schema/$schema_file

  # create pods
  log "Launch coordinator and frontend in one pod."
  schema_name="schema-${object_id}"
  pod_name="pod-${object_id}"
  # TODO: just use the env
  gremlin_image=$(printf '%s\n' "${GREMLIN_IMAGE}")
  coordinator_image=$(printf '%s\n' "${COORDINATOR_IMAGE}")
  kubectl create configmap $schema_name --from-file ${WORKSPACE}/schema_$object_id

  if [ "$preemptive" = "True" ]; then
    sed -e "s@unique_pod_name@${pod_name}@g" \
        -e "s@unique_schema_name@${schema_name}@g" \
        -e "s@gremlin_image@${gremlin_image}@g" \
        -e "s@unique_object_id@${object_id}@g" \
        -e "s@zookeeper_ip@${zookeeper_ip}@g" \
        -e "s@requests_cpu@${requests_cpu}@g" \
        -e "s@requests_mem@${requests_mem}@g" \
        -e "s@limits_cpu@${gremlin_server_cpu}@g" \
        -e "s@limits_mem@${gremlin_server_mem}@g" \
        -e "s@coordinator_image@${coordinator_image}@g" \
        -e "s@unique_schema_path@${schema_path_in_container}@g" \
        -e "s@unique_executor_count@${engine_count}@g" \
        ${WORKSPACE}/config/pod.yaml > ${WORKSPACE}/config/pod_${object_id}.yaml
  else
    sed -e "s@unique_pod_name@$pod_name@g" \
        -e "s@unique_schema_name@$schema_name@g" \
        -e "s@gremlin_image@$gremlin_image@g" \
        -e "s@unique_object_id@$object_id@g" \
        -e "s@zookeeper_ip@${zookeeper_ip}@g" \
        -e "s@requests_cpu@$gremlin_server_cpu@g" \
        -e "s@requests_mem@$gremlin_server_mem@g" \
        -e "s@limits_cpu@$gremlin_server_cpu@g" \
        -e "s@limits_mem@$gremlin_server_mem@g" \
        -e "s@coordinator_image@$coordinator_image@g" \
        -e "s@unique_schema_path@${schema_path_in_container}@g" \
        -e "s@unique_executor_count@${engine_count}@g" \
        ${WORKSPACE}/config/pod.yaml > ${WORKSPACE}/config/pod_${object_id}.yaml
  fi
  kubectl apply -f ${WORKSPACE}/config/pod_${object_id}.yaml

  log "Launch interactive engine per analytical pod."
  _server_id=1
  for pod in `echo ${pod_hosts}`
  do
    launch_executor_cmd="${WORKSPACE}/bin/giectl.sh start_executor $object_id ${_server_id} $(hostname -i) 2181 ${engine_count} ${engine_params}"
    kubectl --namespace=$ENGINE_NAMESPACE exec $pod -c $engine_container -- /bin/bash -c "$launch_executor_cmd"
    let _server_id+=1
  done

  log "Expose gremlin server"
  gremlin_pod=`kubectl get pods -l "graph=pod-${object_id}" | grep -v NAME | awk '{print $1}'`
  # TODO: hard-code on k8s
  port="8182"
  if [ "$GREMLIN_EXPOSE" = "LoadBalancer" ]; then
    # random from range [50001, 53000)
    external_port=$(( ((RANDOM<<15)|RANDOM) % 50001 + 53000 ))
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
    external_ip=`kubectl describe pods pod-${object_id} | grep "Node:" | head -1 | awk -F '[ /]+' '{print $3}'`
  fi
  echo "FRONTEND_PORT:$external_ip:$external_port"
}

close_instance_on_local() {
  object_id=$1
  pid_dir=${GRAPHSCOPE_RUNTIME}/pid/${object_id}

  coordinator_id=`cat $pid_dir/coordinator.pid`
  frontend_id=`cat $pid_dir/frontend.pid`
  executor_id=`cat $pid_dir/executor.pid`

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
  for pod in `echo ${pod_hosts}`
  do
    kill_executor_process_cmd="ps -ef | grep maxgraph${object_id}executor | grep -v grep | awk '{print \$2}' | xargs kill -9"
    kubectl --namespace=$ENGINE_NAMESPACE exec $pod -c ${engine_container} -- sh -c "$kill_executor_process_cmd"
  done
}

start_service() {
  cluster_type=$1
  server_port=$2
  service_id=$3
  zookeeper_port=$4

  if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
    export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
  fi

  set +x
  LIBPATH="."
  for file in `ls ${WORKSPACE}/lib`; do
    LIBPATH=$LIBPATH":"$WORKSPACE/lib/$file
  done
  set -x

  service_dir=$GRAPHSCOPE_RUNTIME/$service_id
  mkdir -p $service_dir
  sed -e "s@SERVER_PORT@$server_port@g" \
      -e "s@CREATE_SCRIPT@$WORKSPACE/bin/giectl.sh@g" \
      -e "s@CLOSE_SCRIPT@$WORKSPACE/bin/giectl.sh@g" \
      -e "s@ZOOKEEPER_PORT@$zookeeper_port@g" \
      $WORKSPACE/config/application.properties > $service_dir/application.properties
  if [ "$cluster_type" == "local" ]; then
    java -cp $LIBPATH -Dspring.config.location=$service_dir/application.properties \
         com.alibaba.maxgraph.admin.InstanceManagerApplication &
    echo $! > $service_dir/graphmanager.pid
  else
    java -cp $LIBPATH -Dspring.config.location=$service_dir/application.properties \
         com.alibaba.maxgraph.admin.InstanceManagerApplication
  fi
}

stop_service() {
  cluster_type=$1
  service_id=$2

  if [ ! -n "$GRAPHSCOPE_RUNTIME" ]; then
    export GRAPHSCOPE_RUNTIME=/tmp/graphscope/runtime
  fi

  if [ "$cluster_type" == "local" ]; then
    service_dir=$GRAPHSCOPE_RUNTIME/$service_id
    manager_pid=`cat $service_dir/graphmanager.pid`
    kill $manager_pid || true > /dev/null 2>&1
  else
    jps | grep InstanceManagerApplication | awk '{print $1}' | xargs kill -9
  fi
}

# parse argv
while test $# -ne 0; do
  arg=$1; shift
  case $arg in
    -h|--help) usage; exit ;;
    start_service) start_service "$@"; exit;;
    stop_service) stop_service "$@"; exit;;
    create_instance_on_local) create_instance_on_local "$@"; exit;;
    create_instance_on_k8s) create_instance_on_k8s "$@"; exit;;
    close_instance_on_local) close_instance_on_local "$@"; exit;;
    close_instance_on_k8s) close_instance_on_k8s "$@"; exit;;
    start_coordinator) start_coordinator "$@"; exit;;
    start_frontend) start_frontend "$@"; exit;;
    start_executor) start_executor "$@"; exit;;
    *)
      usage; exit;;
  esac
done

set +x
set +e
set +o pipefail