#!/bin/bash
#
# A script to perform tests for analytical engine.

set -eo pipefail

# colored error and info functions to wrap messages.
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
err() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] -ERROR- $* ${NC}" >&2
}

info() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

function parse_yaml {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}

# DB HOME, find the HOME using relative path.
# this script should be located at $DB_HOME/bin/db_admin.sh
DB_HOME="$(
  cd "$(dirname "$0")/" >/dev/null 2>&1
  pwd -P
)"
info "DB_HOME = ${DB_HOME}"
# the DB_HOME will be mount into docker container the same directory
DOCKER_DB_HOME="${DB_HOME}"
#find hqps.compose.yaml
if [ -f "${DB_HOME}/conf/hqps-compose.yaml" ]; then
  info "find hqps-compose.yaml"
else
  err "can not find hqps-compose.yaml "${DB_HOME}"/conf/hqps-compose.yaml"
  exit 1
fi
HQPS_COMPOSE_YAML="${DB_HOME}/conf/hqps-compose.yaml"
INTERACTIVE_YAML="${DOCKER_DB_HOME}/gs_interactive.yaml"


# DB data path, where we stores the graph-data
DB_DATA_PATH="${DB_HOME}/data"
DOCKER_DATA_PATH="${DOCKER_DB_HOME}/data"

DOCKER_LOG_PATH="${DOCKER_DB_HOME}/logs"
DB_LOG_PATH="${DB_HOME}/logs"
# create if not exists
mkdir -p "${DB_LOG_PATH}"

HQPS_ENGINE_CONTAINER_NAME="hqps-server"


# parse the args and set the variables.
# start-server
# stop-server
# start-compiler
# stop-compiler
# restart-server
# restart-compiler
function usage() {
  cat <<EOF
   Usage: $./db_admin.sh []
   optional arguments:
    -h, --help                 show this help message and exit
    server start               start the server
    server stop                stop the server
    server restart             restart the server
    compiler start             start the compiler
    compiler stop              stop the compiler
    compiler restart           restart the compiler
    import -n [--name] <graph_name> -c [--config] <path-to-graph-config-yaml> 
                               import a graph into database
    compile <sourcefile[.cc, .cypher] -o [--output_dir] <output-directory>
                                compile cypher/.cc to dynamic library
EOF
}
function import_graph_usage(){
  cat << EOF
    import <graph_name> -c [--config] <path-to-graph-config-yaml> 
                            import a graph into database
                          
EOF
}

function stored_procedure_usage(){
  cat << EOF
    db_admin.sh add_stored_procedure -i <sourcefile[.cc, .cypher] -o [--output_dir] <output-directory>
                                compile cypher/.cc to dynamic library
    db_admin.sh remove_stored_procedure -n <stored_procedure_name>
                                remove stored procedure
EOF
}

function check_enviroment(){
  # check whether docker exists in this enviroment
  if ! [ -x "$(command -v docker)" ]; then
    err "docker is not installed, please install docker first."
    exit 1
  fi
  #check docker compose
  if ! [ -x "$(command -v docker-compose)" ]; then
    err "docker-compose is not installed, please install docker-compose first."
    exit 1
  fi
}

function import_graph(){
  # check variable numbers equals to 4
  if [ $# -ne 4 ]; then
    err "import graph need 4 arguments, but got $#"

    exit 1
  fi

  while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
    -c | --config)
      config="$2"
      shift # past argument
      shift
      ;;
    -n | --name)
      graph_name="$2"
      shift
      shift
      ;;
    *)
      err "unknown option $1"
      import_graph_usage
      exit 1
      ;;
    esac
  done

  echo "graph_name = ${graph_name}"
  echo "config = ${config}"
  # check whether config file exists
  # check whether graph_name is empty
  if [ -z "${graph_name}" ]; then
    err "graph_name is empty"
    exit 1
  fi
  # check whether DB_DATA_PATH/graph_name exists
  if [ -d "${DB_DATA_PATH}/${graph_name}" ]; then
    err "graph ${graph_name} already exists"
    exit 1
  fi

  # to find inside the docker container, we need to use the DOCKER_DATA_PATH
  DST_GRAPH_PATH="${DOCKER_DATA_PATH}/${graph_name}"

  # docker-compose exec command to load the graph
  cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec -it engine bash -c \"cd ${DB_HOME}; /GraphScope/flex/build/bin/graph_db_loader ${config} ${DST_GRAPH_PATH} 1\"; exit $?"
  info "Running cmd: ${cmd}"
  eval ${cmd}

  # check graph is loaded by check DB_DATA_PATH/graph_name/init_snapshot.bin" exists
  if [ ! -f "${DB_DATA_PATH}/${graph_name}/init_snapshot.bin" ]; then
    err "graph ${graph_name} is not loaded"
    exit 1
  fi
}

function do_server() {
  info "[Server] $1"
  case $1 in
  start)
    info "start the server"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} up -d"
    info "Running cmd: ${cmd}"
    eval ${cmd}
    ;;
  stop)
    info "stop the server"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} down"
    info "Running cmd: ${cmd}"
    eval ${cmd}
    ;;
  restart)
    info "restart the server"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} down"
    info "Running cmd: ${cmd}"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} up -d"
    info "Running cmd: ${cmd}"
    eval ${cmd}
    ;;
  *)
    err "unknown server command $1"
    usage
    exit 1
    ;;
  esac
}

function kill_server(){
  info "kill the server"
  cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec -d engine pkill sync_server"
  info "Running cmd: ${cmd}"
  eval ${cmd}
  info "kill the server"
}

function kill_compiler(){
  info "kill the compiler"
  cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec -d compiler pkill compiler-0.0.1-SNAPSHOT.jar"
  info "Running cmd: ${cmd}"
  eval ${cmd}
  info "kill the compiler"
}

function check_server_up(){
  info "check server is up"
  cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec engine ps aux | grep sync_server"
  info "Running cmd: ${cmd}"
  eval ${cmd}
}

function check_compiler_up(){
  info "check compiler is up"
  cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec compiler ps aux | grep compiler-0.0.1-SNAPSHOT.jar"
  info "Running cmd: ${cmd}"
  eval ${cmd}
}

function do_service(){
  # expect [start/stop] [graph]
  # check num args
  if [ $# -lt 1 ]; then
    err "service need 1 arguments, but got $#"
    usage
    exit 1
  fi
  service_cmd=$1

  # start sync_server and compiler make run

  graph_name=$2
  graph_schema_path=$3
  graph_stored_procedures=$4
  info "service_cmd = ${service_cmd}"
  info "graph_name = ${graph_name}"
  info "graph_schema_path = ${graph_schema_path}"
  info "graph_stored_procedures = ${graph_stored_procedures}"
  graph_dir_path="${DB_DATA_PATH}/${graph_name}"
  # check graph_dir_path exists
  if [ ! -d "${graph_dir_path}" ]; then
    err "graph ${graph_name} does not exist"
    exit 1
  fi

  docker_graph_dir_path="${DOCKER_DATA_PATH}/${graph_name}"

  info "[Service] $1"
  case $1 in
  start)
    info "starting the hqps service"
    # check varaibles
    if [ -z "${graph_name}" ]; then
      err "graph_name is empty"
      exit 1
    fi
    if [ -z "${graph_schema_path}" ]; then
      err "graph_schema_path is empty"
      exit 1
    fi
    if [ -z "${graph_stored_procedures}" ]; then
      info "graph_stored_procedures is empty"
    fi
    # kill the sync_server and compiler
    HPQS_COMPILER_LOG="${DOCKER_LOG_PATH}/compiler_${graph_name}.log"
    HPQS_SERVER_LOG="${DOCKER_LOG_PATH}/server_${graph_name}.log"
    kill_server
    kill_compiler
    info "start server"
    #if stored_procedure is specified
    if [ ! -z "${graph_stored_procedures}" ]; then
      cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec -it engine  bash -c \"/GraphScope/flex/build/bin/sync_server -c ${INTERACTIVE_YAML}  \
       --grape-data-path ${docker_graph_dir_path} -p ${graph_stored_procedures} > ${HPQS_SERVER_LOG} 2>&1 &\""
    else 
      cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec -d engine /GraphScope/flex/build/bin/sync_server -c ${INTERACTIVE_YAML}"
      cmd=${cmd}" --grape-data-path ${docker_graph_dir_path} > ${HPQS_SERVER_LOG} 2>&1 &"
    fi
    info "Running cmd: ${cmd}"
    eval `${cmd}`
    sleep 2
    check_server_up

    info "start compiler"

    # create a ir.compiler.properties for this graph.

    # pass the path to ir.compiler.properties for this graph.

    GIE_HOME="/GraphScope/interactive_engine"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec -d compiler "
    cmd=${cmd}"bash -c \""
    cmd=${cmd}"cd ${DB_HOME}; "
    cmd=${cmd}"nohup java -cp \".:${GIE_HOME}/compiler/target/libs/*:${GIE_HOME}/compiler/target/compiler-0.0.1-SNAPSHOT.jar\" "
    cmd=${cmd}" -Djna.library.path=${GIE_HOME}/executor/ir/target/release"
    cmd=${cmd}" -Dgraph.schema=${graph_schema_path}"
    cmd=${cmd}" -Dstored.procedures=${graph_stored_procedures}"
    cmd=${cmd}" -Dgraph.store=exp"
    cmd=${cmd}" com.alibaba.graphscope.GraphServer > ${HPQS_COMPILER_LOG}"
    cmd=${cmd}"\""
    info "Running cmd: ${cmd}"
    eval ${cmd}
    sleep 3
    check_compiler_up
    info "success start the hqps service"
    ;;
  stop)
    info "stop the service"
    kill_server
    kill_compiler
    ;;
  status)
    info "check the service status"
    check_server_up
    check_compiler_up
    ;;
  restart)
    info "restart the service"
    kill_server
    info "start server"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec -d engine /GraphScope/flex/build/bin/sync_server -c ${INTERACTIVE_YAML}  \
       --grape-data-path ${docker_graph_dir_path}"
    info "Running cmd: ${cmd}"
    eval `${cmd}`

    info "start compiler"
    kill_compiler
    cmd='docker-compose -f ${HQPS_COMPOSE_YAML} exec engine \ 
       java -cp ".:${GIE_HOME}/compiler/target/libs/*:${GIE_HOME}/compiler/target/compiler-0.0.1-SNAPSHOT.jar" \
       -Djna.library.path=${GIE_HOME}/executor/ir/target/release \
       -Dgraph.schema=${graph_schema_path} \
       -Dstored.procedures=${graph_stored_procedures} \
       -Dgraph.store=exp \
       com.alibaba.graphscope.GraphServer > ${HPQS_COMPILER_LOG} 2>&1 &'
    info "Running cmd: ${cmd}"
    eval ${cmd}
    ;;
  *)
    err "unknown service command $1"
    usage
    exit 1
    ;;
  esac
}

function add_stored_procedure(){
    # check args num == 4
  if [ $# -ne 4 ]; then
    err "stored_procedure command need 4 args"
    stored_procedure_usage
    exit 1
  fi

  while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
    -i | --input)
      file_path="$2"
      shift # past argument
      shift
      ;;
    -o | --output)
      output_dir="$2"
      shift # past argument
      shift
      ;;
    *)
      err "unknown option $1"
      stored_procedure_usage
      exit 1
      ;;
    esac
  done

  #get real_file_path
  file_path=$(realpath "${file_path}")
  output_dir=$(realpath "${output_dir}")
  echo "file_path: ${file_path}"
  echo "output_dir: ${output_dir}"

  # the path to output_dir should be able to accessed in container
  # check file exist
  if [ ! -f "${file_path}" ]; then
    err "file ${file_path} not exist"
    exit 1
  fi

  # check output_dir exist
  if [ ! -d "${output_dir}" ]; then
    info "output_dir ${output_dir} not exist"
    info "create output_dir ${output_dir}"
    mkdir -p "${output_dir}"
  fi

  # copy file to container
  file_name=$(basename "${file_path}")
  cmd="docker cp ${file_path} ${HQPS_ENGINE_CONTAINER_NAME}:/tmp/${file_name}"
  eval ${cmd} || exit 1
  docker_file_name="/tmp/${file_name}"

  cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec engine bash -c \"/GraphScope/flex/bin/load_plan_and_run.sh -i=${docker_file_name} -w=/tmp/codegen/ -o=${output_dir} \""
  echo "Running cmd: ${cmd}"
  eval ${cmd}
  # check output exists
  # get the file_name of file_path
  file_name=$(basename "${file_path}")
  file_name="${file_name%.*}"
  # set output_file to dynamic lib with different suffix on different os
  if [[ "$(uname)" == "linux-gnu"* ]]; then
    output_file="${output_dir}/lib${file_name}.so"
  elif [[ "$(uname)" == "darwin"* ]]; then
    output_file="${output_dir}/lib${file_name}.dylib"
  else
    err "unknown os type"
    exit 1
  fi

  if [ ! -f "${output_file}" ]; then
    err "output file ${output_file} not exist"
    exit 1
  fi
  info "success generate dynamic lib ${output_file}"
}

# accept the .cypher file or a .cc file to generate dynamic lib from it
# how to remove?
function do_stored_procedure(){
  key="$1"
  case $key in
  add)
    shift # past argument
    add_stored_procedure "$@"
    exit 0
    ;;
  *)
    err "unknown stored_procedure command $1"
    stored_procedure_usage
    exit 1
    ;;
  esac
}

#check_enviroment

if [ $# -eq 0 ]; then
  usage
  exit 1
fi

while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  -h | --help)
    usage
    exit
    ;;
  server)
    server_cmd="$2"
    do_server "$server_cmd"
    shift # past argument
    shift
    ;;
  service)
    info "service: "
    shift # past argument
    do_service "$@"
    exit 0
    ;;
  import)
    #shirf and pass all command to import_graph func
    info "import graph..."
    shift
    import_graph "$@"
    exit 0
    ;;
  stored_procedure)
    info "stored_procedure: "
    shift # past argument
    do_stored_procedure "$@"
    exit 0
    ;;
  *) # unknown option
    err "unknown option $1"
    usage
    exit 1
    ;;
  esac
done




