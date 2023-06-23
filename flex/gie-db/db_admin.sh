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
# the DB_HOME will be mount into docker container as /opt/gie-db/
DOCKER_DB_HOME="/opt/gie-db/"
#find hqps.compose.yaml
if [ -f "${DB_HOME}/conf/hqps-compose.yaml" ]; then
  info "find hqps-compose.yaml"
else
  err "can not find hqps-compose.yaml "${DB_HOME}"/conf/hqps-compose.yaml"
  exit 1
fi
HQPS_COMPOSE_YAML="${DB_HOME}/conf/hqps-compose.yaml"
INTERACTIVE_YAML="${DB_HOME}/gs_interactive.yaml"

SERVER_PORT=`yq '.dbms.server.port' ${INTERACTIVE_YAML}`
echo "SERVER_PORT = ${SERVER_PORT}"

# DB data path, where we stores the graph-data
DB_DATA_PATH="${DB_HOME}/data"
DOCKER_DATA_PATH="${DOCKER_DB_HOME}/data"

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
  cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec engine /GraphScope/flex/build/bin/graph_db_loader ${config}  \
    ${DST_GRAPH_PATH} 1"
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

function do_service(){
  # expect [start/stop] [graph]
  # check num args
  if [ $# -ne 2 ]; then
    err "service need 2 arguments, but got $#"
    usage
    exit 1
  fi

  # start sync_server and compiler make run
  service_cmd=$1
  graph_name=$2
  info "service_cmd = ${service_cmd}"
  info "graph_name = ${graph_name}"
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
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} exec engine /GraphScope/flex/build/bin/sync_server -p ${SERVER_PORT}  \
       -s 1 --grape-data-path ${docker_graph_dir_path}"
    info "Running cmd: ${cmd}"
    eval ${cmd}
    ;;
  stop)
    info "stop the service"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} down"
    info "Running cmd: ${cmd}"
    eval ${cmd}
    ;;
  restart)
    info "restart the service"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} down"
    info "Running cmd: ${cmd}"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} up -d"
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
  *) # unknown option
    err "unknown option $1"
    usage
    exit 1
    ;;
  esac
done




