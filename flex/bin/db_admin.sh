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

# DB HOME, find the HOME using relative path.
# this script should be located at $DB_HOME/bin/db_admin.sh
DB_HOME="$(
  cd "$(dirname "$0")/../" >/dev/null 2>&1
  pwd -P
)"
info "DB_HOME = ${DB_HOME}"
#find hqps.compose.yaml
if [ -f "${DB_HOME}/conf/hqps-compose.yaml" ]; then
  info "find hqps-compose.yaml"
else
  err "can not find hqps-compose.yaml"
  exit 1
fi
HQPS_COMPOSE_YAML="${DB_HOME}/conf/hqps-compose.yaml"

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

function do_server() {
  info "[Server] $1"
  case $1 in
  start)
    info "start the server"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} up -d"
    info "Running cmd: ${cmd}"
    ;;
  stop)
    info "stop the server"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} down"
    info "Running cmd: ${cmd}"
    ;;
  restart)
    info "restart the server"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} down"
    info "Running cmd: ${cmd}"
    cmd="docker-compose -f ${HQPS_COMPOSE_YAML} up -d"
    info "Running cmd: ${cmd}"
    ;;
  *)
    err "unknown server command $1"
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
  compiler)
    compiler_cmd="$2"
    do_compiler "$compiler_cmd"
    shift # past argument
    shift
    ;;
  *) # unknown option
    err "unknown option $1"
    usage
    exit 1
    ;;
  esac
done




