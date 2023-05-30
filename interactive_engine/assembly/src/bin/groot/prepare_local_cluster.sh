#!/usr/bin/env bash

declare -r GROOT_DIR=${GROOT_HOME:-/usr/local/groot}
declare -r CONFIG_FILE="/tmp/groot.config"

sed "s@LOG4RS_CONFIG@${GROOT_DIR}/conf/log4rs.yml@" ${GROOT_DIR}/conf/config.template >${CONFIG_FILE}

GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_HOME}/bin/store_ctl.sh start

# Start container with port mapping
# docker run -p 12312:12312 -p 55556:55556 graphscope/graphscope-store:latest

# pip3 install graphscope_client --user
# export NODE_IP="127.0.0.1"
# export GREMLIN_PORT="12312"
# export GRPC_PORT="55556"
