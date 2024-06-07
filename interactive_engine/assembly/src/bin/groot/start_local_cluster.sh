#!/usr/bin/env bash

SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)

if [ -z "${GROOT_HOME}" ]; then
    GROOT_HOME=$(dirname "$SCRIPT_DIR")
fi

declare -r GROOT_DIR=${GROOT_HOME:-/usr/local/groot}
declare -r CONFIG_FILE="/tmp/groot.config"

sed "s@LOG4RS_CONFIG@${GROOT_DIR}/conf/log4rs.yml@" ${GROOT_DIR}/conf/config.template > ${CONFIG_FILE}

GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_DIR}/bin/store_ctl.sh start &

# coordinator
python3 -m gs_flex_coordinator || true

# Start container with port mapping
# docker run -p 12312:12312 -p 55556:55556 -p 8080:8080 graphscope/graphscope-store:latest /usr/local/groot/bin/start_local_cluster.sh

# pip3 install graphscope_client --user
# export NODE_IP="127.0.0.1"
# export GREMLIN_PORT="12312"
# export GRPC_PORT="55556"
# export COORDINATOR_PORT="8080"
