#!/usr/bin/env bash

declare -r GROOT_DIR=${GROOT_HOME:-/usr/local/groot}
declare -r CONFIG_FILE="/tmp/groot.config"

# cp ~/.m2/repository/org/apache/curator/curator-test/5.4.0/curator-test-5.4.0.jar ${SOURCE_DIR}/lib/
# cp ~/.m2/repository/org/scala-lang/scala-library/2.13.9/scala-library-2.13.9.jar ${SOURCE_DIR}/lib

sed "s@LOG4RS_CONFIG@${GROOT_DIR}/conf/log4rs.yml@" ${GROOT_DIR}/conf/config.template ${CONFIG_FILE}

GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_HOME}/bin/store_ctl.sh start


# pip3 install graphscope_client --user
# export NODE_IP="127.0.0.1"
# export GREMLIN_PORT="12312"
# export GRPC_PORT="55556"
