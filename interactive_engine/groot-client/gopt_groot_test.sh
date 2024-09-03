#!/bin/bash
set -x
BASE_DIR=$(cd "$(dirname "$0")"; pwd)
COMPILER_DIR=${BASE_DIR}/../compiler
DATA_IMPORT_SCRIPT_DIR=${BASE_DIR}/../groot-server/src/main/resources
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
cd ${BASE_DIR}/../assembly/target && tar xvzf groot.tar.gz && cd groot

declare -r CONFIG_FILE="/tmp/groot.config"

# start server
GROOT_DIR=$(pwd)
sed -e "s@LOG4RS_CONFIG@${GROOT_DIR}/conf/log4rs.yml@g" \
    -e "s@collect.statistics=false@collect.statistics=true@g" \
    -e "s@neo4j.bolt.server.disabled=true@neo4j.bolt.server.disabled=false@g" \
    -e "\$a\
        graph.planner.is.on=true" \
    -e "\$a\
        graph.physical.opt=proto" \
    -e "\$a\
        graph.planner.opt=CBO" \
    -e "\$a\
        graph.planner.rules=FilterIntoJoinRule,FilterMatchRule,ExtendIntersectRule,JoinDecompositionRule,ExpandGetVFusionRule" \
    -e "\$a\
        gremlin.script.language.name=antlr_gremlin_calcite" \
    ${GROOT_DIR}/conf/config.template > ${CONFIG_FILE}

GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_DIR}/bin/store_ctl.sh start &

sleep 30
# load data
cd ${DATA_IMPORT_SCRIPT_DIR} && python3 import_data.py --graph modern
sleep 60
# run modern graph test
cd ${COMPILER_DIR} && make gremlin_calcite_test
exit_code=$?
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
# clean data
rm -r ${GROOT_DIR}/data 
rm -r ${GROOT_DIR}/meta 
if [ $exit_code -ne 0 ]; then
    echo "gaia_on_groot gremlin test fail"
    exit 1
fi

# start server
GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_DIR}/bin/store_ctl.sh start &
sleep 30
# load data
cd ${DATA_IMPORT_SCRIPT_DIR} && python3 import_data.py --graph movie
sleep 60
# run movie graph test
cd ${COMPILER_DIR} && make cypher_test
exit_code=$?
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
# clean data
rm -r ${GROOT_DIR}/data 
rm -r ${GROOT_DIR}/meta 
if [ $exit_code -ne 0 ]; then
    echo "gaia_on_groot cypher test fail"
    exit 1
fi

# start server
GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_DIR}/bin/store_ctl.sh start &
sleep 30
# load data
cd ${DATA_IMPORT_SCRIPT_DIR} && python3 import_data.py --graph ldbc
sleep 60
# run ldbc graph test
cd ${COMPILER_DIR} && make ldbc_test && make simple_test && make pattern_test
exit_code=$?
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
# clean data
rm -r ${GROOT_DIR}/data 
rm -r ${GROOT_DIR}/meta 
if [ $exit_code -ne 0 ]; then
    echo "gaia_on_groot ldbc test fail"
    exit 1
fi
