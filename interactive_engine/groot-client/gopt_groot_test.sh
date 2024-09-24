#!/bin/bash
set -x
BASE_DIR=$(cd "$(dirname "$0")"; pwd)
declare -r COMPILER_DIR=${BASE_DIR}/../compiler
declare -r DATA_IMPORT_SCRIPT_DIR=${BASE_DIR}/../groot-server/src/main/resources
declare -r CONFIG_FILE="/tmp/groot.config"
declare -r METADATA_DIR="/tmp/groot/meta"
declare -r DATA_DIR="/tmp/groot/data"
declare -r CSV_DATA_DIR="/tmp/gstest"
export LOG_DIR="/tmp/log/graphscope"

# necessary python packages for data import, including pandas, graphscope and gremlin_python
if ! python3 -c "import pandas" &> /dev/null; then
    echo "Installing pandas..."
    python3 -m pip install pandas
else
    echo "pandas is already installed."
fi

if ! python3 -c "import graphscope" &> /dev/null; then
    echo "Installing graphscope..."
    python3 -m pip install graphscope
else
    echo "graphscope is already installed."
fi

if ! python3 -c "import gremlin_python" &> /dev/null; then
    echo "Installing gremlin_python..."
    python3 -m pip install gremlinpython
else
    echo "gremlin_python is already installed."
fi

# start server
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
cd ${BASE_DIR}/../assembly/target && tar xvzf groot.tar.gz && cd groot
GROOT_DIR=$(pwd)
sed -e "s@LOG4RS_CONFIG@${GROOT_DIR}/conf/log4rs.yml@g" \
    -e "s@collect.statistics=false@collect.statistics=true@g" \
    -e "s@neo4j.bolt.server.disabled=true@neo4j.bolt.server.disabled=false@g" \
    -e "s@gremlin.server.port=12312@gremlin.server.port=8182@g" \
    -e "s@file.meta.store.path=./meta@file.meta.store.path=${METADATA_DIR}@g" \
    -e "s@store.data.path=./data@store.data.path=${DATA_DIR}@g" \
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
cd ${DATA_IMPORT_SCRIPT_DIR} && python3 import_data.py --graph modern --data_path ${CSV_DATA_DIR}/modern_graph
sleep 60
# run modern graph test
cd ${COMPILER_DIR} && make gremlin_calcite_test
exit_code=$?
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
# clean data
rm -r ${METADATA_DIR}
rm -r ${DATA_DIR}
if [ $exit_code -ne 0 ]; then
    echo "gopt_on_groot gremlin test fail"
    exit 1
fi

# start server
GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_DIR}/bin/store_ctl.sh start &
sleep 30
# load data
cd ${DATA_IMPORT_SCRIPT_DIR} && python3 import_data.py --graph movie --data_path ${CSV_DATA_DIR}/movies
sleep 60
# run movie graph test
cd ${COMPILER_DIR} && make cypher_test
exit_code=$?
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
# clean data
rm -r ${METADATA_DIR}
rm -r ${DATA_DIR}
if [ $exit_code -ne 0 ]; then
    echo "gopt_on_groot cypher test fail"
    exit 1
fi

# start server
GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_DIR}/bin/store_ctl.sh start &
sleep 30
# load data
cd ${DATA_IMPORT_SCRIPT_DIR} && python3 import_data.py --graph ldbc --data_path ${CSV_DATA_DIR}/ldbc
sleep 360
# run ldbc graph test
cd ${COMPILER_DIR} && make ldbc_test && make simple_test && make pattern_test
exit_code=$?
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
# clean data
rm -r ${METADATA_DIR}
rm -r ${DATA_DIR}
if [ $exit_code -ne 0 ]; then
    echo "gopt_on_groot ldbc test fail"
    exit 1
fi
