#!/bin/bash
set -x
BASE_DIR=$(cd "$(dirname "$0")"; pwd)
declare -r FLEX_HOME=${BASE_DIR}/../../flex
declare -r CONFIG_FILE="/tmp/groot.config"
declare -r METADATA_DIR="/tmp/groot/meta"
declare -r DATA_DIR="/tmp/groot/data"
export LOG_DIR="/tmp/log/graphscope"

# start server
ps -ef | grep "com.alibaba.graphscope.groot.servers.GrootGraph" | grep -v grep | awk '{print $2}' | xargs kill -9
cd ${BASE_DIR}/../assembly/target && tar xvzf groot.tar.gz && cd groot
GROOT_DIR=$(pwd)
sed -e "s@LOG4RS_CONFIG@${GROOT_DIR}/conf/log4rs.yml@g" \
    -e "s@collect.statistics=false@collect.statistics=true@g" \
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

# test python sdk for groot (the openapi client)
# start groot and groot http server
GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_DIR}/bin/store_ctl.sh start &
sleep 30
GROOT_CONF_FILE=${CONFIG_FILE} ${GROOT_DIR}/bin/store_ctl.sh start_http &
sleep 30

# install groot client
cd ${FLEX_HOME}/interactive/sdk/
bash generate_sdk.sh -g python
cd python
pip3 install -r requirements.txt
pip3 install -r test-requirements.txt
python3 setup.py build_proto
pip3 install .

export ENGINE_TYPE="insight"
cd ${FLEX_HOME}/interactive/sdk/python/gs_interactive
cmd="python3 -m pytest -s tests/test_driver.py"
echo "Run python sdk test: ${cmd}"
eval ${cmd} || (err "test_driver failed" &&  exit 1)
info "Finish python sdk test"