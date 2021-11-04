#!/bin/bash
set -x
base_dir=$(cd `dirname $0`; pwd)
ps -ef | grep "com.alibaba.maxgraph.servers.MaxNode" | grep -v grep | awk '{print $2}' | xargs kill -9
cd ${base_dir}/..
cd ./distribution/target/ && tar xvzf maxgraph.tar.gz && cd maxgraph

# start server
maxgraph_dir=$(pwd)
sed -e "s@LOG4RS_CONFIG@${maxgraph_dir}/conf/log4rs.yml@g" \
    -e "s@engine.type=maxgraph@engine.type=gaia@g" \
    conf/config.template > /tmp/max_node_gaia.config
LOG_NAME=maxnode MAXGRAPH_CONF_FILE=/tmp/max_node_gaia.config ./bin/store_ctl.sh max_node_gaia &
sleep 20
# load data
cd ${base_dir}/../sdk && mvn -Dtest=com.alibaba.graphscope.groot.sdk.DataLoadingTest test
echo "localhost:12312" > ${base_dir}/src/test/resources/graph.endpoint
cd ${base_dir} && mvn test
exit_code=$?
ps -ef | grep "com.alibaba.maxgraph.servers.MaxNode" | grep -v grep | awk '{print $2}' | xargs kill -9
# clean data
rm -fr ${maxgraph_dir}/data || true
if [ $exit_code -ne 0 ]; then
    echo "gaia_on_maxgraph_store gremlin test fail"
    exit 1
fi
