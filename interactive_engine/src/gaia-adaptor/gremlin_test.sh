#!/bin/bash
base_dir=$(cd `dirname $0`; pwd)
ps -ef | grep "com.alibaba.graphscope.gaia.MaxNode" | grep -v grep | awk '{print $2}' | xargs kill -9
cd ${base_dir}/../.. && mvn clean install -DskipTests -Pv2
cd ./distribution/target/ && sudo tar xvzf maxgraph.tar.gz -C ${GRAPHSCOPE_HOME}
# start server
${GRAPHSCOPE_HOME}/bin/giectl max_node_gaia max_node ${GRAPHSCOPE_HOME}/config/sample.config
sleep 20
cd ${base_dir} && mvn test
exit_code=$?
ps -ef | grep "com.alibaba.graphscope.gaia.MaxNode" | grep -v grep | awk '{print $2}' | xargs kill -9
if [ $exit_code -ne 0 ]; then
    echo "gaia_on_maxgraph_store gremlin test fail"
    exit 1
fi
