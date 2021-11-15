#!/bin/bash
set -e
set -x
base_dir=$(cd `dirname $0`; pwd)
_port=8277
rm -rf /tmp/test_modern.log
cd ${base_dir}/../deploy/testing && python3 maxgraph_test_server.py ${_port} &
sleep 5s
# load data and start instance
curl -XPOST http://localhost:${_port} -d 'import graphscope'
curl -XPOST http://localhost:${_port} -d 'graphscope.set_option(show_log=True)'
curl -XPOST http://localhost:${_port} -d 'from graphscope.framework.loader import Loader'
curl -XPOST http://localhost:${_port} -d 'from graphscope.dataset import load_modern_graph'
curl -XPOST http://localhost:${_port} -d 'session=graphscope.session(cluster_type="hosts", num_workers=2, enable_gaia=True)'
curl_sess="curl -XPOST http://localhost:${_port} -d 'graph = load_modern_graph(session, \"${GITHUB_WORKSPACE}/interactive_engine/tests/src/main/resources/modern_graph\")'"
sh -c "$curl_sess"
curl -XPOST http://localhost:${_port} -d 'interactive = session.gremlin(graph)' --output /tmp/test_modern.log
curl -XPOST http://localhost:${_port} -d 'interactive._graph_url[1]' --output /tmp/test_modern.log
GAIA_PORT=`cat /tmp/test_modern.log | awk -F'\/|:' '{print $5}'`
echo "localhost:$GAIA_PORT" > ${base_dir}/src/test/resources/graph.endpoint
cd ${base_dir}/.. &&  mvn clean install -DskipTests -Pjava-release
cd ${base_dir} && mvn test
exit_code=$?
curl -XPOST http://localhost:${_port} -d 'session.close()'
ps -ef | grep "python3 maxgraph_test_server.py ${_port}" | grep -v grep | awk '{print $2}' | xargs kill -9
if [ $exit_code -ne 0 ]; then
    echo "gaia_on_vineyard_store gremlin test fail"
    exit 1
fi
