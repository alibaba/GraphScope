#!/bin/bash
set -e
set -x
base_dir=$(cd $(dirname $0); pwd)
_port=8277
rm -rf /tmp/test_modern.log
cd ${base_dir}/../deploy/testing && python3 maxgraph_test_server.py ${_port} &
sleep 5s
# load data and start instance
curl -XPOST http://localhost:${_port} -d 'import sys'
curl -XPOST http://localhost:${_port} -d 'sys.path.insert(0, "/home/GraphScope/python")'
curl_sess="curl -XPOST http://localhost:${_port} -d 'sys.path.insert(0, \"${GITHUB_WORKSPACE}/python\")'"
sh -c "$curl_sess"
curl -XPOST http://localhost:${_port} -d 'import graphscope'
curl -XPOST http://localhost:${_port} -d 'graphscope.set_option(show_log=True)'
curl -XPOST http://localhost:${_port} -d 'from graphscope.framework.loader import Loader'
curl -XPOST http://localhost:${_port} -d 'from graphscope.dataset import load_modern_graph'
curl -XPOST http://localhost:${_port} -d 'session=graphscope.session(cluster_type="hosts", num_workers=2)'
curl_sess="curl -XPOST http://localhost:${_port} -d 'graph = load_modern_graph(session, \"${GITHUB_WORKSPACE}/interactive_engine/tests/src/main/resources/modern_graph\")'"
sh -c "$curl_sess"
curl -XPOST http://localhost:${_port} -d 'interactive = session.gremlin(graph)' --output /tmp/test_modern.log
curl -XPOST http://localhost:${_port} -d 'interactive._graph_url[0]' --output /tmp/test_modern.log
GAIA_PORT=$(awk -F'\/|:' '{print $5}' < /tmp/test_modern.log)
cd ${base_dir}/.. &&  mvn clean install -DskipTests -Pjava-release
cd ${base_dir} && mvn test -Dtest=com.alibaba.graphscope.ir.maxgraph.IrGremlinTest -Dgremlin.endpoint="localhost:${GAIA_PORT}"
exit_code=$?
curl -XPOST http://localhost:${_port} -d 'session.close()'
ps -ef | grep "python3 maxgraph_test_server.py ${_port}" | grep -v grep | awk '{print $2}' | xargs kill -9
if [ $exit_code -ne 0 ]; then
    echo "ir integration test on vineyard store fail"
    exit 1
fi
