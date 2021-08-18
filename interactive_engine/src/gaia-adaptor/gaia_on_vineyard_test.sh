#!/bin/bash
base_dir=$(cd `dirname $0`; pwd)
_port=8277
rm -rf /tmp/test_modern.log
cd ${base_dir}/../../deploy/testing && python3 maxgraph_test_server.py ${_port} &
sleep 5s
# load data and start instance
curl -XPOST http://localhost:${_port} -d 'import graphscope'
curl -XPOST http://localhost:${_port} -d 'graphscope.set_option(show_log=True)'
curl -XPOST http://localhost:${_port} -d 'from graphscope.framework.loader import Loader'
curl -XPOST http://localhost:${_port} -d 'from graphscope.dataset.modern_graph import load_modern_graph'
curl -XPOST http://localhost:${_port} -d 'session=graphscope.session(cluster_type="hosts", num_workers=2, enable_gaia=True)'
curl -XPOST http://localhost:${_port} -d 'graph = load_modern_graph(session, "/home/GraphScope/interactive_engine/tests/src/main/resources/modern_graph")'
curl -XPOST http://localhost:${_port} -d 'interactive = session.gremlin(graph)' 1>/tmp/test_modern.log 2>&1
GAIA_ENDPOINT=`grep "build gaia frontend" /tmp/test_modern.log`
GAIA_ENDPOINT=`echo $GAIA_ENDPOINT | awk -F"frontend " '{print $2}' | awk -F" " '{print $1}'`
echo $GAIA_ENDPOINT > ${base_dir}/src/test/resources/graph.endpoint
cd ${base_dir} && mvn test
exit_code=$?
curl -XPOST http://localhost:${_port} -d 'session.close()'
kill -INT `lsof -i:${_port} -t`
if [ $exit_code -ne 0 ]; then
    echo "gaia_on_vineyard_store gremlin test fail"
    exit 1
fi
