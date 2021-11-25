# How to run gremlin tests
* Build Artifacts
```
cd GraphScope/research/gaia/scripts
./build.sh
```
* Start RPC Server
```
# default load modern graph
RUST_LOG=Info bin/start_rpc_server
```
* Config Graph Schema
```
# conf/graph.properties
gremlin.graph.schema=conf/modern.schema.json
```
* Start Gremlin Server
```
java -cp bin/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GremlinServiceMain
```
* Run Gremlin Tests
```
cd GraphScope/research/query_service/gremlin/compiler/gremlin-test
mvn test
```