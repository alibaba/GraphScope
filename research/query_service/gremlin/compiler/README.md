### Compiler Steps
* Engine Startup
```
cd query_service/gremlin/gremlin_core 
cargo run --bin main --features proto_inplace
```
* Compiler Startup
```
cd query_service/gremlin/compiler
mvn clean package -DskipTests
java -cp .:gremlin-server-plugin/target/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar com.compiler.demo.server.GremlinServiceMain
```
* Use Console to Submit Query
  - download [apache-tinkerpop-gremlin-console-3.4.9](https://archive.apache.org/dist/tinkerpop/3.4.9/apache-tinkerpop-gremlin-console-3.4.9-bin.zip)
  - conf/remote.yaml settings
    ```
    hosts: [localhost]
    port: 8182
    serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0, config: { serializeResultToString: true }}
    ```
  - console startup
    ```
    ./bin/gremlin.sh
    :remote connect tinkerpop.server conf/remote.yaml
    :remote console
    ```
  - parameterized query
    ```
    # if not set, use conf/gaia.args.json
    graph.variables().set("workers",4)
    # check variable value
    graph.variables().get("workers")
    
    ```
  - submit query in console
  