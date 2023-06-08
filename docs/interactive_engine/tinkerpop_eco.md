# Apache TinkerPop Ecosystem
[Apache TinkerPop](http://tinkerpop.apache.org/) is an open framework for developing interactive graph applications using the Gremlin query language.  GIE implements TinkerPop's [Gremlin Server](https://tinkerpop.apache.org/docs/current/reference/#gremlin-server) interface so that the system can seamlessly interact with the TinkerPop ecosystem, including development tools such as [Gremlin Console] (https://tinkerpop.apache.org/docs/current/reference/#gremlin-console) and language wrappers such as Java and Python.

All you need to connect with existing Tinkerpop ecosystem is to obtain the GIE Frontend service endpoint.
How to do that?
- Follow the [instruction](./deployment.md#deploy-your-first-gie-service) while deploying GIE in a K8s cluster,
- Follow the [instruction](./dev_and_test.md#manually-start-the-gie-services) while starting GIE on a local machine.

## Connecting Gremlin within Python

GIE makes it easy to connect to a loaded graph with Tinkerpop's [Gremlin-Python](https://pypi.org/project/gremlinpython/).

   ```Python
   import sys
   from gremlin_python import statics
   from gremlin_python.structure.graph import Graph
   from gremlin_python.process.graph_traversal import __
   from gremlin_python.process.strategies import *
   from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

   graph = Graph()
   gremlin_endpoint = # the GIE Frontend service endpoint you've obtained
   remoteConn = DriverRemoteConnection('ws://' + gremlin_endpoint + '/gremlin','g')
   g = graph.traversal().withRemote(remoteConn)

   res = g.V().count().next()
   assert res == 6
   ```

````{hint}
A simpler option is to use the `gremlin` object for submitting Gremlin queries through
[GraphScope's python SDK](./getting_started.md), which is a wrapper that encompasses Tinkerpop's
 Gremlin-Python and will automatically acquire the endpoint.
````

## Connecting Gremlin within Java
See [Gremlin-Java](https://tinkerpop.apache.org/docs/current/reference/#gremlin-java) for connecting Gremlin
within the Java language.

Here is an example to guide you how to collect results in a streaming way by java sdk.
```java
Cluster cluster = Cluster.build()
         .addContactPoint("localhost") // use your host ip
         .port(8182) // use your port
         .create();
Client client = cluster.connect();
ResultSet resultSet = client.submit("g.V()"); // use your query
Iterator<Result> results = resultSet.iterator();
while(results.hasNext()) {
   display(results.next()); // display each result in your way
}
client.close();
cluster.close();
```

## Gremlin Console
1. Download Gremlin console and unpack to your local directory.
   ```bash
   # if the given version (3.6.4) is not found, try to access https://dlcdn.apache.org to
   # download an available version.
   curl -LO https://dlcdn.apache.org/tinkerpop/3.6.4/apache-tinkerpop-gremlin-console-3.6.4-bin.zip && \
   unzip apache-tinkerpop-gremlin-console-3.6.4-bin.zip && \
   cd apache-tinkerpop-gremlin-console-3.6.4
   ```

2. In the directory of gremlin console, modify the `hosts` and `port` in `conf/remote.yaml` to the GIE Frontend Service endpoint, as
  ```bash
  hosts: [your_endpoint_address]
  port: [your_endpoint_port]
  serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0, config: { serializeResultToString: true }}
  ```

3. Open the Gremlin console
   ```bash
   chmod +x bin/gremlin.sh
   bin/gremlin.sh
   ```

4. At the `gremlin>` prompt, enter the following to connect to the GraphScope session and switch to remote mode so that all
subsequent Gremlin queries will be sent to the remote connection automatically.
   ```bash
   gremlin> :remote connect tinkerpop.server conf/remote.yaml
   gremlin> :remote console
   gremlin> g.V().count()
   ==> 6
   gremlin>
   ```

5. You are now ready to submit any Gremlin queries via either the Python SDK or Gremlin console.

6. When you are finished, enter the following to exit the Gremlin Console.
```bash
gremlin> :exit
```

## Compatibility with TinkerPop
GIE supports the property graph model and Gremlin traversal language defined by Apache TinkerPop,
and provides a Gremlin Websockets server that supports TinkerPop version 3.4.
In addition to the original Gremlin queries, we further introduce some syntactic sugars to allow
more succinct expression. However, because of the distributed nature and practical considerations, it is worth to notice the following limitations of our implementations of Gremlin.

- Functionalities
  - Graph mutations.
  - Lambda and Groovy expressions and functions, such as the `.map{<expression>}`, the `.by{<expression>}`, and the `.filter{<expression>}` functions, and `System.currentTimeMillis()`, etc. By the way, we have provided the `expr()` [syntactic sugar](../interactive_engine/supported_gremlin_steps.md) to handle complex expressions.
  - Gremlin traversal strategies.
  - Transactions.
  - Secondary index isn’t currently available. Primary keys will be automatically indexed.

- Gremlin Steps: See [here](supported_gremlin_steps.md) for a complete supported/unsupported list of Gremlin.

## Property Graph Constraints
The current release of GIE supports two graph stores: one leverages [Vineyard](https://v6d.io/) to supply an in-memory store for immutable
graph data, and the other, called [groot](../storage_engine/groot.md), is developed on top of [RocksDB](https://rocksdb.org/) that also provides real-time write and data consistency via [snapshot isolation](https://en.wikipedia.org/wiki/Snapshot_isolation). Both stores support graph data being partitioned across multiple servers. By design, the following constraints are introduced (on both stores):
 - Each graph has a schema comprised of the edge labels, property keys, and vertex labels used therein.
 - Each vertex type or label has a primary key (property) defined by user. The system will automatically
  generate a String-typed unique identifier for each vertex and edge, encoding both the label information
  as well as user-defined primary keys (for vertex).
 - Each vertex or edge property can be of the following data types: `int`, `long`, `float`, `double`,
  `String`, `List<int>`, `List<long>`, and `List<String>`.
