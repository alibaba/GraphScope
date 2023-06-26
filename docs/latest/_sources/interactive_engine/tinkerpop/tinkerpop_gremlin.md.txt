# GIE for Gremlin
This document will provide you with step-by-step guidance on how to connect your gremlin applications to the GIE's
FrontEnd service, which offers functionalities similar to the official Tinkerpop service.

Your first step is to obtain the endpoint of GIE Frontend service:
- Follow the [instruction](./deployment.md#deploy-your-first-gie-service) while deploying GIE in a K8s cluster,
- Follow the [instruction](./dev_and_test.md#manually-start-the-gie-services) while starting GIE on a local machine.

## Connecting via Python SDK

GIE makes it easy to connect to a loaded graph with Tinkerpop's [Gremlin-Python](https://pypi.org/project/gremlinpython/).

You first install the dependency:
```bash
pip3 install gremlinpython
```

Then connect to the service and run queries:

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

In large-scale data processing scenarios, streaming the returned data is often necessary to avoid Out of Memory (OOM) issues caused by handling a large volume of data, which provides benefits such as memory efficiency, continuous processing, incremental analysis, reduced latency, scalability, and resource optimization. It enables you to handle and analyze vast amounts of data effectively while mitigating the risk of memory-related issues. Here is an example to guide you how to collect results in a streaming way by python sdk.
   ```Python
   from queue import Queue
   from gremlin_python.driver.client import Client

   graph_url = # the GIE Frontend service endpoint you've obtained
   client = Client(graph_url, "g")

   ret = []
   q = client.submit('g.V()')
   while True:
   try:
      ret.extend(q.next())
   except StopIteration:
      break

   print(ret)
   ```
Furthermore, here are some parameters that can be used to configure the streaming size on the server-side.
```bash
# interactive_engine/compiler/src/main/resources/conf/gremlin-server.yaml
...
# total num of streaming batch size returned by compiler service
resultIterationBatchSize: 64
...

```

## Connecting via Java SDK
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

## Connecting via Gremlin-Console
1. Download Gremlin console and unpack to your local directory.
   ```bash
   # if the given version (3.6.4) is not found, try to access https://dlcdn.apache.org to
   # download an available version.
   curl -LO https://dlcdn.apache.org/tinkerpop/3.6.4/apache-tinkerpop-gremlin-console-3.6.4-bin.zip && \
   unzip apache-tinkerpop-gremlin-console-3.6.4-bin.zip && \
   cd apache-tinkerpop-gremlin-console-3.6.4
   ```

2. In the directory of Gremlin console, modify the `hosts` and `port` in `conf/remote.yaml` to the GIE Frontend Service endpoint, as
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
