
# Groot: Persistent Graph Store

## Overview
In addition to Vineyard, the in-memory columnar graph store supported in GraphScope, we also have a disk-based, row-oriented, multi-versioned, persistent graph store. While Vineyard focuses on great support for in-memory whole graph analytics workloads, the persistent graph store is geared towards better supporting continuous graph data management services that frequently update the graph and answer traversal queries.

The store is a distributed graph store built on top of the popular RocksDB key-value store. It adopts a row-oriented design to support frequent small updates to the graph. Each row is tagged with a snapshot ID as its version. A query reads the most recent version of rows relative to the snapshot ID when it starts and is hence not blocked by writes. For writes, we take a compromise between consistency and higher throughput. In our design, writes in the same session can be grouped and executed atomically as a unit, and the persistent store assigns a snapshot ID (which is a low-resolution timestamp of the current time) to each group and executes groups of writes by the order of their snapshot IDs and by a deterministic (though arbitrary) order for groups of writes that occur in the same snapshot ID. It provides high write throughput while still maintaining some degree of order and isolation, although it provides less consistency than strict snapshot isolation common in databases. We hope our design choice provides an interesting trade-off for practical usage.

## Known Limitation
Initially, the new persistent store is provided as a separate option from Vineyard, and it can accept Gremlin queries for data access. Going forward we hope to evolve them into an integrated hybrid graph store suitable for all kinds of workloads.

## Deploy Groot

We use [Helm](https://helm.sh) to deploy Groot on [Kubernetes](http://kubernetes.io) Cluster.

### Prerequisites
- Kubernetes 1.21+
- Helm 3.2.0+

If you don't have a Kubernetes cluster, you can create a local one by using Docker Desktop, Minikube, or Kind.

If you don't have a Kubernetes cluster, you can create a local one [Docker Desktop](https://docs.docker.com/desktop/kubernetes/), [minikube](https://minikube.sigs.k8s.io/docs/start/) or [kind](https://kind.sigs.k8s.io/).

Refer to [deploy graphscope on self managed k8s cluster](../deployment/deploy_graphscope_on_self_managed_k8s.md) for more details guide.

### Installation

#### Install from ArtifactHub

The latest stable version of [Groot](https://artifacthub.io/packages/helm/graphscope/graphscope-store) can be installed from ArtifactHub by using the following command:

```bash
helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/charts/
helm repo update
helm install demo graphscope/graphscope-store
```

#### Installing from a local directory

If you want to apply the latest updates or modify some files, you can clone the [GraphScope](https://github.com/alibaba/GraphScope) repository and install Groot from a local directory by using the following commands:

```bash
cd GraphScope/charts/graphscope-store
helm dependency update  # fetch the dependency charts
helm install demo .
```

The above commands will deploy Groot with the default configuration. The configurable items during installation can be found in the `Common Configurations` section.

It may take some time for the Groot service to be available because the image needs to be pulled for the first time. You can check if the service is available by using the following command:

```bash
helm test demo
```

Helm will print the following statement on the console, which you could copy and execute to get the connection address. 

You can also check the deployment status and get the connection address by using the following command:

```bash
helm status demo
```


### Common Configurations
| Name | Description | Default value |
| --- | --- | --- |
| image.registry | Image registry | registry.cn-hongkong.aliyuncs.com |
| image.repository | Image repository | graphscope/graphscope-store |
| image.tag | Image tag, default to the version of the Chart | "" |
| auth.username | Username. If empty, then there's no authentication | "" |
| auth.password | Password | "" |
| store.replicaCount | Number of Store Pod | 2 |
| dataset.modern | Load [modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) dataset at the start | false |
| frontend.replicaCount | Number of Frontend | 1 |
| frontend.service.type | Kubernetes Service type of frontend | NodePort |
| frontend.query.per.second.limit | the maximum qps can be handled by frontend service | 2147483647 (without limitation) |
| query.execution.timeout.ms | the total execution time for a query | 3000000 |
| frontend.service.httpPort | Groot http service port | 8080|
| neo4j.bolt.server.disabled | Disable neo4j or not | true |


If Groot is launched with the default configuration, then two Store Pods, one Frontend Pod, and one Coordinator Pod will be started. The number of Coordinator nodes is fixed to 1.

Use the `--set key=value[,key=value]` command to set the parameters for `helm install`, for example:

```bash
helm install demo graphscope/graphscope-store \
    --set auth.username=admin,auth.password=123456
```

The aforementioned command configures the username and password required for connecting to the cluster.

In situations where a multitude of parameters need to be set, utilizing the `--set` option can become difficult to manage. In such cases, one can specify the parameters using a YAML file, as exemplified below:

```bash
helm install demo graphscope/graphscope-store -f settings.yaml
```

A sample configuration for `settings.yaml` is like the following:

```yaml
# cat settings.yaml
---
image:
  tag: latest
auth:
  username: admin
  password: 123456
```

It will specify the image tag to be pulled as latest while setting the username and password.

## User Guide
Upon installing Groot, an empty graph is created by default. We can execute connections, define graph models, load data, and perform queries with [Gremlin Query Language](https://tinkerpop.apache.org/gremlin.html) and [Cypher Query Language](https://neo4j.com/docs/getting-started/cypher/).

We can use the [Interactive SDK](../flex/interactive/development/python/python_sdk_ref.md) to interact with Groot. The Interactive SDK offers a unified interface for managing and querying interactive engines, including the [GIE](../interactive_engine/design_of_gie.md) based on Groot store for low-latency demands, and [GraphScope Interactive](../flex/interactive_intro.md) for high-QPS demands. We provide the following workflow example.

### Installation

Once the service is up and running, the Groot HTTP service will be activated by default. You can connect to the service using the Interactive SDK. To install the SDK, use the following pip command:

```bash
pip3 install gs_interactive
```

Then import the package:
```python
import gs_interactive
```

 For more details, please refer to [Python SDK Guide](../flex/interactive/development/python/python_sdk.md#installation--usage).

### Connection

To connect the service, ensure that the following environment variables are properly set to facilitate the connection:

```bash
############################################################################################
    export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:8080
############################################################################################
```

Then you can connect to the Groot Service as follows:
```python
import gs_interactive

driver = Driver()
sess = driver.session()
```
Once the connection is established, you can use the `driver`and `sess` objects to interact with the Groot Service, as illustrated in the following demonstrations.

Note: After executing the Helm command to obtain connection information, the details are stored in environment variables. Retrieve the `NODE_IP` from these variables and replace `127.0.0.1` in your connection string with its value. Besides, the service endpoint port can be customized using the `frontend.service.httpPort` option, which defaults to 8080. If you have customized the ports when deploying Interactive, ensure you replace the default port with your specified port.


### Create a new graph

To create a new graph, you need to specify the name, description, vertex types and edges types.
For the detail data model of the graph, please refer to [Data Model](../../data_model). 

In this example, we will create a simple graph with only one vertex type `persson`, and one edge type named `knows`.

```python
def create_graph(sess : Session):
    # Define the graph schema via a python dict.
    test_graph_def = {
        "name": "test_graph",
        "description": "This is a test graph",
        "schema": {
            "vertex_types": [
                {
                    "type_name": "person",
                    "properties": [
                        {
                            "property_name": "id",
                            "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                        },
                        {
                            "property_name": "name",
                            "property_type": {"string": {"long_text": ""}},
                        },
                        {
                            "property_name": "age",
                            "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                        },
                    ],
                    "primary_keys": ["id"],
                }
            ],
            "edge_types": [
                {
                    "type_name": "knows",
                    "vertex_type_pair_relations": [
                        {
                            "source_vertex": "person",
                            "destination_vertex": "person",
                            "relation": "MANY_TO_MANY",
                        }
                    ],
                    "properties": [
                        {
                            "property_name": "weight",
                            "property_type": {"primitive_type": "DT_DOUBLE"},
                        }
                    ],
                    "primary_keys": [],
                }
            ],
        },
    }
    create_graph_request = CreateGraphRequest.from_dict(test_graph_def)
    resp = sess.create_graph(create_graph_request)
    assert resp.is_ok()
    return resp.get_value().graph_id

graph_id = create_graph(sess)
print("Created graph, id is ", graph_id)
```

In the aforementioned example, a graph named `test_graph` is defined using a python dictionaly. You can also define the graph using the programmatic interface provided by [CreateGraphRequest](./CreateGraphRequest.md). Upon calling the `createGraph` method, a string representing the unique identifier of the graph is returned.

````{note}
You might observe that we define the graph schema in YAML with `gsctl`, but switch to using `dict` in Python code. You may encounter challenges when converting between different formats.
However, converting `YAML` to a Python `dict` is quite convenient.

First, install pyYAML

```bash
pip3 install pyYAML
```

Then use pyYAML to convert the YAML string to a Python dict

```python
import yaml

yaml_string = """
...
"""

python_dict = yaml.safe_load(yaml_string)

print(python_dict)
```

Afterwards, you can create a `CreateGraphRequest` from the Python dict.
````

### Import data to the graph

After creating a new graph, you may want to import data into it. Real-time data writing is currently supported via the HTTP service. 

For example, you can insert vertices and edges as follows:

```python
# Add vertices and edges
vertex_request = [
    VertexRequest(
        label="person",
        primary_key_values= [
            ModelProperty(name="id", value=1),
        ],
        properties=[
            ModelProperty(name="name", value="Alice"),
            ModelProperty(name="age", value=20),
        ],
    ),
    VertexRequest(
        label="person",
        primary_key_values= [
            ModelProperty(name="id", value=8),
        ],            
        properties=[
            ModelProperty(name="name", value="mike"),
            ModelProperty(name="age", value=1),
        ],
    ),
]
edge_request = [
    EdgeRequest(
        src_label="person",
        dst_label="person",
        edge_label="knows",
        src_primary_key_values=[ModelProperty(name="id", value=8)],
        dst_primary_key_values=[ModelProperty(name="id", value=1)],
        properties=[ModelProperty(name="weight", value=7)],
    ),
]
api_response = sess.add_vertex(graph_id, vertex_edge_request=VertexEdgeRequest(vertex_request=vertex_request, edge_request=edge_request))

# the response will return the snapshot_id after the realtime write.
snapshot_id = ast.literal_eval(api_response.get_value()).get("snapshot_id")
# get the snapshot status to check if the written data is available for querying
snapshot_status =  sess.get_snapshot_status(graph_id, snapshot_id)

```

Additionally, we provide an offline data loading tool. For more information, refer to [Offline Data Loading](./groot.md#offline-data-loading).

### Query Data

Now you may want to query the data. We support both Gremlin and Cypher query languages.

#### Submit Gremlin Queries

You can submit gremlin queries as follows:

```python
gremlin_client = driver.getGremlinClient()
resp = gremlin_client.submit("g.V().count()")
for result in resp:
    print(result)
```

#### Submit Cypher Queries

You can submit cypher queries as follows:

```python
neo4j_session = driver.getNeo4jSession()
resp = neo4j_session.run("MATCH (n) RETURN COUNT(n);")
for result in resp:
    print(result)
```

Note: The Neo4j Bolt protocol is disabled by default in Groot. To enable Cypher queries, set `neo4j.bolt.server.disabled=false`.

### Modify the graph schema
You may want to modify the graph schema to accommodate new types of vertices or edges, add properties to existing types, or delete existing types as needed.

For example, you can create new vertex and edge types as follows:

```python
# create new vertex type
create_vertex_type = CreateVertexType(
    type_name="new_person",
    properties=[
        CreatePropertyMeta(
            property_name="id",
            property_type=GSDataType.from_dict({"primitive_type": "DT_SIGNED_INT64"}),
        ),
        CreatePropertyMeta(
            property_name="name",
            property_type=GSDataType.from_dict({"string": {"long_text": ""}}),
        ),
    ],
    primary_keys=["id"],
)
api_response = sess.create_vertex_type(graph_id, create_vertex_type)

# create new edge type
create_edge_type = CreateEdgeType(
    type_name="new_knows",
    vertex_type_pair_relations=[
        BaseEdgeTypeVertexTypePairRelationsInner(
            source_vertex="new_person",
            destination_vertex="new_person",
            relation="MANY_TO_MANY",
        )
    ],
    properties=[
        CreatePropertyMeta(
            property_name="weight",
            property_type=GSDataType.from_dict({"primitive_type": "DT_DOUBLE"}),
        )
    ],
)
api_response = sess.create_edge_type(graph_id, create_edge_type)
```

### Delete the graph

Finally, you can delete the graph, as follows:

```python
resp = sess.delete_graph(graph_id)
assert resp.is_ok()
print("delete graph res: ", resp)
```

For the full example on Groot, please refer to [Groot Python SDK Example](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/groot-http/example/python_example.py).

<!-- ### Querying Data
#### Python

Using the connection information obtained earlier, we can perform Gremlin queries in Python.

```python
g = conn.gremlin()
print(g.V().count().toList())
```

Alternatively, we can directly retrieve the Gremlin IP address and port from the connection information and use the `gremlinpython` library for querying.

1. Install the `gremlinpython` package

```bash
pip install gremlinpython ‑‑user
```

2. Copy the following code and set the `endpoint` to the connection information obtained earlier:

```python
import os
from gremlin_python.driver.client import Client

endpoint = f"{os.environ['NODE_IP']}:{os.environ['GREMLIN_PORT']}"
graph_url = f"ws://{endpoint}/gremlin"
username = "<username>"
password = "<password>"
client = Client(
    graph_url,
    "g",
    username=username,  # If auth enabled
    password=password,  # If auth enabled
    )
print(client.submit("g.V().limit(2)").all().result())
client.close()
``` -->

## Offline Data Loading

There are two methods for importing data. The first is real-time writing, as introduced in the [previous section](./groot.md#import-data-to-the-graph). The second is batch importing data from external storage, such as HDFS, using an offline import tool. This section introduces the offline data loading process.

Note: Offline import will **overwrite** the full data of the imported label.

### Prerequisite
- Hadoop Cluster
- Data import tool [data_load.tar.gz](https://github.com/alibaba/GraphScope/releases/download/v0.20.0/data_load.tar.gz)

Extract data_load.tar.gz, where `data_load/bin/load_tool.sh` is the tool that will be used below.

```bash
tar xzvf data_load.tar.gz
```

### Data Format
Source data needs to be stored in HDFS in a certain format. Each file includes data related to a type of vertex or edge label. 

The following is an example of the data related to the `person` vertex label and the `knows` edge label, which contains the `person`->`knows`<-`person` relationship.

- `person.csv`

```csv
id|name
1000|Alice
1001|Bob
```

- `person_knows_person.csv`

```csv
person_id|person_id_1|date
1000|1001|20210611151923
```


The first line of the data file is a header that describes the key of each field. The header is not required. If there is no header in the data file, you need to set skip.header to true in the data building process (For details, see params description in “Building a partitioned graph”).

The rest lines are the data records. Each line represents one record. Data fields are separated by a custom separator (“|” in the example above). In the vertex data file person.csv, id field and name field are the primary-key and the property of the vertex type person respectively. In the edge data file person_knows_person.csv, person_id field is the primary-key of the source vertex, person_id_1 field is the primary-key of the destination vertex, date is the property of the edge type knows.

All the data fields will be parsed according to the data-type defined in the graph schema. If the input data field cannot be parsed correctly, data building process would be failed with corresponding errors.

### Loading Process
The loading process contains three steps:

1. A partitioned graph is built from the source files and stored in the same HDFS using a MapReduce job

2. The graph partitions are loaded into the store servers (in parallel)

3. Commit to the online service so that data is ready for serving queries

#### 1. Build: Building a partitioned graph

  Build data by running the hadoop map-reduce job with following command:
  
  ```
  $ ./load_tool.sh build <path/to/config/file>
  ```

  The config file should follow a format that is recognized by Java `java.util.Properties` class. Here is an example:
  
```
split.size=256
separator=\\|
input.path=/tmp/ldbc_sample
output.path=/tmp/data_output
graph.endpoint=1.2.3.4:55555
column.mapping.config={"person_0_0.csv":{"label":"person","propertiesColMap":{"0":"id","1":"name"}},"person_knows_person_0_0.csv":{"label":"knows","srcLabel":"person","dstLabel":"person","srcPkColMap":{"0":"id"},"dstPkColMap":{"1":"id"},"propertiesColMap":{"2":"date"}}}
skip.header=true
load.after.build=true
# This is not required when load.after.build=true
# hadoop.endpoint=127.0.0.1:9000
```
  
  Details of the parameters are listed below:
  
  | Config key            | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
  |-----------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
  | split.size            | false    | 256     | Hadoop map-reduce input data split size in MB                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
  | separator             | false    | \\\\\|  | Separator used to parse each field in a line                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | 
  | input.path            | true     | -       | Input HDFS dir                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
  | output.path           | true     | -       | Output HDFS dir                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
  | graph.endpoint        | true     | -       | RPC endpoint of the graph storage service. You can get the RPC endpoint following this document: [GraphScope Store Service](https://github.com/alibaba/GraphScope/tree/main/charts/graphscope-store)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
  | column.mapping.config | true     | -       | Mapping info for each input file in JSON format. Each key in the first level should be a fileName that can be found in the `input.path`, and the corresponding value defines the mapping info. For a vertex type, the mapping info should includes 1) `label` of the vertex type, 2) `propertiesColMap` that describes the mapping from input field to graph property in the format of `{ columnIdx: "propertyName" }`. For an edge type, the mapping info should includes 1) `label` of the edge type, 2) `srcLabel` of the source vertex type, 3) `dstLabel` of the destination vertex type, 4) `srcPkColMap` that describes the mapping from input field to graph property of the primary keys in the source vertex type, 5) `dstPkColMap` that describes the mapping from input field to graph property of the primary keys in the destination vertex type, 6) `propertiesColMap` that describes the mapping from input field to graph property of the edge type |
  | skip.header           | false    | true    | Whether to skip the first line of the input file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
  | load.after.build      | false    | false   | Whether to immediately ingest and commit the built files                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
  | hadoop.endpoint       | false    | -       | Endpoint of hadoop cluster in the format of <host>:<ip>. Not required when `load.after.build` is set to true                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |

  After data building completed, you can find the output files in the `output.path` of HDFS. The output files includes a 
  meta file named `META`, an empty file named `_SUCCESS`, and some data files that one for each partition named in the 
  pattern of `part-r-xxxxx.sst`. The layout of the output directory should look like:
  
```
/tmp/data_output
  |- META
  |- _SUCCESS
  |- part-r-00000.sst
  |- part-r-00001.sst
  |- part-r-00002.sst
  ...
```

If `load.after.build=true`, then you can skip step 2 and 3.
Else, please proceed to ingest and commit.

#### 2. Loading graph partitions
  
  Now ingest the offline built data into the graph storage. Run:
  
  ```
  $ ./load_data.sh ingest <path/to/config/file>
  ```

  The offline built data can be ingested successfully only once, otherwise errors will occur.

#### 3. Commit to store service
  
  After data ingested into graph storage, you need to commit data loading. The data will not be able to read until committed successfully. Run:
  
  ```
  $ ./load_data.sh commit <path/to/config/file>
  ```

  **Note: The later committed data will overwrite the earlier committed data which have same vertex types or edge relations.**




### Other features

Groot could enable user to replay realtime write records from a specific offset, or a timestamp, this is useful when you want to restore some records before
a offline load finished, since offload will overwrite all records.

You can only specify one of `offset` and `timestamp`. The other unused one must be set to -1. If not, `offset` will take precedence.

Example API:
- Python:
    ```python
    import time
    import graphscope
    conn = graphscope.conn()
    current_timestamp = int(time.time() * 1000) - 100 * 60 * 1000

    r = conn.replay_records(-1, current_timestamp)
    ```
- Java

    ```java
    GrootClient client = GrootClientBuilder.build();
    long timestamp = System.currentTimeMillis();
    client.replayRecords(-1, timestamp);
    ```

## Uninstalling and Restarting

### Uninstall Groot

To uninstall/delete the `demo` Groot cluster deployment, use

```bash
helm delete demo
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

If the cluster supports [dynamic provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/), Groot will create a set of [PersistentVolumeClaims (PVCs)](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims) to claim [PersistentVolumes (PVs)](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) by default for storing metadata and graph data. The PVs will not be deleted by default when Groot is uninstalled. You can query the PVCs and PVs using the following commands.

```bash
kubectl get pvc
kubectl get pv
# To query only the PVC belonging to the demo deployment
kubectl get pvc -lapp.kubernetes.io/instance=demo
```

### Restart Groot

To relaunch Groot on the original PV with the same command used for the initial installation. At this point, Groot can access the data from before the uninstallation, and all other operations are the same as before the uninstallation. This can facilitate seamless version updates, or when using cloud provider services, you can uninstall Groot on demand to release elastic computing resources and keep only the block storage to save costs.


```bash
# Note that if the node count is configured during installation, it should be exactly the same when reinstalling.
helm install demo graphscope/graphscope-store
```

### Destroy Groot

Destroying Groot means releasing all resources used by Groot, including StatefulSets, Services, PVCs, and PVs.

```bash
helm delete demo
kubectl delete pvc -lapp.kubernetes.io/instance=demo

# If the PV was dynamically provisioned with a PVC, then there is no need to delete the PV explicitly as it will be deleted automatically with the PVC.

# However, if the PV was manually created, then it must be explicitly deleted.

# To delete a PV, you can use the kubectl delete command followed by the PV name:
# kubectl delete pv ${PV_NAME}
```

## Developing Guide

### Build image

```bash
cd GraphScope/k8s
make graphscope-store VERSION=latest
```

This would produce an image named `graphscope/graphscope-store:latest`.

### Persistence

Groot stores the graph data in `/var/lib/graphscope-store` directory in the Store Pod and the metadata in `/etc/groot/my.meta` directory in the Coordinator Pod.

### Interactive SDK

#### Python SDK
We have demonstrated the Python SDK in the user guide example. For the full documentation for interactive python sdk reference, please refer to [Python SDK Reference](../flex/interactive/development/python/python_sdk_ref.md).

### Troubleshooting
#### Viewing logs

You can view the logs of each Pod using the command 
`kubectl logs ${POD_NAME}`.

 It is common to check the logs of Frontend and Store roles. When debugging, it is often necessary to check the logs of Coordinator as well. The logs of Frontend include the logs of the Compiler that generates the logical query plan, while the logs of Store include the logs of the query engine execution. For example,

```bash
kubectl logs demo-graphscope-store-frontend-0
kubectl logs demo-graphscope-store-store-0
```
#### Configuring logs

Groot uses `logback` as the logging library for the Java part, and `log4rs` as the logging library for the Rust part.

Both of these logging libraries support automatic periodic reloading of configuration, which means that the logging configuration file can be changed and will take effect after a short time (up to 30 seconds).

The location of the logging configuration file in the container is:

- configuration file of `logback` is in `/usr/local/groot/conf/logback.xml`
- configuration file of `log4rs` is in `/usr/local/groot/conf/log4rs.yml` 

### Secondary Instance

Groot support open secondary instance along with primary instances. It leverages the [Secondary Instance](https://github.com/facebook/rocksdb/wiki/Read-only-and-Secondary-instances) of RocksDB
to provide the ability to serve the querying requests as well as catching up the schema and data updates.

To use it, just set the `secondary.enabled=true` in the helm charts.
Also remember the data path, ZK connect string as well as Kafka endpoint and topic should be as same as the primary instance.
And use a different `zk.base.path` for each secondary instance to avoid conflict with each other when doing node discovery.

`storeGcIntervalMs` controls how often should the secondary perform a `try_catch_up_with_primary` call, default to `5000` which is 5 seconds.

### Traces

use `--set otel.enabled=true` to enable trace export.

### Write High-availability

use `--set write.ha.enabled=True` in multi-pod deployment mode to open a backup store pod.
