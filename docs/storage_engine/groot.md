
# Groot: Persistent Graph Store

## Overview
In addition to Vineyard, the in-memory columnar graph store supported in GraphScope, we also have a disk-based, row-oriented, multi-versioned, persistent graph store. While Vineyard focuses on great support for in-memory whole graph analytics workloads, the persistent graph store is geared towards better supporting continuous graph data management services that frequently update the graph and answer traversal queries.

The store is a distributed graph store built on top of the popular RocksDB key-value store. It adopts a row-oriented design to support frequent small updates to the graph. Each row is tagged with a snapshot ID as its version. A query reads the most recent version of rows relative to the snapshot ID when it starts and is hence not blocked by writes. For writes, we take a compromise between consistency and higher throughput. In our design, writes in the same session can be grouped and executed atomically as a unit, and the persistent store assigns a snapshot ID (which is a low-resolution timestamp of the current time) to each group and executes groups of writes by the order of their snapshot IDs and by a deterministic (though arbitrary) order for groups of writes that occur in the same snapshot ID. It provides high write throughput while still maintaining some degree of order and isolation, although it provides less consistency than strict snapshot isolation common in databases. We hope our design choice provides an interesting trade-off for practical usage.

## Known Limitation
Initially, the new persistent store is provided as a separate option from Vineyard, and it can accept Gremlin queries for data access. Going foward we hope to evolve them into an integrated hybrid graph store suitable for all kinds of workloads.

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
| frontend.replicaCount | Number of Frontend | 1 |
| ingestor.replicaCount | Number of Ingestor Pod | 2 |
| frontend.service.type | Kubernetes Service type of frontend | NodePort |
| dataset.modern | Load [modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) dataset at the start | false |


If Groot is launched with the default configuration, then two Store Pods, one Frontend Pod, one Ingestor Pod, and one Coordinator Pod will be started. The number of Coordinator nodes is fixed to 1.

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

### Connecting to Groot
Upon installing Groot, an empty graph is created by default. We can execute connections, define graph models, load data, and perform queries using the [Gremlin Query Language](https://tinkerpop.apache.org/gremlin.html).

#### Connection
In the previous step, upon executing the command to obtain connection information as printed by Helm, the said information is set to environment variables. The following statement can be used to obtain and connect to Groot:

```python
import os
import graphscope
node_ip = os.environ["NODE_IP"]
grpc_port = os.environ["GRPC_PORT"]
gremlin_port = os.environ["GREMLIN_PORT"]
grpc_endpoint = f"{node_ip}:{grpc_port}"
gremlin_endpoint = f"{node_ip}:{gremlin_port}"

conn = graphscope.conn(grpc_endpoint, gremlin_endpoint)
```

In case a username and password were configured during the installation process, they will need to be provided when establishing a connection.


```python
conn = graphscope.conn(grpc_endpoint, gremlin_endpoint, username="admin", password="123456")
```

#### Building and Modifying Graph Models

The graph object can be obtained through the `conn` object.


```python
graph = conn.g()
# Create schema
schema = graph.schema()
```

#### Using Built-in Datasets

If `dataset.modern=true` is set during installation, Groot will load a simple example dataset for quick start.

````{note}
Not supported at this moment
````
#### Customizing Models and Datasets

Users can also customize models and load their own datasets.

Common statements used to define graph models are as follows:

```python
schema.add_vertex_label('v_label_name').add_primary_key('pk_name', 'type').property('prop_name_1', 'type').property('prop_name_2', 'type')
schema.add_edge_label('e_label_name').source('src_label').destination('dst_label').property('prop_name_3', 'type')
schema.drop('label')
schema.drop('label', 'src_label', 'dst_label')
schema.update()
```

A graph model defines several labels, each with a label name and several properties (`.property()`).

Among them, point labels can define primary keys (`.add_primary_key()`), and edge labels need to define the source label (`.source()`) and destination label (`.destination()`). `.drop()` is used to delete a label. `.update()` submits a transaction to apply changes.

Here is an example of a simple model that defines the relationship between people who know each other, with the labels `person` -> `knows` <- `person`. The model includes:

`person` label, which includes a primary key named `id` of type `long`, and a property named name of type `str`.
`knows` label, which includes a primary key named `date` of type `str`, with the source and destination labels both being person.
```python
schema.add_vertex_label("person").add_primary_key("id", "long").add_property(
        "name", "str"
    )
schema.add_edge_label("knows").source("person").destination("person").add_property(
        "date", "str"
    )
schema.update()
```

### Querying Data
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
```

#### Java

1. Create a directory structure as follows, where pom.xml and Main.java are files

```
gremlin
├── pom.xml
└── src
    ├── main
        ├── java
            └── org
                └── example
                    └── Main.java
```

2. Configure `pom.xml` as follows:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>gremlin</artifactId>
    <version>1.0-SNAPSHOT</version>
    
    <packaging>jar</packaging>
    <name>GremlinExample</name>
    <url>https://maven.apache.org</url>
    <dependencies>
        <dependency>
            <groupId>org.apache.tinkerpop</groupId>
            <artifactId>gremlin-driver</artifactId>
            <version>3.6.1</version>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.10.1</version>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.1.0</version>
                <configuration>
                    <executable>java</executable>
                    <arguments>
                        <argument>-classpath</argument>
                        <classpath/>
                        <argument>org.example.Main</argument>
                    </arguments>
                    <mainClass>org.example.Main</mainClass>
                    <complianceLevel>1.11</complianceLevel>
                    <killAfter>-1</killAfter>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

3. Configure `Main.java` as follows:

```java
package org.example;

import org.apache.tinkerpop.gremlin.driver.*;

public class Main {

    public static void main(String[] args) {
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint("127.0.0.1");
        builder.port(8182);
        builder.credentials("username", "password");
        Cluster cluster = builder.create();

        Client client = cluster.connect();
        ResultSet results = client.submit("g.V().limit(3).valueMap()");
        for (Result result : results) {
            System.out.println(result.getObject());
        }
        client.close();
        cluster.close();
    }
}
```

4. Execute the program

```bash
mvn compile exec:exec
```

#### Node.js

1. Install package `gremlin` for javascript

```bash
npm install gremlin
```

2. Execute these codes

```javascript
const gremlin = require('gremlin');
const DriverRemoteConnection = gremlin.driver.DriverRemoteConnection;
const Graph = gremlin.structure.Graph;

graph_url = `ws://{gremlin_endpoint}/gremlin`
remoteConn = new DriverRemoteConnection(graph_url,{});

const graph = new Graph();
const g = graph.traversal().withRemote(remoteConn);

g.V().limit(2).count().next().
    then(data => {
        console.log(data);
        remoteConn.close();
    }).catch(error => {
        console.log('ERROR', error);
        remoteConn.close();
    });
```

Now since we have only defined the schema, and there is no data yet, the query result would be empty. So the next step is to load data.

## Data Import

There are two methods for importing data. One method is to batch import data from external storage (such as HDFS) using an offline import tool, and the other is to perform real-time writing using statements provided by the SDK.

Note: Offline import will **overwrite* the full data of the imported label.

### Offline Import
#### Prerequisite
- Hadoop Cluster
- Data import tool [data_load.tar.gz](https://github.com/alibaba/GraphScope/releases/download/v0.20.0/data_load.tar.gz)

Extract data_load.tar.gz, where `data_load/bin/load_tool.sh` is the tool that will be used below.

```bash
tar xzvf data_load.tar.gz
```

#### Data Format
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

The rest lines are the data records. Each line represents one record. Data fields are seperated by a custom seperator (“|” in the example above). In the vertex data file person.csv, id field and name field are the primary-key and the property of the vertex type person respectively. In the edge data file person_knows_person.csv, person_id field is the primary-key of the source vertex, person_id_1 field is the primary-key of the destination vertex, date is the property of the edge type knows.

All the data fields will be parsed according to the data-type defined in the graph schema. If the input data field cannot be parsed correctly, data building process would be failed with corresponding errors.

#### Loading Process
The loading process contains three steps:

1. A partitioned graph is built from the source files and stored in the same HDFS using a MapReduce job

2. The graph partitions are loaded into the store servers (in parallel)

3. Commit to the online service so that data is ready for serving queries

##### Build: Building a partitioned graph
Build data by running the hadoop map-reduce job with following command.

```bash
./data_load/bin/load_tool.sh hadoop-build <path/to/config/file>
```
The config file should follow a format that is recognized by Java `java.util.Properties` class. 

Here is an example:

```properties
split.size=256
separator=\\|
input.path=/tmp/ldbc_sample
output.path=/tmp/data_output
graph.endpoint=<ip>:<grpc_port>
column.mapping.config={"person_0_0.csv":{"label":"person","propertiesColMap":{"0":"id","1":"name"}},"person_knows_person_0_0.csv":{"label":"knows","srcLabel":"person","dstLabel":"person","srcPkColMap":{"0":"id"},"dstPkColMap":{"1":"id"},"propertiesColMap":{"2":"date"}}}
skip.header=true
```

Details of the parameters are listed below:

| **Config key** | **Required** | **Default** | **Description** |
| --- | --- | --- | --- |
| split.size | false | 256 | Hadoop map-reduce input data split size in MB |
| separator | false | \\\\&#124; | Seperator used to parse each field in a line |
| input.path | true | N/A | Input HDFS dir |
| output.path | true | N/A | Output HDFS dir |
| graph.endpoint | true | N/A | GRPC endpoint of groot. You can find the  endpoint in previous section, or use `helm status demo`to get it. |
| column.mapping.config | true | N/A | Mapping info for each input file in JSON format. Each key in the first level should be a fileName that can be found in the input.path, and the corresponding value defines the mapping info. For a vertex type, the mapping info should includes 1) label of the vertex type, 2) propertiesColMap that describes the mapping from input field to graph property in the format of { columnIdx: “propertyName” }. For an edge type, the mapping info should includes 1) label of the edge type, 2) srcLabel of the source vertex type, 3) dstLabel of the destination vertex type, 4) srcPkColMap that describes the mapping from input field to graph property of the primary keys in the source vertex type, 5) dstPkColMap that describes the mapping from input field to graph property of the primary keys in the destination vertex type, 6) propertiesColMap that describes the mapping from input field to graph property of the edge type |
| skip.header | false | true | Whether to skip the first line of the input file |


After data building completed, you can find the output files in the `output.path` of HDFS. The output files includes a meta file named `META`, an empty file named `_SUCCESS`, and some data files that one for each partition named in the pattern of `part-r-xxxxx.sst`. The layout of the output directory should look like:

```properties
/tmp/data_output
  |- META
  |- _SUCCESS
  |- part-r-00000.sst
  |- part-r-00001.sst
  |- part-r-00002.sst
  ...
```

##### Ingest: Loading graph partitions

Now ingest the offline built data into the graph storage:<br />NOTE: You need to make sure that the HDFS endpoint that can be accessed from the processes of the graph store.
```bash
./data_load/bin/load_tool.sh -c ingest -d hdfs://1.2.3.4:9000/tmp/data_output
```
The offline built data can be ingested successfully only once, otherwise errors will occur.

##### Commit: Commit to store service

After data ingested into graph storage, you need to commit data loading. The data will not be able to read until committed successfully.
```bash
./data_load/bin/load_tool.sh -c commit -d hdfs://1.2.3.4:9000/tmp/data_output
```
Notice: The later committed data will overwrite the earlier committed data which have same vertex types or edge relations.

### Realtime Write

Groot graph have several methods for realtime write as follows:

```python
# Inserts one vertex
def insert_vertex(self, vertex: VertexRecordKey, properties: dict) -> int: pass

# Inserts a list of vertices
def insert_vertices(self, vertices: list) -> int: pass

# Update one vertex to new properties
def update_vertex_properties(self, vertex: VertexRecordKey, properties: dict) -> int: pass

# Delele one vertex
def delete_vertex(self, vertex_pk: VertexRecordKey) -> int: pass

# Delete a list of vertices
def delete_vertices(self, vertex_pks: list) -> int: pass

# Insert one edge
def insert_edge(self, edge: EdgeRecordKey, properties: dict) -> int: pass

# Insert a list of edges
def insert_edges(self, edges: list) -> int: pass

# Update one edge to new properties
def update_edge_properties(self, edge: EdgeRecordKey, properties: dict) -> int: pass

# Delete one edge
def delete_edge(self, edge: EdgeRecordKey) -> int: pass

# Delete a list of edges
def delete_edges(self, edge_pks: list) -> int: pass

# Make sure the snapshot is avaiable
def remote_flush(self, snapshot_id: int): pass
```

We use two utility class called `VertexRecordKey` and `EdgeRecordKey` to denote the key to uniquely identify a record.


```python
class VertexRecordKey:
    """Unique identifier of a vertex.
    The primary key may be a dict, the key is the property name,
    and the value is the data.
    """
    def __init__(self, label, primary_key):
        self.label: str = label
        self.primary_key: dict = primary_key

class EdgeRecordKey:
    """Unique identifier of an edge.
    The `eid` is required in Update and Delete, which is a
    system generated unsigned integer. User need to get that eid
    by other means such as gremlin query.
    """
    def __init__(self, label, src_vertex_key, dst_vertex_key, eid=None):
        self.label: str = label
        self.src_vertex_key: VertexRecordKey = src_vertex_key
        self.dst_vertex_key: VertexRecordKey = dst_vertex_key
        self.eid: int = eid  # Only required in update and delete operation
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

### Troubleshooting
#### Viewing logs

You can view the logs of each Pod using the command 
`kubectl logs ${POD_NAME}`.

 It is common to check the logs of Frontend and Store roles. When debugging, it is often necessary to check the logs of Coordinator and Ingestor as well. The logs of Frontend include the logs of the Compiler that generates the logical query plan, while the logs of Store include the logs of the query engine execution. For example,

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
