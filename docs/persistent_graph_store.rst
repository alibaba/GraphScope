Persistent Graph Store
======================

Overview
--------

In addition to Vineyard, the in-memory columnar graph store currently supported in GraphScope, we introduced a new disk-based row-oriented multi-versioned persistent graph store, from the version v0.5. While Vineyard focuses on great support for in-memory whole graph analytics workload, the new persistent graph store is geared towards better supporting for running continuous graph data management service that frequently updates the graph and answers traversal queries.

The store is a distributed graph store built on top of the popular RocksDB key value store. It adopts row-oriented design to support frequent small updates to the graph. Each row is tagged with a snapshot ID as its version. A query reads most recent version of rows relative to the snapshot ID when it starts and hence not blocked by writes. For writes we take a compromise between consistency and higher throughput. In our design writes in the same session can be grouped and executed atomically as a unit and the persistent store assigns a snapshot ID (which is a low-resolution timestamp of current time) to each group and executes groups of writes by the order of their snapshot IDs and by a deterministic (though arbitrary) order for groups of writes that occur in the same snapshot ID. It provides high write throughput while still with some degree of order and isolation though it provides less consistency than strict snapshot isolation common in database. We hope our design choice provides an interesting trade-off for practical usage.


Known Limitation
~~~~~~~~~~~~~~~~

*  Initially, the new persistent store is provided as a separate option from Vineyard, and it can accept Gremlin queries for data access. Going foward we hope to evolve them into an integrated hybrid graph store suitable for all kinds of workloads.
*  In the v0.5 release, data can only be loaded into the store in a bulk fashion. APIs for real-time updates (inserts and deletes of individual vertices and egdes) has been added in the v0.7 release.


Deploying Store Service
-----------------------

This chart bootstraps a `GraphScope Store <https://github.com/alibaba/GraphScope/tree/main/interactive_engine/groot/src/main/java/com/alibaba/graphscope/groot>`_ cluster deployment on a `Kubernetes <http://kubernetes.io>`_ cluster using the `Helm <https://helm.sh>`_ package manager.


Prerequisites
~~~~~~~~~~~~~~


*  Kubernetes 1.12+
*  Helm 3.1.0
*  PersistentVolume(PV) provisioner support in the underlying infrastructure


Installing the Chart
~~~~~~~~~~~~~~~~~~~~~

To install the chart with the release name `my-release`:

.. code:: bash

    $ helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/store/charts/
    $ helm install my-release graphscope/graphscope-store

These commands deploy GraphScope Store on the Kubernetes cluster in the default configuration. The :ref:`Parameters` section lists the parameters that can be configured during installation.

Tip: List all releases using `helm list`


Get GraphScope Store Endpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note that it may take a few minutes for pulling image at first time, you can watch the status by running `helm test` many times.


.. code:: bash

    # After installation, you can check service availability by:
    $ helm test my-release
    
    # Default, with kubernetes `NodePort` service type, you can get service endpoint by:
    $ export NODE_IP=$(kubectl get nodes --namespace default -o jsonpath="{.items[0].status.addresses[0].address}")
    $ export GRPC_PORT=$(kubectl get --namespace default -o jsonpath="{.spec.ports[0].nodePort}" services my-release-graphscope-store-frontend)
    $ export GREMLIN_PORT=$(kubectl get --namespace default -o jsonpath="{.spec.ports[1].nodePort}" services my-release-graphscope-store-frontend)
    $ echo "GRPC endpoint is: ${NODE_IP}:${GRPC_PORT}"
    $ echo "GREMLIN endpoint is: ${NODE_IP}:${GREMLIN_PORT}"


Uninstalling the Chart
~~~~~~~~~~~~~~~~~~~~~~~~

To uninstall/delete the `my-release` deployment:

.. code:: bash

    $ helm delete my-release


The command removes all the Kubernetes components associated with the chart and deletes the release.

Note: The PersistentVolume remains even after the release was uninstalled. To delete the PV manually:

.. code:: bash

    $ kubectl delete pvc -l app.kubernetes.io/instance=my-release


Parameters
~~~~~~~~~~

Here we give a list of most frequently used parameters.

Common parameters
"""""""""""""""""

+-------------------+--------------------------------------------+-----------------------------------+
|      Name         |    Description                             |            Value                  |
+===================+============================================+===================================+
| image.registry    | Docker image registry                      | registry.cn-hongkong.aliyuncs.com |
+-------------------+--------------------------------------------+-----------------------------------+
| image.repository  | Docker image repository                    | graphscope/graphscope-store       |
+-------------------+--------------------------------------------+-----------------------------------+
| image.tag         | Docker image tag                           |            ""                     |
+-------------------+--------------------------------------------+-----------------------------------+
| image.pullPolicy  | Docker image pull policy                   | IfNotPresent                      |
+-------------------+--------------------------------------------+-----------------------------------+
| image.pullSecrets | Docker-registry secrets                    | []                                |
+-------------------+--------------------------------------------+-----------------------------------+
| clusterDomain     | Default Kubernetes cluster domain          | cluster.local                     |
+-------------------+--------------------------------------------+-----------------------------------+
| commonLabels      | Labels to add to all deployed objects      | {}                                |
+-------------------+--------------------------------------------+-----------------------------------+
| commonAnnotations | Annotations to add to all deployed objects | {}                                |
+-------------------+--------------------------------------------+-----------------------------------+
| executor          | Executor type, "maxgraph" or "gaia"        | maxgraph                          |
+-------------------+--------------------------------------------+-----------------------------------+
| javaOpts          | Java options                               | ""                                |
+-------------------+--------------------------------------------+-----------------------------------+



Statefulset parameters
""""""""""""""""""""""

+-----------------------------+--------------------------------------+-----------------------------------+
| Name                        | Description                          | Value                             |
+=============================+======================================+===================================+
| image.registry              | Docker image registry                | registry.cn-hongkong.aliyuncs.com |
+-----------------------------+--------------------------------------+-----------------------------------+
| store.replicaCount          | Number of nodes                      |  2                                |
+-----------------------------+--------------------------------------+-----------------------------------+
| store.updateStrategy        | Update strategy for the store        |  RollingUpdate                    |
+-----------------------------+--------------------------------------+-----------------------------------+
| frontend.replicaCount       | Number of nodes                      |  1                                |
+-----------------------------+--------------------------------------+-----------------------------------+
| frontend.updateStrategy     | Update strategy for the frontend     |  RollingUpdate                    |
+-----------------------------+--------------------------------------+-----------------------------------+
| frontend.service.type       | Kubernetes service type              |  NodePort                         |
+-----------------------------+--------------------------------------+-----------------------------------+
| ingestor.replicaCount       | Number of nodes                      |  1                                |
+-----------------------------+--------------------------------------+-----------------------------------+
| ingestor.updateStrategy     | Update strategy for the ingestor     |  RollingUpdate                    |
+-----------------------------+--------------------------------------+-----------------------------------+
| coordinator.replicaCount    | Number of nodes                      |  1                                |
+-----------------------------+--------------------------------------+-----------------------------------+
| coordinator.updateStrategy  | Update strategy for the coordinator  | RollingUpdate                     |
+-----------------------------+--------------------------------------+-----------------------------------+

Kafka chart parameters
""""""""""""""""""""""

+------------------------+---------------------------------------------------+-------+
| Name                   | Description                                       | Value |
+========================+===================================================+=======+
| kafka.enabled          | Switch to enable or disable the Kafka helm chart  | true  |
+------------------------+---------------------------------------------------+-------+
| kafka.replicaCount     | Number of Kafka nodes                             | 1     |
+------------------------+---------------------------------------------------+-------+
| externalKafka.servers  | Server or list of external Kafka servers to use   | []    |
+------------------------+---------------------------------------------------+-------+



Configuration
~~~~~~~~~~~~~

See `Customizing the Chart Before Installing <https://helm.sh/docs/intro/using_helm/#customizing-the-chart-before-installing>`_. To see all configurable options with detailed comments, visit the chart's `values.yaml <https://github.com/alibaba/GraphScope/blob/main/charts/graphscope-store/values.yaml>`_, or run these configuration commands:

.. code:: bash

    $ helm show values graphscope/graphscope-store


Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

.. code:: bash

    $ helm install my-release \
      --set image.tag=latest graphscope/graphscope-store


Add multiple extra config to the component which is defined in the configmap by `--set extraConfig=k1=v1:k2=v2`. Note we use `:` to seperate config items. For example,

.. code:: bash

    $ helm install my-release \
      --set extraConfig=k1=v1:k2=v2 graphscope/graphscope-store


Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart. For example,

.. code:: bash

    $ helm install my-release -f values.yaml graphscope/graphscope-store


Defining Graph Schema
----------------------

After we deployed a cluster, there is an empty graph inside, we can then use the Python API to get the graph, and define schema over the graph, then we can load data according to that schema.

In the previous step we have the IP of the cluster, and the GRPC port and GREMLIN port, we can use these to setup a connection like this

.. code:: python

    import graphscope
    node_ip = os.environ["NODE_IP"]
    grpc_port = os.environ["GRPC_PORT"]
    gremlin_port = os.environ["GREMLIN_PORT"]
    grpc_endpoint = f"{node_ip}:{grpc_port}"
    gremlin_endpoint = f"{node_ip}:{gremlin_port}"
    
    conn = graphscope.conn(grpc_endpoint, gremlin_endpoint)


Then we get the graph and graph schema

.. code:: python

    graph = gs_conn.g()
    # Create schema
    schema = graph.schema()


The schema have defined several method


.. code:: python

    schema.add_vertex_label('v_label_name').partition_by('primary_key_name', 'property_type').property('property_name_1', 'property_type').property('property_name_2', 'property_type')
    schema.add_edge_label('e_label_name').from('source_label').to('destination_label').property('property_name_3', 'property_type')
    schema.update()
    schema.drop('label')
    schema.drop('label', 'src_label', 'dst_label')


Here the `label_name`, `primary_key_name`, `property_type` is specified by user, the `property_type` can be one of `int`, `float`, `str`, and one label can have multiple `property` statement.

For vertices, the `partitioned_by` is to specify the primary key of the label, also be called ID.

For edges, the `from` and `to` will specify the source label and destination label of the edge kind, respectively.

The `update()` method will issue a transction to the store.

The `drop()` method can drop a label from the store, Note for edge label you must drop all relations first by using the `src_label` and `dst_label`, then call the `drop(label)` to final drop the entire label.

Here we give an example to define a simple `person -> knows <- person` schema.

.. code:: python

    schema.add_vertex_label("person").add_primary_key("id", "long").add_property(
            "name", "str"
        )
    schema.add_edge_label("knows").source("person").destination("person").add_property(
            "date", "str"
        )
    schema.update()


Running Gremlin Queries
-----------------------

As we have the gremlin endpoint avaiable, we can do gremlin queries over the graph.
It's simple just use one line to get the traversal source,

.. code:: python

    g = gs_conn.gremlin()
    print(g.V().count().toList())

The graph is empty for now, so the count should be 0. Let's write some data.


Realtime Write
--------------

Once the graph schema is well defined, we can write vertex/edge records straightly from python client.

We use two utility class called `VertexRecordKey` and `EdgeRecordKey` to denote the key to uniquely identify a record.

.. code:: python

    class VertexRecordKey:
        """Unique identifier of a vertex.
        The primary key may be a dict, the key is the property name,
        and the value is the data.
        """
        def __init__(self, label, primary_key):
            self.label: str = label
            self.primary_key: dict = primary_key
    
    
    class EdgeRecordKey:
        """Unique identifier of a edge.
        The `eid` is required in Update and Delete, which is a
        system generated unsigned integer. User need to get that eid
        by other means such as gremlin query.
        """
        def __init__(self, label, src_vertex_key, dst_vertex_key, eid=None):
            self.label: str = label
            self.src_vertex_key: VertexRecordKey = src_vertex_key
            self.dst_vertex_key: VertexRecordKey = dst_vertex_key
            self.eid: int = eid  # Only required in Update and Delete.


And the graph have several methods as follows:

.. code:: python

    # Inserts one vertex
    # Returns snapshot id
    def insert_vertex(self, vertex: VertexRecordKey, properties: dict): pass
    
    # Inserts a list of vertices
    # Returns snapshot id
    def insert_vertices(self, vertices: list): pass
    
    # Update one vertex to new properties
    # Returns snapshot id
    def update_vertex_properties(self, vertex: VertexRecordKey, properties: dict): pass
    
    # Delele one vertex
    # Returns snapshot id
    def delete_vertex(self, vertex_pk: VertexRecordKey): pass
    
    # Delete a list of vertices
    # Returns snapshot id
    def delete_vertices(self, vertex_pks: list): pass
    
    # Insert one edge
    # Returns snapshot id
    def insert_edge(self, edge: EdgeRecordKey, properties: dict): pass
    
    # Insert a list of edges
    # Returns snapshot id
    def insert_edges(self, edges: list): pass
    
    # Update one edge to new properties
    # Returns snapshot id
    def update_edge_properties(self, edge: EdgeRecordKey, properties: dict): pass
    
    # Delete one edge
    # Returns snapshot id
    def delete_edge(self, edge: EdgeRecordKey): pass
    
    # Delete a list of edges
    # Returns snapshot id
    def delete_edges(self, edge_pks: list): pass
    
    # Make sure the snapshot is avaiable
    def remote_flush(self, snapshot_id: int): pass


We give some examples to illustrate the usage, note when deleting edges or updating edges, we need to use gremlin queries to retrieve the eid of edges, then we can uniquely refer to the edge.

.. code:: python

    # Construct several vertices
    v_src = [VertexRecordKey("person", {"id": 99999}), {"name": "ci_person_99999"}]
    v_dst = [VertexRecordKey("person", {"id": 199999}), {"name": "ci_person_199999"}]
    v_srcs = [
        [
            VertexRecordKey("person", {"id": 100000 + i}),
            {"name": f"ci_person_{100000 + i}"},
        ]
        for i in range(10)
    ]
    v_dsts = [
        [
            VertexRecordKey("person", {"id": 200000 + i}),
            {"name": f"ci_person_{200000 + i}"},
        ]
        for i in range(10)
    ]
    v_update = [v_src[0], {"name": "ci_person_99999_updated"}]
    graph.insert_vertex(*v_src)
    graph.insert_vertex(*v_dst)
    graph.insert_vertices(v_srcs)
    snapshot_id = graph.insert_vertices(v_dsts)

    assert gs_conn.remote_flush(snapshot_id)

    snapshot_id = graph.update_vertex_properties(*v_update)

    assert gs_conn.remote_flush(snapshot_id)

    assert (
        g.V().has("id", v_src[0].primary_key["id"]).values("name").toList()[0]
        == "ci_person_99999_updated"
    )

    edge = [EdgeRecordKey("knows", v_src[0], v_dst[0]), {"date": "ci_edge_2000"}]
    edges = [
        [EdgeRecordKey("knows", src[0], dst[0]), {"date": "ci_edge_3000"}]
        for src, dst in zip(v_srcs, v_dsts)
    ]
    edge_update = [edge[0], {"date": "ci_edge_4000"}]
    snapshot_id = graph.insert_edge(*edge)

    assert gs_conn.remote_flush(snapshot_id)

    edge[0].eid = (
        g.V()
        .has("id", edge[0].src_vertex_key.primary_key["id"])
        .outE()
        .toList()[0]
        .id
    )

    snapshot_id = graph.insert_edges(edges)

    assert gs_conn.remote_flush(snapshot_id)

    for e in edges:
        e[0].eid = (
            g.V()
            .has("id", e[0].src_vertex_key.primary_key["id"])
            .outE()
            .toList()[0]
            .id
        )
    snapshot_id = graph.update_edge_properties(*edge_update)

    assert gs_conn.remote_flush(snapshot_id)

    assert (
        g.V()
        .has("id", edge[0].src_vertex_key.primary_key["id"])
        .outE()
        .values("date")
        .toList()[0]
        == "ci_edge_4000"
    )

    graph.delete_edge(edge[0])
    graph.delete_edges([e[0] for e in edges])

    graph.delete_vertex(v_src[0])
    graph.delete_vertex(v_dst[0])
    graph.delete_vertices([key[0] for key in v_srcs])
    snapshot_id = graph.delete_vertices([key[0] for key in v_dsts])

    assert gs_conn.remote_flush(snapshot_id)


Bulk Loading
------------

Apart from real-time write, we provide a data-loading utility for bulk-loading data from external storage (e.g., HDFS) into the store service. Currently the tool supports a specific format of the raw data as described in "Data Format", and the originial data must be located in an HDFS. To load the data files into GraphScope storage, users can run the data-loading tool from a terminal on a Client machine, and we assume that Client has access to a Hadoop cluster, which can run MapReduce jobs, have read/write access to the HDFS, and connect to a running GraphScope storage service.


Prequisities
~~~~~~~~~~~~~


*  Java compilation environment (Maven 3.5+ / JDK1.8), if you need to build the tools from source code
*  Hadoop cluster (version 2.x) that can run map-reduce jobs and has HDFS supported
*  Running GIE with persistent storage service (graph schema should be properly defined)


Get Binary
~~~~~~~~~~

You can download the data-loading utility from here: `data_load.tar.gz <https://github.com/alibaba/GraphScope/releases/latest/download/graphscope_store_data_load.tar.gz>`_. Decompress it, and you can find the executable here: `./data_load/bin/load_tool.sh`.


Data Format
~~~~~~~~~~~

The data loading tools assume the original data files are in the HDFS.

Each file should represents either a vertex type or a relationship of an edge type. Below are the sample data of a vertex type `person` and a relationShip `person-knows->person` of edge type `knows`:


*  person.csv:

.. code:: plain-text

    id|name
    1000|Alice
    1001|Bob
    ...


*  person_knows_person.csv

.. code:: plain-text

    person_id|person_id_1|date
    1000|1001|20210611151923
    ...


The first line of the data file is a header that describes the key of each field. The header is not required. If there is no header in the data file, you need to set `skip.header` to `true` in the data building process (For details, see params description in "Building a partitioned graph").

The rest lines are the data records. Each line represents one record. Data fields are seperated by a custom seperator ("|" in the example above). In the vertex data file `person.csv`, `id` field and `name` field are the primary-key and the property of the vertex type `person` respectively. In the edge data file `person_knows_person.csv`, `person_id` field is the primary-key of the source vertex, `person_id_1` field is the primary-key of the destination vertex, `date` is the property of the edge type `knows`.

All the data fields will be parsed according to the data-type defined in the graph schema. If the input data field cannot be parsed correctly, data building process would be failed with corresponding errors.


Loading Process
~~~~~~~~~~~~~~~~

The loading process contains three steps:

Step 1: A partitioned graph is built from the source files and stored in the same HDFS using a MapReduce job

Step 2: The graph partitions are loaded into the store servers (in parallel)

Step 3: Commit to the online service so that data is ready for serving queries


1. Building a partitioned graph
"""""""""""""""""""""""""""""""

Build data by running the hadoop map-reduce job with following command.

NOTE: You should make sure `hadoop` can be found in the env `$PATH`.

.. code:: bash

    $ ./data_load/bin/load_tool.sh hadoop-build <path/to/config/file>

The config file should follow a format that is recognized by Java `java.util.Properties` class. Here is an example:

.. code:: plain-text

    split.size=256
    separator=\\|
    input.path=/tmp/ldbc_sample
    output.path=/tmp/data_output
    graph.endpoint=1.2.3.4:55555
    column.mapping.config={"person_0_0.csv":{"label":"person","propertiesColMap":{"0":"id","1":"name"}},"person_knows_person_0_0.csv":{"label":"knows","srcLabel":"person","dstLabel":"person","srcPkColMap":{"0":"id"},"dstPkColMap":{"1":"id"},"propertiesColMap":{"2":"date"}}}
    skip.header=true

Details of the parameters are listed below:

+-----------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Config key            | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
+=======================+==========+=========+==================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================+
| split.size            | false    | 256     | Hadoop map-reduce input data split size in MB                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
+-----------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| separator             | false    | \\|     | Seperator used to parse each field in a line                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
+-----------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| input.path            | true     | -       | Input HDFS dir                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
+-----------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| output.path           | true     | -       | Output HDFS dir                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
+-----------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| graph.endpoint        | true     | -       | RPC endpoint of the graph storage service. You can get the RPC endpoint following this document: GraphScope Store Service                                                                                                                                                                                                                                                                                                                                                                                                        |
+-----------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| column.mapping.config | true     | -       | Mapping info for each input file in JSON format. Each key in the first level should be a fileName that can be found in the input.path, and the corresponding value defines the mapping info.                                                                                                                                                                                                                                                                                                                                     |
|                       |          |         | For a vertex type, the mapping info should includes 1) label of the vertex type, 2) propertiesColMap that describes the mapping from input field to graph property in the format of { columnIdx: "propertyName" }.                                                                                                                                                                                                                                                                                                               |
|                       |          |         | For an edge type, the mapping info should includes 1) label of the edge type, 2) srcLabel of the source vertex type, 3) dstLabel of the destination vertex type, 4) srcPkColMap that describes the mapping from input field to graph property of the primary keys in the source vertex type, 5) dstPkColMap that describes the mapping from input field to graph property of the primary keys in the destination vertex type, 6) propertiesColMap that describes the mapping from input field to graph property of the edge type |
+-----------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| skip.header           | false    | true    | Whether to skip the first line of the input file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
+-----------------------+----------+---------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


After data building completed, you can find the output files in the `output.path` of HDFS. The output files includes a meta file named `META`, an empty file named `_SUCCESS`, and some data files that one for each partition named in the pattern of `part-r-xxxxx.sst`. The layout of the output directory should look like:

.. code:: plain-text
    /tmp/data_output
      |- META
      |- _SUCCESS
      |- part-r-00000.sst
      |- part-r-00001.sst
      |- part-r-00002.sst
      ...


2. Loading graph partitions
""""""""""""""""""""""""""""

Now ingest the offline built data into the graph storage:

NOTE: You need to make sure that the HDFS endpoint that can be accessed from the processes of the graph store.

.. code:: bash

    $ ./data_load/bin/load_tool.sh -c ingest -d hdfs://1.2.3.4:9000/tmp/data_output


The offline built data can be ingested successfully only once, otherwise errors will occur.


3. Commit to store service
""""""""""""""""""""""""""

After data ingested into graph storage, you need to commit data loading. The data will not be able to read until committed successfully.

.. code:: bash

    $ ./data_load/bin/load_tool.sh -c commit -d hdfs://1.2.3.4:9000/tmp/data_output

Notice: The later committed data will overwrite the earlier committed data which have same vertex types or edge relations.


