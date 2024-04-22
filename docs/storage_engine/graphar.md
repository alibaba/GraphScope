# GraphAr: Standard Graph Data File Format

GraphAr is a file format for storing graph data that provides a standard format for different applications and systems to communicate with each other efficiently. With GraphAr, graph data can be imported/exported, stored persistently, and used as a direct data source for graph processing applications. GraphAr is highly compatible with multiple data processing frameworks such as Apache Spark, Apache Hive, Neo4j.


## The GraphAr File Format

### Features

GraphAr supports the property graph data model and different representations for the graph structure such as COO, CSR and CSC. It is also compatible with existing widely-used file types including CSV, ORC and Parquet. Apache Spark can be utilized to generate, load and transform the GraphAr files efficiently. GraphAr is also flexible enough to modify the topology structure or the properties of the graph, or to construct a new graph with a set of selected vertices/edges.

### File Format

GraphAr consists of two types of files:
- YAML files to describe the meta information;
- data files to store the vertex and edge data.

#### Information files

GraphAr uses two kinds of files to store a graph: a group of Yaml files to describe meta information; and data files to store actual data for vertices and edges.
A graph information file which named "<name>.graph.yml" describes the meta information for a graph whose name is <name>. The content of this file includes:

- the graph name;
- the root directory path of the data files;
- the vertex information and edge information files included;
- the version of GraphAr.

A vertex information file which named "<label>.vertex.yml" defines a single group of vertices with the same vertex label <label>, and all vertices in this group have the same schema.

An edge information file which named "<source label>_<edge label>_<destination label>.edge.yml" defines a single group of edges with specific label for source vertex, destination vertex and the edge. It describes the meta information for these edges.

Please note that GraphAr supports the storage of multiple types of adjLists for a given group of edges, e.g., a group of edges could be accessed in both CSR and CSC way when two copies (one is *ordered_by_source* and the other is *ordered_by_dest*) of the relevant data are present in GraphAr.

#### Data files

As previously mentioned, each logical vertex/edge table is divided into multiple physical tables stored in one of the following file formats:

- [Apache ORC](https://orc.apache.org/)
- [Apache Parquet](https://parquet.apache.org/)
- CSV

See [Information Files](https://graphar.apache.org/docs/specification/format#information-files) and [Data Files](https://graphar.apache.org/docs/specification/format#data-files) for an example.


More details about the GraphAr file format can be found in the [GraphAr File Format](https://graphar.apache.org/docs/specification/format).

#### Data Types

Property Data Types
-------------------
GraphAr support a set of built-in property data types that are common in real use cases and supported by most file types (CSV, ORC, Parquet), includes:

```
- Boolean 
- Int32: Integer with 32 bits
- Int64: Integer with 64 bits
- Float: 32-bit floating point values
- Double: 64-bit floating point values
- String: Textual data
- Date: days since the Unix epoch
- Timestamp: milliseconds since the Unix epoch
- List: A list of values of the same type
```

## GraphAr in GraphScope

GraphScope provides a set of APIs to load and archive graph data in GraphAr format. The GraphScope client (Python) can be used to load and archive graph data in GraphAr format through the `save_to` and `load_from` functions. 

### Saving Graph Data in GraphAr

You can save a graph in GraphAr format using the `save_to` function.

`save_to` supports the following GraphAr related parameters:

- **graphar_graph_name**: The name of the graph, default is "graph".
- **graphar_file_type**: The file type of the graph data, including "csv", "orc", "parquet". default is "parquet".
- **graphar_vertex_chunk_size**: The chunk size of the vertex data in graphar format, default is 2^18.
- **graphar_edge_chunk_size**: The chunk size of the edge data in graphar format, default is 2^22.
- **graphar_store_in_local**: Whether to make each worker store the part of the graph data in local file system, default is False.
- **selector**: The selector to select the subgraph to save, if not specified, the whole graph will be saved.

Here's an example:

```python
import graphscope
from graphscoped.dataset import load_ldbc

# initialize a session
sess = graphscope.session(cluster_type="hosts")
# load ldbc graph
graph = load_ldbc(sess)

# save the ldbc graph to GraphAr format
r = g.save_to(
    "/tmp/ldbc_graphar/",
    format="graphar",
    graphar_graph_name="ldbc",  # the name of the graph
    graphar_file_type="parquet",  # the file type of the graph data
    graphar_vertex_chunk_size=1024,  # the chunk size of the vertex data
    graphar_edge_chunk_size=4096,  # the chunk size of the edge data
)
# the result is a dictionary that contains the format and the URI path of the saved graph
print(r)
{ "format": "graphar", "uri": "graphar+file:///tmp/ldbc_graphar/ldbc.graph.yaml"}
```

You can also save a subgraph in GraphAr format using the `save_to` function with the `selector` parameter. Here's an example:

```python
import graphscope
from graphscoped.dataset import load_ldbc

# initialize a session
sess = graphscope.session(cluster_type="hosts")
# load ldbc graph
graph = load_ldbc(sess)

# define the selector
# we only want to save the "person" and "comment" vertices and the "knows" and "replyOf" edges
# with the specified properties
selector = {
    "vertices": {
        "person": ["id", "firstName", "lastName"],
        "comment": None,  # None means all properties
    },
    "edges": {
        "knows": ["creationDate"],
        "likes": ["creationDate"],
    },
}

# save the subgraph to GraphAr format
r = g.save_to(
    "/tmp/ldbc_subgraph_graphar/",
    format="graphar",
    selector=selector,
    graphar_graph_name="ldbc_subgraph",  # the name of the graph
    graphar_file_type="parquet",  # the file type of the graph data
    graphar_vertex_chunk_size=1024,  # the chunk size of the vertex data
    graphar_edge_chunk_size=4096,  # the chunk size of the edge data
)
# the result is a dictionary that contains the format and the URI path of the saved graph
print(r)
{ "format": "graphar", "uri": "graphar+file:///tmp/ldbc_graphar/ldbc_subgraph.graph.yaml"}
```

### Loading GraphAr Data into GraphScope

You can load a graph from GraphAr format data using the `load_from` function.

`load_from` supports the following GraphAr related parameters:
- **graphar_store_in_local**: Whether the graph data is stored in the local file system of each worker, default is False.
- **selector**: The selector to select the subgraph to load, if not specified, the whole graph will be loaded.

Here's an example:


```python
import graphscope
from graphscope import pagerank
from graphscope.framework.graph import Graph 

# initialize a session
sess = graphscope.session(cluster_type="hosts")

# assume the graph data is saved in the "/tmp/ldbc_graphar/" directory and it's graph information file is "ldbc.graph.yaml", that the URI is "graphar+file:///tmp/ldbc_graphar/ldbc.graph.yaml"
uri = "graphar+file:///tmp/ldbc_graphar/ldbc.graph.yaml"

# load the graph from GraphAr format
g = Graph.load_from(uri, sess)
print(g.schema)

# do some graph processing
pg = g.project(vertices={"person": ["id"]}, edges={"knows": []})
ctx = pagerank(pg, max_round=10)
df = ctx.to_dataframe(selector={"id": "v.data", "r": "r"})
print(df)
```

You can also load a subgraph from the whole ldbc dataset with GraphAr format data using the `load_from` function with the `selector` parameter. Here's an example:

```python
import graphscope
from graphscope.framework.graph import Graph

# initialize a session
sess = graphscope.session(cluster_type="hosts")

# assume the ldbc data is saved in the "/tmp/ldbc__graphar/" directory and it's graph information file is "ldbc.graph.yaml", that the URI is "graphar+file:///tmp/ldbc_graphar/ldbc.graph.yaml"
uri = "graphar+file:///tmp/ldbc_graphar/ldbc.graph.yaml"

# define the selector, you want to only load the "person" and "comment" vertices and the "knows" and "replyOf" edges
selector = {
    "vertices": {
        "person": None,
        "comment": None,  # None means all properties
    },
    "edges": {
        "knows": None,
        "likes": None,
    },
}
g = Graph.load_from(uri, sess, selector=selector)
print(g.schema)

# do some graph processing
pg = g.project(vertices={"person": ["id"]}, edges={"knows": []})
ctx = pagerank(pg, max_round=10)
df = ctx.to_dataframe(selector={"id": "v.data", "r": "r"})
print(df)
```

More examples about how to use GraphAr in GraphScope can be found in the [test_graphar](https://github.com/alibaba/GraphScope/blob/main/python/graphscope/tests/unittest/test_graphar.py).
