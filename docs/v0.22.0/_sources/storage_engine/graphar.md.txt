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

See [Gar Information Files](https://alibaba.github.io/GraphAr/user-guide/getting-started.html#gar-information-files) and [Gar Data Files](https://alibaba.github.io/GraphAr/user-guide/getting-started.html#gar-data-files) for an example.


More details about the GraphAr file format can be found in the [GraphAr File Format](https://alibaba.github.io/GraphAr/user-guide/file-format.html).

## GraphAr in GraphScope

GraphScope can read, store GraphAr formatted graph data.

### Loading GraphAr Data into GraphScope

To load GraphAr formatted data into GraphScope:

1. Define the graph meta files with YAML. The meta files describe the properties of vertices and edges, and where the data files are stored.

2. Load the graph data using the GraphScope client (Python) with graphscope.load_from_gar(graph_yaml_path) function. Here's an example:

```python
import graphscope

graph_yaml_path = "file:///path-yaml/demo.graph.yml"
g = graphscope.load_from_gar(graph_yaml_path)
g.schema()
```

### Archiving the Graph Data in GraphAr

To archive the graph data in GraphAr format:

1. Define the graph meta files with YAML.

2. Call the g.archive(graph_yaml_path) function, where g is the GraphScope graph object, and graph_yaml_path is the graph info file path. Here's an example:

```python
import graphscope

graph_yaml_path = "file:///path-yaml/demo.graph.yml"
g.archive(graph_yaml_path)
```