# Tutorial: Graph Operations with NetowrkX APIs
[NetworkX](https://networkx.org/) is a Python package for the manipulation and functions for graph data on a single machine. However, it lacks the capability of handling large-scale graphs on a distributed environment. Fortunately, GraphScope is compatible with NetworkX APIs, and thus a program written with NetworkX can directly run on GraphScope with only some small changes. In this tutorial, we will first introduce how to manipulate graph data with NetworkX APIs.

## Creating an Empty Graph
To create an empty graph with no nodes and no edges, we only need to simply new a `Graph` object.

```python
# Import the graphscope and graphscope networkx module.
import graphscope
import graphscope.nx as nx

G = nx.Graph()
```

## Adding Nodes
The nodes of graph `G` can be added in several ways. In `graphscope.nx`, nodes can be some hashable Python objects, including `int`, `str`, `float`, `tuple`, `bool` objects. To get started though, we’ll look at simple manipulations and start from an empty graph. You can add one node at a time,

```python
G.add_node(1)
```

or add nodes from any [iterable container](https://docs.python.org/3/glossary.html#term-iterable), such as a `list`:

```python
G.add_nodes_from([2, 3])
```

You can also add nodes along with node attributes if your container yields 2-tuples of the form `(node, node_attribute_dict)`:

Node attributes are discussed further below.

```python
G.add_nodes_from(
    [
        (4, {"color": "red"}),
        (5, {"color": "green"}),
    ]
)
```

Nodes from one graph can be incorporated into another:

```python
H = nx.path_graph(10)
G.add_nodes_from(H)
```

After that, `G` contains the nodes of `H` as nodes of `G`.

```python
list(G.nodes)

list(G.nodes.data())  # shows the node attributes
```

## Adding Edges
The edges of `G` can also be grown by adding one edge at a time,

```python
G.add_edge(1, 2)
e = (2, 3)
G.add_edge(*e)  # unpack edge tuple*

list(G.edges)
```

or by adding a list of edges,

```python
G.add_edges_from([(1, 2), (1, 3)])
list(G.edges)
```

or by adding any ebunch of edges. An ebunch is any iterable container of edge-tuples. An edge-tuple can be a 2-tuple of nodes or a 3-tuple with 2 nodes followed by an edge attribute dictionary, e.g., `(2, 3, {'weight': 3.1415})`. Edge attributes are discussed further below.

```python
G.add_edges_from([(2, 3, {"weight": 3.1415})])
list(G.edges.data())  # shows the edge arrtibutes
G.add_edges_from(H.edges)
list(G.edges)
```

or by adding new nodes and edges once with `.update(nodes, edges)`

```python
G.update(edges=[(10, 11), (11, 12)], nodes=[10, 11, 12])
list(G.nodes)
list(G.edges)
```

Note that there are no complaints when adding existing nodes or edges. For example, after removing all nodes and edges,

```python
G.clear()
```

we add new nodes/edges and `graphscope.nx` quietly ignores any that are already present.

```python
G.add_edges_from([(1, 2), (1, 3)])
G.add_node(1)
G.add_edge(1, 2)
G.add_node("spam")  # adds node "spam"
G.add_nodes_from("spam")  # adds 4 nodes: 's', 'p', 'a', 'm'
G.add_edge(3, "m")
```

At this stage the graph `G` consists of 8 nodes and 3 edges, as can be seen by:

```python
G.number_of_nodes()
G.number_of_edges()
```

## Examining Elements of a Graph
Now we can examine the nodes and edges. Four basic graph properties facilitate reporting: `G.nodes`, `G.edges`, `G.adj` and `G.degree`. These are set-like views of the nodes, edges, neighbors (adjacencies), and degrees of nodes in a graph. They offer a continually updated read-only view into the graph structure. They are also dict-like in that you can look up node and edge data attributes via the views and iterate with data attributes using methods `.items()` and `.data('span')`. If you want a specific container type instead of a view, you can specify one. Here we use lists, though sets, dicts, tuples and other containers may be better in other contexts.

```python
list(G.nodes)
list(G.edges)
list(G.adj[1])  # or list(G.neighbors(1))
G.degree[1]  # the number of edges incident to 1
```

One can specify to report the edges and degree from a subset of all nodes using an *nbunch*. An *nbunch* is any of: `None` (meaning all nodes), a node, or an iterable container of nodes that is not itself a node in the graph.

## Removing Elements from a Graph
One can remove nodes and edges from the graph in a similar fashion to adding nodes/edges. One can use methods `Graph.remove_node()`, `Graph.remove_nodes_from()`, `Graph.remove_edge()` and `Graph.remove_edges_from()`, e.g.,

```python
G.remove_node(2)
G.remove_nodes_from("spam")
list(G.nodes)
list(G.edges)

G.remove_edge(1, 3)
G.remove_edges_from([(1, 2), (2, 3)])
list(G.edges)
```

## Using Graph Constructors
Graph objects do not have to be built up incrementally - data specifying graph structure can be passed directly to the constructors of the various graph classes. When creating a graph structure by instantiating one of the graph classes you can specify data in several formats.

```python
G.add_edge(1, 2)
H = nx.DiGraph(G)  # create a DiGraph using the connections from G
list(H.edges())

edgelist = [(0, 1), (1, 2), (2, 3)]
H = nx.Graph(edgelist)
list(H.edges)
```

## Accessing Edges and Neighbors
In addition to the views `Graph.edges` and `Graph.adj`, one can access edges and neighbors of a node using subscript notation.

```python
G = nx.Graph([(1, 2, {"color": "yellow"})])
G[1]  # same as G.adj[1]
G[1][2]
G.edges[1, 2]
```

One can get/set the attributes of an edge using subscript notation if the edge already exists.

```python
G.add_edge(1, 3)
G[1][3]["color"] = "blue"
G.edges[1, 3]

G.edges[1, 2]["color"] = "red"
G.edges[1, 2]
```

Fast examination of all (node, adjacency) pairs is achieved using `G.adjacency()` or `G.adj.items()`. Note that for undirected graphs, adjacency iteration sees each edge twice.

```python
FG = nx.Graph()
FG.add_weighted_edges_from([(1, 2, 0.125), (1, 3, 0.75), (2, 4, 1.2), (3, 4, 0.375)])
for n, nbrs in FG.adj.items():
    for nbr, eattr in nbrs.items():
        wt = eattr["weight"]
        if wt < 0.5:
            print(f"({n}, {nbr}, {wt:.3})")
```

Access to all edges with edge properties is achieved with:

```python
for (u, v, wt) in FG.edges.data("weight"):
    if wt < 0.5:
        print(f"({u}, {v}, {wt:.3})")
```

## Adding Attributes to Graphs, Nodes and Edges
Attributes such as weights, labels, colors, can be attached to graphs, nodes, or edges.

Each graph, node, and edge can hold key/value attribute. By default these are empty, but attributes can be added or changed by using `add_edge()`, `add_node()` or direct manipulation of the attribute dictionaries named `G.graph`, `G.nodes`, and `G.edges` for a graph `G`.

### Graph Attributes
One can assign graph attributes when creating a new graph

```python
G = nx.Graph(day="Friday")
G.graph
```

and one can modify attributes later

```python
G.graph["day"] = "Monday"
G.graph
```

### Node Attributes

One can add node attributes using `add_node()`, `add_nodes_from()`, or `G.nodes`

```python
G.add_node(1, time="5pm")
G.add_nodes_from([3], time="2pm")
G.nodes[1]

G.nodes[1]["room"] = 714
G.nodes.data()
```

Note that adding a node to `G.nodes` does not add it to the graph, use` G.add_node()` to add new nodes. Similarly for edges.

### Edge Attributes

One can add/change edge attributes using `add_edge()`, `add_edges_from()`, or subscript notation.

```python
G.add_edge(1, 2, weight=4.7)
G.add_edges_from([(3, 4), (4, 5)], color="red")
G.add_edges_from([(1, 2, {"color": "blue"}), (2, 3, {"weight": 8})])
G[1][2]["weight"] = 4.7
G.edges[3, 4]["weight"] = 4.2

G.edges.data()
```

The special attribute `weight` should be numeric as it is used by algorithms requiring weighted edges.

## Induce deepcopy `subgraph` and `edge_subgraph`

`graphscope.nx` supports to induce a `deepcopy` subgraph by given node set or edge set.

```python
G = nx.path_graph(10)
# induce a subgraph by nodes
H = G.subgraph([0, 1, 2])
list(H.nodes)

list(H.edges)

# induce a edge subgraph by edges
K = G.edge_subgraph([(1, 2), (3, 4)])
list(K.nodes)
list(K.edges)
```

Note that different from `subgraph`/`edge_subgraph` APIs in NetworkX which return a view, `graphscope.nx` returns a deepcopy of `subgraph`/`edge_subgraph`.

## Making Copies
One can use `to_directed` to return a directed representaion of the graph.

```python
DG = G.to_directed()  # here would return a "deepcopy" directed representation of G.
list(DG.edges)

# or with
DGv = G.to_directed(as_view=True)  # return a view.
list(DGv.edges)

# or with
DG = nx.DiGraph(G)  # return a "deepcopy" of directed representation of G.
list(DG.edges)
```

or get a copy of the graph.

```python
H = G.copy()  # return a view of copy
list(H.edges)

# or with
H = G.copy(as_view=False)  # return a "deepcopy" copy
list(H.edges)

# or with
H = nx.Graph(G)  # return a "deepcopy" copy
list(H.edges)
```

Note that `graphscope.nx` does not support shallow copy of the graph.

## Directed Graphs

The `DiGraph` class provides additional methods and properties specific to directed edges, e.g., `DiGraph.out_edges`, `DiGraph.in_degree`, `DiGraph.predecessors()` and `DiGraph.successors()`. To allow algorithms to work with both directed and undirected classes easily, the directed versions of `neighbors()` is equivalent to `successors()`, while `degree` reports the sum of `in_degree` and `out_degree` even though that may feel inconsistent at times.

```python
DG = nx.DiGraph()
DG.add_weighted_edges_from([(1, 2, 0.5), (3, 1, 0.75)])
DG.out_degree(1, weight="weight")
DG.degree(1, weight="weight")
list(DG.successors(1))
list(DG.neighbors(1))
list(DG.predecessors(1))
```

Some algorithms work only for directed graphs and others are not well defined for directed graphs. Indeed the tendency to lump directed and undirected graphs together is dangerous. If you want to treat a directed graph as undirected for some measurement you should probably convert it using `Graph.to_undirected()`

```python
H = DG.to_undirected()  # return a "deepcopy" of undirected represetation of DG.
list(H.edges)

# or with
H = nx.Graph(DG)  # create an undirected graph H from a directed graph G
list(H.edges)
```

Directed graph also supports to reverse edge using `DiGraph.reverse()`.

```python
K = DG.reverse()  # retrun a "deepcopy" of reversed copy.
list(K.edges)

# or with
K = DG.reverse(copy=False)  # return a view of reversed copy.
list(K.edges)
```

## Analyzing Graphs
The structure of `G` can be analyzed using various graph-theoretic functions such as:

```python
G = nx.Graph()
G.add_edges_from([(1, 2), (1, 3)])
G.add_node(4)
sorted(d for n, d in G.degree())

nx.builtin.clustering(G)
```

In `graphscope.nx,` we support some builtin algorithms for analyzing graph, see [builtin algorithm](https://graphscope.io/docs/reference/networkx/builtin.html) for more details on graph algorithms supported.

## Create graph from GraphScope Graph Object
In addition, we can create a graph in the GraphScope way, which is created from GraphScope graph object.

```python
# we load a GraphScope graph with load_ldbc
from graphscope.dataset import load_ldbc

graph = load_ldbc(directed=False)

# create graph with the GraphScope graph object
G = nx.Graph(graph)
```

## Transform to GraphScope Graph Object
As `graphscope.nx` graph can create from GraphScope graph, the `graphscope.nx` graph can also transform to GraphScope graph, e.g,

```python
nodes = [(0, {"foo": 0}), (1, {"foo": 1}), (2, {"foo": 2})]
edges = [(0, 1, {"weight": 0}), (0, 2, {"weight": 1}), (1, 2, {"weight": 2})]
G = nx.Graph()
G.update(edges, nodes)

# transform to GraphScope graph
g = graphscope.g(G)
```

