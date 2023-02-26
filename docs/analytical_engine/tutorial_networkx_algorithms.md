# Tutorial: Graph Algorithms with NetowrkX APIs

In the [previous tutorial](https://graphscope.io/docs/latest/analytical_engine/tutorial_networkx_operations.html), we have introduced how to manipulate graph data with NetworkX APIs. In this tutorial, we will show how to use GraphScope to perform graph analysis like Networkx. 

## How does Networkx Perform Graph Analysis?

Usually, the graph analysis process of NetworkX starts with the construction of a graph.

In the following example, we create an empty graph first, and then expand the data through the graph manipulation interfaces of NetworkX.

```python
import networkx
# Initialize an empty graph
G = networkx.Graph()

# Add edges (1, 2)and（1 3） by `add_edges_from` interface
G.add_edges_from([(1, 2), (1, 3)])

# Add vertex "4" by `add_node` interface 
G.add_node(4)
```

Then we can query the graph information.

```python
# Query the number of vertices by `number_of_nodes` interface.
G.number_of_nodes()
# Similarly, query the number of edges by `number_of_edges` interface.
G.number_of_edges()
# Query the degree of each vertex by `degree` interface.
sorted(d for n, d in G.degree())
```

Finally, we can call the builtin algorithms of NetworkX to analysis the graph `G`.

```python
# Run 'connected components' algorithm
list(networkx.connected_components(G))

# Run 'clustering' algorithm
networkx.clustering(G)
```

## How to Perform Graph Analysis with NetworkX APIs from GraphScope
To use NetworkX APIs from GraphScope, we just need to replace `import networkx as nx` with `import graphscope.nx as nx`.

According to the [previous tutorial](https://graphscope.io/docs/latest/analytical_engine/tutorial_networkx_operations.html), we use create a graph `nx.Graph()` first.

```python
import graphscope
graphscope.set_option(show_log=True)
import graphscope.nx as nx

# Initialize an empty graph
G = nx.Graph()

# Add one vertex by `add_node` interface
G.add_node(1)

# Or add a batch of vertices from iterable list
G.add_nodes_from([2, 3])

# Also you can add attributes while adding vertices
G.add_nodes_from([(4, {"color": "red"}), (5, {"color": "green"})])

# Similarly, add one edge by `add_edge` interface
G.add_edge(1, 2)
e = (2, 3)
G.add_edge(*e)

# Or add a batch of edges from iterable list
G.add_edges_from([(1, 2), (1, 3)])

# Add attributes while adding edges
G.add_edges_from([(1, 2), (2, 3, {'weight': 3.1415})])
```

### Graph Analysis

The interface of graph analysis module in GraphScope is also compatible with NetworkX.

With the above created graph in place, we use `connected_components` to analyze the connected components of the graph, use `clustering` to get the clustering coefficient of each vertex, and `all_pairs_shortest_path` to compute the shortest path between any two vertices.

```python
# Run connected_components
list(nx.connected_components(G))

# Run clustering
nx.clustering(G)

# Run all_pairs_shortest_path
sp = dict(nx.all_pairs_shortest_path(G))
sp[3]
```

### Graph Display

Like NetworkX, you can draw a graph by `draw` interface, which relies on the drawing function of `Matplotlib`.

You should install `matplotlib` first,

```bash
pip3 install matplotlib
```

Then you can draw a graph with 

```python
nx.draw(G, with_labels=True, font_weight='bold')
```

### The Performance Speed-up of GraphScope over NetworkX

Let's see how much performance can be improved by GraphScope over NetworkX, compared with NetworkX, by running the `clustering` algorithm on [Twitter dataset](https://snap.stanford.edu/data/ego-Twitter.html).

Download dataset if it is not in environment:

```bash
wget https://raw.githubusercontent.com/GraphScope/gstest/master/twitter.e -P /tmp
```

Then load dataset both in GraphScope and NetwrokX.

```python
import os
import graphscope.nx as gs_nx
import networkx as nx

# loading graph in NetworkX
g1 = nx.read_edgelist(
     os.path.expandvars('/tmp/twitter.e'), nodetype=int, data=False, create_using=nx.Graph
)
type(g1)

# Loading graph in GraphScope
g2 = gs_nx.read_edgelist(
     os.path.expandvars('/tmp/twitter.e'), nodetype=int, data=False, create_using=gs_nx.Graph
)
type(g2)
```

Run algorithm and display time both in GraphScope and NetworkX.

```python
%%time
# GraphScope
ret_gs = gs_nx.clustering(g2)
%%time
# NetworkX
ret_nx = nx.clustering(g1)
# Result comparison
ret_gs == ret_nx
```