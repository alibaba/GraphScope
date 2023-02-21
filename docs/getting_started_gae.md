# Getting Started for GAE
This tutorial will give you a quick tour of GraphScope's GAE. To get started, we’ll start by installing GraphScope on local with `python3 -m pip install graphscope --upgrade`. The examples in this guide are based on Python.


## Loading Graph Data
First, we can load a build-in graph into GraphScope:

```python
import graphscope
from graphscope.dataset import load_ogbn_mag
g = load_ogbn_mag()
```

If you need to load your own dataset, please check out our detailed [introduction to how to load graph data](https://graphscope.io/docs/latest/how_to_load_graphs.html).

## Running Built-in Graph Analytics Algorithms
GAE of GraphScope provides [20 graph analytics algorithms](https://graphscope.io/docs/latest/design_of_gae.html#Algorithm-Library) as built-in algorithms, and users can directly invoke them. The build-in algorithms contain most commonly used algorithms, including PageRank, BFS, DFS, shortest path and LCC. For example, we can run PageRank with:

```python
result = graphscope.pagerank(g)
print(result)
``` 

You may encounter the compilation error when running built-in algorithms, as the built-in graph analytics algorithms are written in C++ for better performance. To solve this problem, you can run the following command to install necessary environments for compilation.

```bash
apt-get install cmake -y
```

## Running NexworkX Graph Algorithms

GraphScope is compatible with NetworkX APIs, and thus a graph algorithm developed for NetworkX (e.g., degree centrality) can directly run on GraphScope:

```python
import graphscope.nx as nx
G = nx.Graph(g)
nx.degree_centrality(G)
```

More details about NetworkX algorithms can refer to [this](https://networkx.org/documentation/stable/reference/algorithms/index.html).

## Storing Results

Finally, users can store the outputs of graph analytics algorithms as dataframe format for further processing:

```python
# result = graphscope.pagerank(g)
result.to_dataframe({"node": "v.id", "r": "r"})
```
