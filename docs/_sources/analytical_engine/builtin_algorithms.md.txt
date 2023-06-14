# Built-in Algorithms

The graph analytical engine (GAE) of GraphScope offers many built-in algorithms, which enable users to analyze their graph data with least effort. The built-in algorithms covers a wide range of applications, such as the shortest path, community detection, clustering, etc. The built-in algorithms are implemented with the [PIE programming model](https://graphscope.io/docs/latest/analytical_engine/programming_model_pie.html) and highly optimized for the best performance, and users can use them in the out-of-box manner.

Here is the full list of supported built-in algorithms:

- [All Pairs Shortest Path Length](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#all-pairs-shortest-path-length)
- [Attribute Assortativity](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#attribute-assortativity)
- [Average Degree Connectivity](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#average-degree-connectivity)
- [Betweenness Centrality](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#betweenness-centrality)
- [Breadth-First Search (BFS)](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#breadth-first-search)
- [Closeness Centrality](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#closeness-centrality)
- [Clustering](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#clustering)
- [Degree Assortativity Coefficient](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#degree-assortativity-coefficient)
- [Degree Centrality](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#degree-centrality)
- [Depth-First Search (DFS)](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#depth-first-search)
- [Eigenvector Centrality](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#eigenvector-centrality)
- [Hyperlink-Induced Topic Search (HITS)](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#hyperlink-induced-topic-search)
- [Katz Centrality](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#katz-centrality)
- [K-Core](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#k-core)
- [K-Shell](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#k-shell)
- [Label Propagation Algorithm (LPA)](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#label-propagation-algorithm)
- [PageRank](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#pagerank)
- [Sampling Path](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#sampling-path)
- [Single-Source Shortest Paths (SSSP)](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#single-source-shortest-paths)
- [VoteRank](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html#voterank)

## All Pairs Shortest Path Length

This algorithm is used to find all pair shortest path problem from a given weighted graph. As a result of this algorithm, it will compute the minimum distance from any vertex to all other vertices in the graph.

```{py:function} all_pairs_shortest_path_length()

Compute the shortest path lengths between all vertices in the graph.
```

## Attribute Assortativity

Assortativity in a graph refers to the tendency of vertices to connect with other *similar* vertices over *dissimilar* vertices. Attribute assortativity is a measure of the extent to which vertices with the same properties connect to each other.

```{py:function} attribute_assortativity()

Compute assortativity for vertex attributes.
```

## Average Degree Connectivity

The average degree connectivity is the average nearest neighbor degree of vertices with degree *k*. This algorithm returns a list of *k*-average degree connectivity for a graph for successive *k*=0,1,2….

```{py:function} average_degree_connectivity(source_degree_type, target_degree_type)

Compute the average degree connectivity of the graph.

:param source_degree_type: use “in”-,“out”- or ”in+out”-degree for source vertex
:type source_degree_type: str
:param target_degree_type: use “in”-,“out”- or ”in+out”-degree for target vertex
:type target_degree_type: str
```

## Betweenness Centrality

Betweenness centrality is a measure of centrality in a graph based on shortest paths. Betweenness centrality of a vertex *v* is the sum of the fraction of all-pairs shortest paths that pass through *v*. 

```{py:function} betweenness_centrality(normalized, endpoints)

Compute the shortest-path betweenness centrality for vertices.

:param normalized: whether the result should be normalized
:type normalized: bool
:param endpoints: whether including the endpoints in the shortest path counts
:type endpoints: bool
```

## Breadth-First Search

Breadth-First Search (BFS) is an algorithm for traversing or searching graph data. It starts at a root vertex and explores all vertices at the present depth prior to moving on to the vertices at the next depth level. 

```{py:function} bfs(source)

Compute a list of vertices in breadth-first search from source.

:param source: The source vertex for breadth-first search
:type source: int
```

## Closeness Centrality

The original closeness centrality of a vertex *v* is the reciprocal of the average shortest path distance to *v* over all n-1 reachable nodes. Wasserman and Faust proposed an improved formula for graphs with more than one connected component. The result is “a ratio of the fraction of actors in the group who are reachable, to the average distance” from the reachable actors. 

```{py:function} closeness_centrality(wf)

Compute closeness centrality for vertices.

:param wf: whether the Wasserman and Faust improved formula is used
:type wf: bool
```

## Clustering

The clustering algorithm is to compute the clustering coefficient for each vertex of a graph. The clustering coefficient of a vertex in a graph quantifies how close its neighbors are to being a clique (complete graph).

```{py:function} clustering()

Compute the clustering coefficient for each vertex in the graph.
```

## Degree Assortativity Coefficient

Similar to attribute assortativity, degree assortativity coefficient measures tendency of having an edge (*u*,*v*) such that, degree(*u*) equals to degree(*v*).

```{py:function} degree_assortativity_coefficient(source_degree_type, target_degree_type, weighted)

Compute degree assortativity coefficient of the graph.

:param source_degree_type: use “in”-,“out”- or ”in+out”-degree for source vertex
:type source_degree_type: str
:param target_degree_type: use “in”-,“out”- or ”in+out”-degree for target vertex
:type target_degree_type: str
:param weight: The edge attribute that holds the numerical value used as a weight. If `False`, then each edge has weight 1. The degree is the sum of the edge weights adjacent to the vertex.
:type weight: bool
```

## Degree Centrality

The degree centrality for a vertex *v* is the fraction of nodes it is connected to. If the graph is directed, then three separate measures of degree centrality are defined, namely, in-degree, out-degree and both in- and out-degree. 

```{py:function} degree_centrality(centrality_type)

Compute the degree centrality for vertices.

:param centrality_type: `in`, `out` or `both` degree are applied
:type centrality_type: str
```

## Depth-First Search

Depth-First-Search (DFS) is an algorithm for traversing or searching graph data. It starts at a root vertex and explores as far as possible along each branch before backtracking.

```{py:function} dfs(source)

Compute a list of vertices in depth-first search from source.

:param source: The source vertex for depth-first search
:type source: int
```

## Eigenvector Centrality

Eigenvector centrality is a measure of the influence of a vertex in a graph. Relationships originating from high-scoring vertices contribute more to the score of a vertex than connections from low-scoring vertices. A high eigenvector score means that a vertex is connected to many vertices who themselves have high scores.

```{py:function} eigenvector_centrality(tolerance, max_round)

Compute the eigenvector centrality for the graph.

:param tolerance: error tolerance used to check convergence
:type tolerance: double
:param max_round: maximum number of iterations
:type max_round: int
```

## Hyperlink-Induced Topic Search

Hyperlink-Induced Topic Search (HITS) is an iterative algorithm to compute the importance of each vertices in a graph.  It rates vertices based on two scores, a hub score and an authority score. The authority score estimates the importance of the vertex within the graph. The hub score estimates the value of its relationships to other vertices.

```{py:function} hits(tolerance, max_round, normalized)

Compute the HITS value for each vertex in the graph.

:param tolerance: error tolerance used to check convergence
:type tolerance: double
:param max_round: maximum number of iterations
:type max_round: int
:param normalized: whether we need to normalize the resulting values
:type normalized: bool
```

## Katz Centrality

[Katz centrality](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.katz_centrality.html) computes the relative influence of a vertex within a graph by measuring the number of the immediate neighbors (first degree vertices) and also all other vertices in the graph that connect to the vertex under consideration through these immediate neighbors. 

```{py:function} katz_centrality(alpha, beta, tolerance, max_round, normalized)

Compute the Katz centrality for the vertices of the graph.

:param alpha: attenuation factor
:type alpha: double
:param beta: weight attributed to the immediate neighborhood
:type beta: double
:param tolerance: error tolerance used to check convergence
:type tolerance: double
:param max_round: maximum number of iterations
:type max_round: int
:param normalized: whether we need to normalize the resulting values
:type normalized: bool
```

## K-Core

This algorithm is to find a k-core from an original graph, and a k-core is a maximal subgraph that contains vertices of degree *k* or more. The k-core is found by recursively pruning vertices with degrees less than *k*.

```{py:function} kkore(k)

Compute the k-core of the graph.

:param k: The order of the core
:type k: int
```

## K-Shell

The k-shell is the subgraph induced by vertices with core number *k*. That is, vertices in the k-core that are not in the (k+1)-core. 

```{py:function} kshell(k)

Compute the k-shell of the graph.

:param k: The order of the shell
:type k: int
```

## Label Propagation Algorithm

The Label Propagation Algorithm (LPA) is a fast algorithm for finding communities in a graph. It is an iterative algorithm where we assign labels to unlabelled vertices by propagating labels through the graph. 

```{py:function} lpa(max_round)

Compute the label of each vertex in the graph.

:param max_round: the number of iterations during the computation
:type max_round: int
```

## PageRank

PageRank is a way of measuring the importance of each vertices in a graph. The PageRank algorithm exists in many variants, the PageRank in GAE follows [the PageRank implementation of NetworkX](https://networkx.org/documentation/networkx-1.7/reference/generated/networkx.algorithms.link_analysis.pagerank_alg.pagerank.html).

```{py:function} pagerank(alpha, max_round, tolerance)

Compute the PageRank value for each vertex in the graph.

:param alpha: damping parameter for PageRank
:type alpha: double
:param max_round: maximum number of iterations
:type max_round: int
:param tolerance: error tolerance used to check convergence
:type tolerance: double
```

## Sampling Path

This algorithm samples a set of paths starting from a root vertex with path length.

```{py:function} sampling_path(source_id, cutoff)

Compute a set of paths starting from a root vertex.

:param source_id: the source vertex of path sampling
:type source_id: int
:param cutoff: maximum length of paths
:type cutoff: int
```


## Single-Source Shortest Paths

The Single-Source Shortest Paths (SSSP) fixes a single vertex as the "source" vertex and finds shortest paths from the source to all other vertices in the graph. 

```{py:function} sssp(source)

Compute shortest paths from a source vertex in the graph.

:param source: The source vertex for the shortest paths
:type source: int
```

## VoteRank

VoteRank is to measure a ranking of the vertices in a graph based on a voting scheme. With VoteRank, all vertices vote for each of its in-neighbors and the vertices with the top highest votes is elected iteratively. 

```{py:function} voterank(num_of_nodes)

Select a list of influential vertices in a graph using VoteRank algorithm.

:param num_of_nodes: the number of ranked vertices to extract
:type num_of_nodes: int
```