# Built-in Algorithms

The graph analytical engine (GAE) of GraphScope offers many built-in algorithms, which enable users to analyze their graph data with least effort. The built-in algorithms covers a wide range of applications, such as the shortest path, community detection, clustering, etc. The built-in algorithms are implemented with the [PIE programming model](https://graphscope.io/docs/latest/analytical_engine/programming_model_pie.html) and highly optimized for the best performance, and users can use them in the out-of-box manner.

Here is the full list of supported built-in algorithms:

- `sssp(source)`
- `pagerank(alpha, max_round, tolerance)`
- `lpa(max_round)`
- `kkore(k)`
- `kshell(k)`
- `hits(tolerance, max_round, normalized)`
- `dfs(source_id)`
- `bfs(source_id)`
- `voderank(num_of_nodes)`
- `clustering()`
- `all_pairs_shortest_path_length()`
- `attribute_assortativity()`
- `average_degree_connectivity(source_degree_type, target_degree_type)`
- `degree_assortativity_coefficient(source_degree_type, target_degree_type, weighted)`
- `betweenness_centrality(normalized, endpoints)`
- `closeness_centrality(wf)`
- `degree_centrality(centrality_type)`
- `eigenvector_centrality(tolerance, max_round)`
- `katz_centrality(alpha, beta, tolerance, max_round, normalized)`
- `sampling_path(source_id, cutoff)`

## Single-Source Shortest Paths (SSSP)

The Single-Source Shortest Paths (SSSP) fixes a single vertex as the "source" vertex and finds shortest paths from the source to all other vertices in the graph. To run `sssp(source)`, users need to assign a vertex `source` as the "source" vertex.

## PageRank

PageRank is a way of measuring the importance of each vertices in a graph. The PageRank algorithm exists in many variants, the `pagerank(alpha, max_round, tolerance)` in GAE follows [the PageRank implementation of NetworkX](https://networkx.org/documentation/networkx-1.7/reference/generated/networkx.algorithms.link_analysis.pagerank_alg.pagerank.html), where `alpha` represents damping parameter for PageRank, `max_round` is maximum number of iterations, and `tolerance` denotes error tolerance used to check convergence. 

## Label Propagation Algorithm (LPA)

The Label Propagation Algorithm (LPA) is a fast algorithm for finding communities in a graph. It is an iterative algorithm where we assign labels to unlabelled vertices by propagating labels through the graph. To run `lpa(max_round)`, users need to decide the number of iterations `max_round` during the computation.

## K-Core

This algorithm is to find a k-core from an original graph, and a k-core is a maximal subgraph that contains vertices of degree *k* or more. The k-core is found by recursively pruning vertices with degrees less than *k*. To run `kkore(k)`, users need to assign `k`, the order of the core.

## K-Shell

The k-shell is the subgraph induced by vertices with core number *k*. That is, vertices in the k-core that are not in the (k+1)-core. Similarly, `kshell(k)` requires users to assign the order of the shell `k`.

## Hyperlink-Induced Topic Search (HITS)

Similar to PageRank, Hyperlink-Induced Topic Search (HITS) is an iterative algorithm to compute the importance of each vertices in a graph. In `hits(tolerance, max_round, normalized)`, `tolerance`, `max_round`, and `normalized` denote error tolerance used to check convergence, maximum number of iterations, and whether the result should be normalized, respectively.

## Depth-First-Search (DFS) 

Depth-First-Search (DFS) is an algorithm for traversing or searching graph data. It starts at a root vertex and explores as far as possible along each branch before backtracking. In `dfs(source_id)`, users need to decide the `source_id` as the root vertex.

## Breadth-First Search (BFS)

Breadth-First Search (BFS) is also an algorithm for traversing or searching graph data. Different from DFS, it starts at a root vertex and explores all vertices at the present depth prior to moving on to the vertices at the next depth level. In `bfs(source_id)`, users need to decide the `source_id` as the root vertex.

## VoteRank

VoteRank is to measure a ranking of the vertices in a graph based on a voting scheme. With VoteRank, all vertices vote for each of its in-neighbours and the vertices with the top `num_of_nodes` highest votes is elected iteratively. In `voderank(num_of_nodes)`, users need to assign `num_of_nodes`, the number of ranked vertices to extract.

## Clustering

The clustering algorithm is to compute the clustering coefficient for each vertex of a graph. The clustering coefficient of a vertex in a graph quantifies how close its neighbours are to being a clique (complete graph). By invoking `clustering()`, users can get the the clustering coefficient of each vertex.

## All Pairs Shortest Path Length

This algorithm is used to find all pair shortest path problem from a given weighted graph. As a result of `all_pairs_shortest_path_length()`, it will compute the minimum distance from any vertex to all other vertices in the graph.

## Attribute Assortativity

Assortativity in a graph refers to the tendency of vertices to connect with other *similar* vertices over *dissimilar* vertices. The result of `attribute_assortativity()` is a measure of the extent to which vertices with the same properties connect to each other.

## Average Degree Connectivity

The average degree connectivity is the average nearest neighbor degree of vertices with degree *k*. `average_degree_connectivity(source_degree_type, target_degree_type)` returns a list of *k*-average degree connectivity for a graph for successive *k*=0,1,2…. If the graph is directed, users can assign `source_degree_type` and `target_degree_type` to indicate whether “in”-, “out”- or "in+out"-degree is used for source/target vertices when computing average degree connectivity.

## Degree Assortativity Coefficient

Similar to attribute assortativity, degree assortativity coefficient measures tendency of having an edge (*u*,*v*) such that, degree(*u*) equals to degree(*v*). If the graph is directed, users can assign `source_degree_type` and `target_degree_type` in `degree_assortativity_coefficient(source_degree_type, target_degree_type, weighted)` to indicate whether “in”-, “out”- or "in+out"-degree is used for source/target vertices when computing degree assortativity coefficient. If the graph is weighted (`weighted = True`), the degree is the sum of the edge weights adjacent to the vertex.

## Betweenness Centrality

Betweenness centrality is a measure of centrality in a graph based on shortest paths. Betweenness centrality of a vertex *v* is the sum of the fraction of all-pairs shortest paths that pass through *v*. In `betweenness_centrality(normalized, endpoints)`, `normalized` and `endpoints` denotes whether the result should be normalized, and whether including the endpoints in the shortest path counts, respectively.

## Closeness Centrality

The original closeness centrality of a vertex *v* is the reciprocal of the average shortest path distance to *v* over all n-1 reachable nodes. Wasserman and Faust proposed an improved formula for graphs with more than one connected component. The result is “a ratio of the fraction of actors in the group who are reachable, to the average distance” from the reachable actors. In `closeness_centrality(wf)`, if `wf` is set as `True`, it returns according to the Wasserman and Faust improved formula. Otherwise, the original formula is applied.

## Degree Centrality

The degree centrality for a vertex *v* is the fraction of nodes it is connected to. If the graph is directed, then three separate measures of degree centrality are defined, namely, in-degree, out-degree and both in- and out-degree. Users can achieve this by set `centrality_type` as `in`, `out` or `both` in `degree_centrality(centrality_type)`.

## Eigenvector Centrality

Eigenvector centrality is a measure of the influence of a vertex in a graph. Relationships originating from high-scoring vertices contribute more to the score of a vertex than connections from low-scoring vertices. A high eigenvector score means that a vertex is connected to many vertices who themselves have high scores. In `eigenvector_centrality(tolerance, max_round)`, `tolerance` and `max_round` denote error tolerance used to check convergence and maximum number of iterations, respectively.

## Katz Centrality

[Katz centrality](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.katz_centrality.html) computes the relative influence of a vertex within a graph by measuring the number of the immediate neighbors (first degree vertices) and also all other vertices in the graph that connect to the vertex under consideration through these immediate neighbors. In `katz_centrality(alpha, beta, tolerance, max_round, normalized)`, `alpha` is attenuation factor, `beta` is weight attributed to the immediate neighborhood, `tolerance` is error tolerance used to check convergence, `max_round` is maximum number of iterations, and `normalized` indicates whether we need to normalize the resulting values.

## Sampling Path

`sampling_path(source_id, cutoff)` executes a set of paths starting from a root vertex `source_id` with path length `cutoff`.


