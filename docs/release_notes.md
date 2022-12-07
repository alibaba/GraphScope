# Release Notes


## v0.18.0

This release contains many important features and enhancements to Graph Interactive Engine (GIE), including introducing a new strategy for pattern-matching queries and supporting canceling running queries. The Graph Learning Engine (GLE) now supports PyTorch and is also compatible with PyG. In addition, we take a first step towards modularized deployment for different components of GraphScope in Kubernetes.

We highlight the following improvements included in this release:

### Enhancements for GIE

A new execution strategy based on worst-case optimal join is introduced to GIE engine, potentially improving the performance of match step by orders of magnitude.
The query can be canceled after its execution exceeds a pre-given overtime parameter (10min by default).
GIE engine supports a failover mechanism: if an executor pod fails, but the data is not missing, it can be restarted by k8s. Existing queries cannot recover, but the engine can serve the following.

### Enhancements for GAE

Add more variants of WCC algorithm.
Supports local vertex map to make it could scale to larger graphs given more workers.

### Enhancements for GLE

Add support for PyTorch and PyG
Add heterogeneous graph support for subgraph-based GNN, add HeteroSubGraph and HeteroConv, bipartite GraphSAGE and UltraGCN.
Add edge feature support in both EgoGraph and SubGraph.
Add recommendation metrics: Recall, NDCG and HitRate.
Add hiactor-based graph engine.

### Standalone deployment

Supports a version of GIE standalone deployment in Kubernetes.
Redesigned docker files from the bottom to the top, making it more clear and more concise.
5. GAE Java Enhancement

Introduce @GrapeSkip for better user experience in template method overloading.
Speedup Java App runimte codegen.

Learn more about this release on [Release](https://github.com/alibaba/GraphScope/releases/tag/v0.18.0).