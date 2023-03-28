# Design of GLE

## Introduction

GLE (Graph Learning Engine) is a distributed framework to develop and implement graph neural networks at a large scale. GLE has been successfully applied in various scenarios such as network security, knowledge graph, and search recommendation. It facilitates sampling on batch graphs and enables offline or incremental GNN model training. GLE provides graph sampling operations with both Python and C++ interfaces, and a GSL (Graph Sampling Language) interface that is similar to Gremlin. GLE provides model development paradigms and processes for GNN models, and is compatible with TensorFlow and PyTorch. It offers data layer and model layer interfaces, as well as several model examples.

## Architecture

:::{figure-md}

<img src="../images/../docs/images/gle_arch.png"
     alt="graphlearn architecture."
     width="80%">

# Graph Sampling

## Introduction
Graph sampling is an effective technique for managing large graphs and is widely used in programming paradigms represented by the GraphSAGE framework. Sampling reduces data size and facilitates efficient processing by Tensor-Based computing frameworks through data alignment.

Before sampling, we are required to provide seeds, which can be either nodes or edges. Correspondingly, GLE provides graph traversal operators to prepare the seeds for batch-sampling:
- node sampler
- edge sampler

Then, sampling requirements by users can be abstracted and categorized into the following classes of operations in GLE: 
- Neighborhood Sampling.
- Subgraph Sampling.
- Negative Sampling. 
  
Neighborhood Sampling involves selecting one-hop or multi-hop neighboring vertices based on the input vertex to construct the perceptual field in GCN theory. Input for neighborhood sampling can be from the graph traversal output or other external data sources.

Subgraph Sampling involves vertices of one-hop or multi-hop and all edges with src and dst vertices already sampled, which form a subgraph. This sampling method becomes more and more important as the research in GNN expressivity advances.

Negative Sampling selects vertices that are not directly connected to the input vertices, and it is commonly used in unsupervised learning.

## Graph Traversal

Graph traversal, in GNN, has a different semantics than classical graph computation. The training model of mainstream deep learning algorithms iterates by batch. To meet this requirement, the data has to be accessible by batch, and we call this data access pattern traversal. In GNN algorithms, the data source is the graph, and the training samples usually consist of the vertices and edges of the graph. Graph traversal refers to providing the algorithm with the ability to access vertices, edges or subgraphs by batch.

Currently GLE supports batch traversal of vertices and edges. This random traversal can be either putback-free or putback. In a no-replay traversal, gl.OutOfRangeError is triggered every time an epoch ends. The data source being traversed is partitioned, i.e. the current worker (in the case of distributed TF) only traverses the data on the Server corresponding to it.

For usage and interfaces of graph traversal, please check [the traversal part of GLE](https://graph-learn.readthedocs.io/en/latest/en/gl/graph/graph_operator/graph_traverse.html) for more details.


## Neighborhood Sampling

Different implementation strategies such as random and edge-weight are available for each sampling operation. We have accumulated over 10 sampling operators through practical production and have made the operator programming interface open to allow user customization to keep up with the ever-evolving GNN.

For sampling strategies, GLE currently has support for the following sampling strategies, corresponding to the `strategy` parameters when generating `NeighborSampler` objects.

|strategy|description   |
|------|-----------|
|edge_weight	| Samples with probability with edge weights|
|random	| random_with_replacement|
|topk	|Return the neighbors with edge weight topK, if there are not enough neighbors, refer to the padding rule.|
|in_degree	|Probability sampling by vertex degree.|
|full	|Returns all neighbors, the expand_factor parameter does not work, the result object is SparseNodes or SparseEdges.|

Next is an example for neighbor sampling:

**Example:**

As shown in the figure below, starting from a vertex of type user, sample its 2-hop neighbors, and return the result as layers, which contains layer1 and layer2. layer’s index starts from 1, i.e. 1-hop neighbor is layer1 and 2-hop neighbor is layer2.

:::{figure-md}

<img src="../images/../docs/images/2_hop_sampling.png"
     alt="graphlearn architecture."
     width="45%">

```python
s = g.neighbor_sampler(["buy", "i2i"], expand_factor=[2, 2])
l = s.get(ids) # input ids: shape=(batch_size)

# Nodes object
# shape=(batch_size, expand_factor[0])
l.layer_nodes(1).ids
l.layer_nodes(1).int_attrs

 # Edges object
 # shape=(batch_size * expand_factor[0], expand_factor[1])
l.layer_edges(2).weights
l.layer_edges(2).float_attrs
```

For usage and interfaces of neighborhood sampling, please check [the sampling part of GLE](https://graph-learn.readthedocs.io/en/latest/en/gl/graph/graph_operator/graph_sampling.html) for more details.

## Subgraph Sampling

Unlike EgoGraph, SubGraph contains the edge_index of the graph topology, so the message passing path (forward computation path) can be determined directly by the edge_index, and the implementation of the conv layer can be done directly by the edge_index and the nodes/edges data. In addition, SubGraph is fully compatible with the Data in PyG, so the model part of PyG can be reused.

Subgraph Sampling involves vertices of one-hop or multi-hop and all edges with src and dst vertices already sampled, which form a subgraph. 

This sampling method becomes more and more important as the research in GNN expressivity advances.

## Negative Sampling
**Negative sampling** refers to sampling vertices that have no direct edge relationship with a given vertex. Similar to neighbor sampling, negative sampling has different implementation strategies, such as random, in-degree of nodes, etc. As a common operator of GNN, negative sampling supports extensions and scenario-oriented customization. In addition, GLE provides the ability to negative sampling by specified attribute condition.

GLE currently supports the following negative sampling strategies, corresponding to the strategy argument when generating NegativeSampler objects.

|strategy	|description|
|------- | ------|
|random	|Random negative sampling, not guaranteed true-negative|
|in_degree	|Negative sampling with probability of vertex entry distribution, guaranteed true-negative|
|node_weight	|Negative sampling with probability of vertex weight, true-negative|

Next is an example for negative sampling:

**Example:**

```python
es = g.edge_sampler("buy", batch_size=3, strategy="random")
ns = g.negative_sampler("buy", 5, strategy="random")

for i in range(5):
    edges = es.get()
    neg_nodes = ns.get(edges.src_ids)
    
    print(neg_nodes.ids) # shape is (3, 5)
    print(neg_nodes.int_attrs) # shape is (3, 5, count(int_attrs))
    print(neg_nodes.float_attrs) # shape as (3, 5, count(float_attrs))
```

For usage and interfaces of negative sampling, please check [the negative sampling part of GLE](https://graph-learn.readthedocs.io/en/latest/en/gl/graph/graph_operator/negative_sampling.html) for more details.


## GSL Introduction
GLE abstracts sampling operations into a set of interfaces, called GSL (Graph Sampling Language). Generally, graph sampling consists of several categories as follows.

- Traversal type (Traverse), which obtains point or edge data of a batch from the graph.

- Relational (Neighborhood, Subgraph), which obtains the N-hop neighborhood of points or generates a subgraph composed of points for constructing training samples.

- Negative sampling (Negative), as opposed to relational, which is generally used in unsupervised training scenarios to generate negative example samples.

For example, for the heterogeneous graph scenario of “users clicking on products”, “randomly sample 64 users and sample 10 related products for each user by the weight of the edges”. This can be presented by GSL as 

`g.V("user").batch(64).outV("click").sample(10).by("edge_weight") `

GSL covers support for oversized graphs, heterogeneous graphs, and attribute graphs considering the characteristics of the actual graph data, and the syntax is designed close to the Gremlin form for easy understanding.

For more details, please check [the GSL part of GLE](https://graph-learn.readthedocs.io/en/latest/en/gl/graph/gsl.html).

## Model Paradigms
### Introduction
Most GNNs algorithms follow the computational paradigm of message passing or neighbor aggregation, and some frameworks and papers divide the message passing process into aggregate, update, etc. However, in practice, the computational process required by different GNNs algorithms is not exactly the same.

In practical industrial applications, the size of the graph is relatively large and the features on the nodes and edges of the graph are complex (there may be both discrete and continuous features), so it is not possible to perform message passing/neighbor aggregation directly on the original graph. A feasible and efficient approach is based on the idea of sampling, where a subgraph is first sampled from the graph and then computed based on the subgraph. After sampling out the subgraph, the node and edge features of that subgraph are preprocessed and uniformly processed into vectors, and then the computation of efficient message passing can be performed based on that subgraph.

To summarize, we summarize the paradigm of GNNs into 3 stages: subgraph sampling, feature preprocessing, and message passing.

1. **Subgraph sampling**: Subgraphs are obtained through GSL sampling provided by GraphLearn, which provides graph data traversal, neighbor sampling, negative sampling, and other functions.

2. **Feature preprocessing**: The original features of nodes and edges are preprocessed, such as vectorization (embedding lookup) of discrete features.

3. **Message passing**: Aggregation and update of features through topological relations of the graph.

According to the difference of neighbor sampling operator in subgraph sampling and NN operator in message passing, we organize the subgraph into EgoGraph or SubGraph format. EgoGraph consists of central object ego and its fixed-size neighbors, which is a dense organization format. SubGraph is a more general subgraph organization format, consisting of nodes, edges features and edge index (a two-dimensional array consisting of row index and column index of edges), generally using full neighbor. The conv layer based on SubGraph generally uses the sparse NN operator. EgoGraph refers to a subgraph composed of ego (central node) and k-hop neighbors; SubGraph refers to a generalized subgraph represented by nodes, edges and edge_index.

Next, we introduce two different computational paradigms based on EgoGraph and SubGraph.

### EgoGraph-based node-centric aggregation
EgoGraph consists of ego and neighbors, and the message aggregation path is determined by the potential relationship between ego and neighbors. k-hop neighbors only need to aggregate the messages of k+1-hop neighbors, and the whole message passing process is carried out along the directed meta-path from neighbors to themselves. In this approach, the number of sampled neighbor hops and the number of layers of the neural network need to be exactly the same. The following figure illustrates the computation of a 2-hop neighbor model of GNNs. The vector of original nodes is noted as h(0); the first layer forward process needs to aggregate 2-hop neighbors to 1-hop neighbors and 1-hop neighbors to itself, the types of different hop neighbors may be different, so the first layer needs two different conv layers (for homogeneous graphs, these two conv layers are the same), and the features of nodes after the first layer are updated to h(1) as the input of the second layer; at the second layer, it needs to aggregate the h(1) of 1-hop neighbors to update the ego node features, and the final output node features h(2) as the embedding of the final output ego node.

:::{figure-md}

<img src="../images/../docs/images/egolayer.png"
     alt="egograph."
     width="80%">

### SubGraph-based graph message passing
Unlike EgoGraph, SubGraph contains the edge_index of the graph topology, so the message passing path (forward computation path) can be determined directly by the edge_index, and the implementation of the conv layer can be done directly by the edge_index and the nodes/edges data. In addition, SubGraph is fully compatible with the Data in PyG, so the model part of PyG can be reused.

### Pipeline for Learning

A GNN training/prediction task usually consists of the following steps.

:::{figure-md}

<img src="../images/../docs/images/gle_pipeline.png"
     alt="egograph."
     width="90%">

The first step in using GraphLearn is to prepare the graph data according to the application scenario. Graph data exists in the form of a vertex table and an edge table, and an application scenario will usually involve multiple types of vertices and edges. These can be added one by one using the interface provided by GraphLearn. The construction of graph data is a critical part of the process as it determines the upper limit of algorithm learning. It is important to generate reasonable edge data and choose appropriate features that are consistent with business goals.

Once the graph is constructed, samples need to be sampled from the graph to obtain the training samples. It is recommended to use GraphLearn Sample Language (GSL) to construct the sample query. GSL can use GraphLearn’s asynchronous and multi-threaded cache sampling query function to efficiently generate the training sample stream.

The output of GSL is in Numpy format, while the model based on TensorFlow or PyTorch needs data in tensor format. Therefore, the data format needs to be converted first. Additionally, the features of the original graph data may be complex and cannot be directly accessed for model training. For example, node features such as "id=123456, age= 28, city=Beijing" and other plaintexts need to be processed into continuous features by embedding lookup. GraphLearn provides a convenient interface to convert raw data into vector format, and it is important to describe clearly the type, value space, and dimension of each feature after vectorization when adding vertex or edge data sources.

In terms of GNN model construction, GraphLearn encapsulates EgoGraph based layers and models, and SubGraph based layers and models. These can be used to build a GNNs model after selecting a model paradigm that suits your needs. The GNNs model takes EgoGraph or BatchGraph (SubGraph of mini-batch) as input and outputs the embedding of the nodes.

After getting the embedding of the vertices, the loss function is designed with the scenario in mind. Common scenarios can be classified into two categories: node classification and link prediction. For link prediction, the input required includes "embedding of source vertex, embedding of destination vertex, embedding of target vertex with negative sampling", and the output is the loss. This loss is then optimized by iterating through the trainer. GraphLearn encapsulates some common loss functions that can be found in the section "Common Losses".
