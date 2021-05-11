Graph Learning Model
====================

Overview
--------

There are two ways to develop a graph learning model. The first is to
compute base on the whole graph directly. The examples are GCN and GAT
that perform computations directly on an adjacency matrix. However, this
method consumes a large amount of memory on large-scale graphs. As a
result, it may cause poor performance of training. In some cases,
training is even not possible. The second idea is to divide the whole
graph into several subgraphs so that we can use batch training, which is
a common technic in deep learning, for training. We can use
GraphSAGE，FastGCN and GraphSAINT methods to train a subgraph.

**GL** is mainly designed for extremely large-scale graph neural
networks. It consists of a graph engine and high-level algorithm models.
The graph engine stores graph topology and attributes distributedly.
Additionally, it provides efficient interfaces for graph sampling amd
graph query. The algorithm model obtains subgraphs and performs
operations using graph sampling and query interfaces

**GL** is a graph learning framework that supports building common graph
learning algorithms, such as GNNs, knowledge graph models, graph
embedding algorithms, etc. It also compatible with common deep learning
algorithms including TensorFlow and PyTorch. Currently, our model
implementations are based on TensorFlow. The ones are based on PyTorch
are under development.

.. raw:: html

   <p align="center">

.. raw:: html

   </p>

Data model
----------

To build and training models, **GL** first samples data from subgraphs
and then perform computation. Next, we introduce the basic data model.

``EgoGraph`` is the underlying data model in **GL**. It consists of a
batch of seed nodes or edges(named ‘ego’) and their receptive fields
(multi-hops neighbors). We implement many build-in samplers to traverse
the graph and sample neighbors. Negative samplers are also implemented
for unsupervised training.

The sampled data grouped in ``EgoGraph`` is organized into numpy format.
It can be converted to different tensor format, ``EgoTensor``, based on
the different deep learning engine. **GL** uses ``EgoFlow`` to convert
``EgoGraph`` to ``EgoTensor`` that is the training data.

.. raw:: html

   <p align="center">

.. raw:: html

   </p>

Encoder
-------

All graph learning models can be abstracted as using an encoder to
encode ``EgoTensor`` into vectors of nodes, edges or subgraphs

**GL** first uses feature encoders to encode raw features of nodes or
edges, then feature embeddings are encoded by different models to the
final outputs. For most of GNNs models, graph encoders provide abstracts
on how to aggregate neighbors’ information to target
nodes/edges,implemented with different graph convolutional layers.

.. raw:: html

   <p align="center">

.. raw:: html

   </p>

Based on data models and encoders, developers can easily implement
different graph learning models. We introduce in detail how to develope
a GNN model in `Custom Algorithm <algo_en.md>`__

`Home <../README.md>`__
