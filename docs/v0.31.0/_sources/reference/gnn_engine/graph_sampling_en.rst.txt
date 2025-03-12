Graph Sampling
==============

1 Introduction
==============

Graph sampling is an effective way to deal with very large graphs, which
has been widely adopted by mainstream Graph Neural Network models such
as `GraphSage <https://arxiv.org/abs/1706.02216>`__. In practice,
sampling not only reduces the data scale, but also aligns the data,
which is conducive to the efficient processing of the tensor-based
computing framework.

We characterized the sampling operations into **2 types** according to
user needs: **neighbor sampling** and **negative sampling**. Neighbor
sampling retrieves multi-hops neighbors of the given input vertices.
Those sampled neighbors could be used to construct the receptive field
in `GCN <https://arxiv.org/abs/1609.02907>`__ theory. Negative sampling
retrieves vertices which are not directly connected to the given input
vertices. In practice, negative sampling is an important method for
supervised learning.

Each type of sampling operation has different sampling strategies, such
as random, weighted, etc. To support the rich application scenarios, we
have implemented more than ten kinds of ready-made sampling operators.
We also expose necessary APIs for users to customize their own sampling
operators to meet the needs of the rapidly developing GNN. This chapter
introduces neighbor sampling, and negative sampling will be described in
detail in the next chapter. In addition, we are working on implementing
the sub-graph sampling methods proposed in recent AI top conferences.

# 2 Usage ## 2.1 Interface The sampling operator takes meta-path and
sampling number as input which supports for sampling any heterogeneous
graph and arbitrary hops. The sampling results are organized into
``Layers`` where the n-th ``Layer`` contains the sampling result from
the n-th hop. One can access the ``Nodes`` and ``Edges`` in each
``Layer`` object. A sampling operation can be divided into the following
three steps:

-  Define the sampling operator by invoking ``g.neighbor_sampler()`` to
   get the ``NeighborSampler`` object ``S``
-  Invoke ``S.get(ids)`` to get the neighbor ``Layers`` object ``L`` of
   the vertex;
-  Invoke ``L.layer_nodes(i)``, ``L.layer_edges(i)`` to get the vertices
   and edges of the i-th hop neighbors;

.. code:: python

   def neighbor_sampler(meta_path, expand_factor, strategy="random"):
   """
   Args:
     meta_path(list):     List of edge_type(string). Stand for paths of neighbor sampling.
     expand_factor(list): List of int. The i-th element represents the number of neighbors sampled by the i-th hop; the length must be consistent with meta_path
     strategy(string):    Sampling strategy. Please refer to the detailed explanation below.
   Return:
     NeighborSampler object
   """

.. code:: python

   def NeighborSampler.get(ids):
   """ Sampling multi-hop neighbors of given ids.
   Args:
     ids(numpy.ndarray): 1-d int64 array
   Return:
     Layers object
   """

The returned result of the sampling is a ``Layers`` object, which
represents “the neighbor vertices of the source vertex and the edges in
between”. The shape of each ``Layer``\ ’s ids is two-dimensional.
Specifically, ids shape=\ **[expanded size of ids of the previous layer,
number of samples in the current layer]**.

.. code:: python

   def Layers.layer_nodes(layer_id):
   """ Get the `Nodes` of the i-th layer, layer_id starts from 1. """
       
   def Layers.layer_edges(layer_id):
   """ Get the `Edges` of the i-th layer, layer_id starts from 1. """

In GSL, refer to the ``g.out*`` and ``g.in*`` families of interfaces.
E.g.

.. code:: python

   # Sampling 1-hop neighbor vertices
   g.V().outV().sample(count).by(strategy)

   # Sampling 2-hop neighbor vertices
   g.V().outV().sample(count).by(strategy).outV().sample(count).by(strategy)

## 2.2 Example The figure below shows a query that samples the 2-hop
neighbors of a given “user” vertex. The returned result contains two
layers, where layer1 is one-hop neighbors and layer2 is two-hop
neighbors.

.. container::

.. code:: python

   s = g.neighbor_sampler(["buy", "i2i"], expand_factor=[2, 2])
   l = s.get(ids) # input ids: shape=(batch_size)

   # Nodes object
   # shape=(batch_size, expand_factor[0])
   l.layer_nodes(1).ids
   l.layer_nodes(1).int_attrs

    # Edges object
    # shape=(batch_size *  expand_factor[0],  expand_factor[1])
   l.layer_edges(2).weights
   l.layer_edges(2).float_attrs

# 3 Sampling Strategy GL currently supports the following sampling
``strategy`` options when creating the ``NeighborSampler`` object.

+-----------------------------------+-----------------------------------+
| **strategy**                      | **Description**                   |
+===================================+===================================+
| edge_weight                       | Sampling with edge weight as      |
|                                   | probability                       |
+-----------------------------------+-----------------------------------+
| random                            | Random sampling with replacement  |
+-----------------------------------+-----------------------------------+
| random_without_replacement        | Random sampling without           |
|                                   | replacement. Refer to the padding |
|                                   | rule when the number of neighbors |
|                                   | is not enough                     |
+-----------------------------------+-----------------------------------+
| topk                              | Return the top k neighbors with   |
|                                   | edge weight. Refer to the padding |
|                                   | rule when the number of neighbors |
|                                   | is not enough                     |
+-----------------------------------+-----------------------------------+
| in_degree                         | Sampling with vertex in-degree    |
|                                   | probability                       |
+-----------------------------------+-----------------------------------+
| full                              | Return all neighbors. The         |
|                                   | expand_factor parameter has no    |
|                                   | effect on this strategy. The      |
|                                   | result object is SparseNodes or   |
|                                   | SparseEdges, see `graph           |
|                                   | q                                 |
|                                   | uery <graph_query_en.md#FPU74>`__ |
|                                   | for detailed description of those |
|                                   | objects.                          |
+-----------------------------------+-----------------------------------+

Padding rules: the returned value needs to be padded when the amount of
data required for sampling is not enough. ``default_neighbor_id`` is
used to pad missing ``id``, whose value is 0 unless set by users through
``gl.set_default_neighbor_id(xx)``. One can use other padding modes,
e.g., padding with existing neighbor ids instead of
``default_neighbor_id`` using ``gl.set_padding_mode(gl.CIRCULAR)``.
