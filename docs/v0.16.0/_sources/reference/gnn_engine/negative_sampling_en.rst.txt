Negative Sampling
=================

# 1 Introduction As an important method of unsupervised training,
negative sampling refers to sample the vertices that have no direct edge
connected the given vertex. Similar to neighbor sampling, negative
sampling also has different sampling strategies, such as random sampling
and sampling by in-degree of input vertex. As a common operator of GNN,
negative sampling supports expansion and scene-oriented customization.

# 2 Usage ## 2.1 Interface The input of the negative sampling operator
can be an edge or vertex type. When the input is an edge type, the
operator refers to “under certain edge type, sample vertices that are
not directly connected to a given vertex, from a candidate vertices
set”. The candidate vertices set contains all vertices that are
connected by the certain edge type and are not connected to the given
vertex.

When the input is a vertex type, the operator refers to “sample vertices
of a certain type that are not directly connected to a given vertex,
from a candidate vertices set”. Here, the user needs to specify the
candidate vertex set to sample from. The sampling results are organized
into ``Nodes`` objects (similar to 1-hop neighbor sampling, but there is
no ``Edges`` object).

A negative sampling operation can be divided into the following three
steps: - Define the negative sampling oprator by calling
``g.negative_sampler()`` to get the ``NegativeSampler`` object ``S``; -
Invoke ``S.get(ids)`` to get the ``Nodes`` object; - Invoke the
`interface <graph_query_cn.md#FPU74>`__ of ``Nodes`` object to get
specific values.

.. code:: python

   def negative_sampler(object_type, expand_factor, strategy="random"):
   """
   Args:
     object_type(string): Edge type or vertex type
     expand_factor(int):  Number of random samples
     strategy(string):    Sampling strategy, see below for detailed description.
   Return:
     NegativeSampler object.
   """

.. code:: python

   def NegativeSampler.get(ids, **kwargs):
   """ Negative sampling on given vertices ids.
   Args:
     ids(numpy.ndarray): 1-d int64 array
     **kwargs: Extended parameters, different sampling strategies may require different parameters
   Return:
     Nodes object.
   """

## 2.2 Example

.. code:: python

   es = g.edge_sampler("buy", batch_size=3, strategy="random")
   ns = g.negative_sampler("buy", 5, strategy="random")

   for i in range(5):
       edges = es.get()
       neg_nodes = ns.get(edges.src_ids)
       
       print(neg_nodes.ids)          # shape is (3, 5)
       print(neg_nodes.int_attrs)    # shape is (3, 5, count(int_attrs))
       print(neg_nodes.float_attrs)  # shape is (3, 5, count(float_attrs))

In GSL, the main operations of negative sampling are ``outNeg()`` /
``inNeg()`` / ``Neg()``.

.. code:: python

   # 1. Negative sampling 1-hop neighbor vertices
   g.V().outNeg(edge_type).sample(count).by(strategy)

   # 2. Negative sampling 2-hop neighbor vertices
   g.V().outNeg(edge_type).sample(count).by(strategy).outNeg(edge_type).sample(count).by(strategy)

   # 3. Negative sampling on given candidate vertex set
   g.V().Neg(node_type).sample(count).by("node_weight")

# 3 Negative Sampling Strategies GL currently supports the following
negative sampling strategies. They are also possible values of the
``strategy`` parameter when creating the ``NegativeSampler`` object.

+-----------------------------------+-----------------------------------+
| **strategy**                      | **Description**                   |
+===================================+===================================+
| random                            | Random negative sampling,         |
|                                   | true-negative is not guaranteed   |
+-----------------------------------+-----------------------------------+
| in_degree                         | Negative sampling with vertex     |
|                                   | in-degree distribution as         |
|                                   | probability, true-negative is     |
|                                   | guaranteed                        |
+-----------------------------------+-----------------------------------+
| node_weight                       | Negative sampling with vertex     |
|                                   | weight as probability,            |
|                                   | true-negative is guaranteed       |
+-----------------------------------+-----------------------------------+
