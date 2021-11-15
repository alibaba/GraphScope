Graph Traversal
===============

# 1. Introduction The semantics of graph traversal in GNN is different
from classic graph computation. Training by batch is the characteristics
of mainstream deep learning alogrithms. Thus, to meet this requirement,
graph data must be able to access by batches. We denote this data access
pattern as graph traversal. In a GNN algorithm, the data source is the
graph and the training samples are comprised of vertices and edges.
Graph traversal refers to the approach of obtaining the training
vertices and edges in batch.

Currently, **GL** supports traversing graph by batch and such random
traversal could be with or without replacement. In the traversal without
replacement, the program will throw a ``gl.OutOfRangeError`` exception
when an epoch is finished. The data source to be traversed is
partitioned, which means each worker(e.g. distributed tensorflow) only
traverses the data on the corresponding Server.

# 2. Vertex Traversal ## 2.1 Usages There are three type of data sources
for vertices: all unique vertices, source vertices of all edges and
destination vertices of all edges. Vertex traversal is implemented by
the ``NodeSampler`` operator. The ``node_sampler()`` API of ``Graph``
object returns a ``NodeSampler`` object, and then invokes ``get()`` API
of this object to return ``Nodes`` data.

.. code:: python

   def node_sampler(type, batch_size=64, strategy="by_order", node_from=gl.NODE):
   """
   Args:
     type(string):     type="vertex" when node_from=gl.NODE, otherwise type="edge";
     batch_size(int):  number of vertices in each traversal
     strategy(string):  can only take value from {"by_order, "random"}.
         "by_order": stands for traverse without replacements, the return number 
             is the actual traversed count. A gl.OutOfRangeError will throw if 
             the actual traversed count is 0 (i.e. when an epoch is finished); 
         "random": stands for random traverse.
     node_from: data source, can only take value from {gl.NODE、gl.EDGE_SRC、gl.EDGE_DST}
   Return:
     NodeSampler object
   """

.. code:: python

   def NodeSampler.get():
   """
   Return:
       Nodes object, data shape=[batch_size] if current epoch isn't finished.
   """

You can access specific values through the ``Nodes`` object, such as id,
weight, attribute and etc. Please refer to APIs
`API <graph_query_cn.md#FPU74>`__ for more details. And refer to
``g.V()`` for vertex traversal in GSL.

## 2.2 Example \| id \| attributes \| \| — \| — \| \| 10001 \| 0:0.1:0
\| \| 10002 \| 1:0.2:3 \| \| 10003 \| 3:0.3:4 \|

.. code:: python

   sampler = g.node_sampler("user", batch_size=3, strategy="random")
   for i in range(5):
       nodes = sampler.get()
       print(nodes.ids)
       print(nodes.int_attrs)
       print(nodes.float_attrs)

# 3 Edge Traversal ## 3.1 Usages Edge traversal is implemented by the
``EdgeSampler`` operation. The ``edge_sampler()`` API of ``Graph``
object returns a ``EdgeSampler`` object, and then invokes ``get()`` API
of this object to return ``Edges`` data.

.. code:: python

   def edge_sampler(edge_type, batch_size=64, strategy="by_order"):
   """
   Args:
     edge_type(string): edge type
     batch_size(int):   number of edges in each traversal
     strategy(string):  can only take value from {"by_order, "random"}.
         "by_order": stands for traverse without replacements, the return number 
             is the actual traversed count. A gl.OutOfRangeError will throw if 
             the actual traversed count is 0 (i.e. when an epoch is finished); 
         "random": stands for random traverse.
   Return:
     EdgeSampler object
   """

.. code:: python

   def EdgeSampler.get():
   """
   Return:
       Edges object, data shape=[batch_size] if current epoch isn't finished.
   """

You can access specific values through the ``Edges`` object, such as id,
weight, attribute, etc. Please refer to
`API <graph_query_cn.md#FPU74>`__ for more details. And refer to
``g.E()`` for edge traversal in GSL.

## 3.2 Example \| src_id \| dst_id \| weight \| attributes \| \| — \| —
\| — \| — \| \| 20001 \| 30001 \| 0.1 \|
0.10,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19 \| \| 20001 \| 30003
\| 0.2 \| 0.20,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29 \| \| 20003
\| 30001 \| 0.3 \| 0.30,0.31,0.32,0.33,0.34,0.35,0.36,0.37,0.38,0.39 \|
\| 20004 \| 30002 \| 0.4 \|
0.40,0.41,0.42,0.43,0.44,0.45,0.46,0.47,0.48,0.49 \|

.. code:: python

   sampler = g.edge_sampler("buy", batch_size=3, strategy="random")
   for i in range(5):
       edges = sampler.get()
       print(edges.src_ids)
       print(edges.src_ids)
       print(edges.weights)
       print(edges.float_attrs)
