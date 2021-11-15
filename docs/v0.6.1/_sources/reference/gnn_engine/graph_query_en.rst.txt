Graph Query
===========

After constructing the graph object, we can query on the graph. Graph
query refers to getting **meta information** and **data information**
without complex calculation and sampling logic.

# 1 Meta Query Meta infomation refers to the graph structure and
statistical data, including graph topology, number of vertices,
distribution of edges and vertices, maximum in-and-out degrees and etc.

## 1.1 Graph Topology

.. code:: python

   def get_topology()
   """ Get the topology of the graph
   The return type is dict, the key is edge_type and the value consists of src_type and dst_type
   """

The following code block retrieves the topology and the return format of
the heterogeneous graph shown in the following image.

.. container::

   Figure1. Graph topology

.. code:: python

   g = Graph(...)
   g.init(...)
   topo = g.get_topology()
   topo.print_all()

   """
   egde_type:buy, src_type:user, dst_type:item
   egde_type:click, src_type:user, dst_type:item
   egde_type:swing, src_type:item, dst_type:item
   """

## 1.2 In-and-Out Degree Distribution Coming soon…

#2 Data Query

**GL** has two basic data types: ``Nodes`` and ``Edges``. Graph
raversal, query, and sampling operations all return a batch of vertices
or edges. In particular, non-aligned sampling returns the sparse form of
two basic data types, namely ``SparseNodes`` and ``SparseEdges``. The
interface of ``Nodes`` is shown as follows.

.. code:: python

   @property
   def ids(self):
   """ vertex id，numpy.ndarray(int64) """

   @property
   def shape(self):
   """ the shape of vertex id """

   @property
   def int_attrs(self):
   """ int attributes，numpy.ndarray(int64)，shape is [ids.shape, the number of int attributes] """

   @property
   def float_attrs(self):
   """ float attributes，numpy.ndarray(float32)，shape is [ids.shape, the number of float attributes] """

   @property
   def string_attrs(self):
   """ string attributes，numpy.ndarray(string)，shape is [ids.shape, the number of string attributes] """

   @property
   def weights(self):
   """ weight，numpy.ndarray(float32)，shape is ids.shape """

   @property
   def labels(self):
   """ lable，numpy.ndarray(int32)，shape is ids.shape """

Compared to the ``Nodes`` interface, ``Edges`` does not have ``ids``
interface. Instead, it has following four additional interfaces to
access the source and destination vertices.

.. code:: python

   @property
   def src_nodes(self):
   """ source vertices """

   @property
   def dst_nodes(self):
   """ destination vertices """

   @property
   def src_ids(self):
   """ ids of source vertices，numpy.ndarray(int64) """

   @property
   def dst_ids(self):
   """ ids of destination vertices，numpy.ndarray(int64) """

When we traverse vertices and edges, the shape of ``ids`` is
one-dimensional and the size is the specified batch size. However, the
shape of ``ids`` in sampling operation is two-dimensional, and the size
is ``[the size of input data, current sample size]``. We use
``SparseNodes`` to represent the sparse neighbor vertices of a vertex.
In addition to the interfaces of ``Nodes``, ``SparseNodes`` has
additional interfaces as follows.

.. code:: python

   @property
   def offsets(self):
   """ one dimentional int array: the number of neighbors of each vertex """

   @property
   def dense_shape(self):
   """ tuple of size 2: the shape of the corresponding Dense Nodes """

   @property
   def indices(self):
   """ two dimentional arra: the location of each neighbor """

   def __next__(self):
   """ iterator: iterate over the neighbors of each vertex """
     return Nodes

\ ``SparseEdges`` is used to express sparse edges of vertices. In
addition to the interfaces of ``Edges``, ``SparseEdges`` has additional
interfaces as follows.

.. code:: python

   @property
   def offsets(self):
   """ one dimential int array: the number of neighbors of each vertex """

   @property
   def dense_shape(self):
   """ tuple of size 2: the shape of the corresponding Dense Edges """

   @property
   def indices(self):
   """ two dimentional array: the location of each neighbor """

   def __next__(self):
   """ iterator: iterate the neighbors of each vertex """
     return Edges

## 2.1 Vertices Query We can get Nodes by traversing the graph, sampling
or specifying the node id. Once we get the nodes, we can query their
attributes, weights or lables. Query vertices with specified ids:
``python def get_nodes(node_type, ids) ''' get the weights, lables and attributes by node type Args:   node_type(string): vertex type   ids(numpy.array): vertex id Return:   Nodes object '''``

We use the following example to show the interface and usage
``get_nodes()``.

Table1: user vertices

===== ==========
id    attributes
===== ==========
10001 0:0.1:0
10002 1:0.2:3
10003 3:0.3:4
===== ==========

.. code:: python

   g = Graph(...)
   u_nodes = g.get_nodes("user", np.array([10001, 10002, 10003]))

   print(u_nodes.int_attrs) # shape = [3, 2]
   # array([[0, 0], [1, 3], [2, 4]])

   print(u_nodes.float_attrs) # shape = [3, 1]
   # array([[ 0.1],  [0.2],  [0.3]])

In GSL，we can use ``V()`` to replace ``get_nodes()``

.. code:: python

   g = Graph(...)
   u_nodes = g.V("user", feed=np.array([10001, 10002, 10003])).emit()

## 2.2 Edges Query We can get edges from graph travesal, sampling or
specifying src_id and dst_id. Once we get the edges, we can query their
attributes, weights or labels. edges query by specifying src_id, dst_id:

.. code:: python

   def get_edges(node_type, src_ids, dst_ids)
   ''' get the edge weight, labels, and attributes by node type, src_ids and dst_ids.
   Args:
     node_type(string): edges type
     src_ids(numpy.array): id of source vertex
     dst_ids(numpy.array): id of destination vertex
   Return:
     Edges object
   '''

We use the following example to show the inferface and usage
``get_edges()``

Table2: swing edges

====== ====== ======
src_id dst_id weight
====== ====== ======
10001  10002  0.1
10002  10001  0.2
10003  10002  0.3
10004  10003  0.4
====== ====== ======

Table3: click edges data

====== ====== ====== =================================================
src_id dst_id weight attributes
====== ====== ====== =================================================
20001  30001  0.1    0.10,0.11,0.12,0.13,0.14,0.15,0.16,0.17,0.18,0.19
20001  30003  0.2    0.20,0.21,0.22,0.23,0.24,0.25,0.26,0.27,0.28,0.29
20003  30001  0.3    0.30,0.31,0.32,0.33,0.34,0.35,0.36,0.37,0.38,0.39
20004  30002  0.4    0.40,0.41,0.42,0.43,0.44,0.45,0.46,0.47,0.48,0.49
====== ====== ====== =================================================

.. code:: python

   g = Graph(...)

   edges = g.get_edges(edge_type="swing", 
                       src_ids=np.array([10001, 10002, 10003]), 
                       dst_ids=np.array([10002, 10001, 10002]))

   print(edges.weights)  # shape=[3]
   # array([0.1,  0.2,  0.3])

   click_edges = g.get_edges(edge_type="click", 
                             src_ids=np.array([20001, 20003, 20004]), 
                             dst_ids=np.array([30003, 30001, 30002]))

   print(click_edges.weights)  # shape=[3]
   # array([0.2,  0.3,  0.4])

In GSL，we can use ``E()`` to replace ``get_edges()`` as follows.

.. code:: python

   g = Graph(...)
   edges = g.E("swing",
               feed=(np.array([10001, 10002, 10003]), np.array([10002, 10001, 10002])) \
            .emit()

## 2.3 Sparse Vertices and Edges Query We can get Nodes or Edges object
from graph traversal or sampling, and query them by interfaces
introduced in 2.1 and 2.2.The results of non-aligned sampling are often
sparse. For instance, the full neighbor sampling returns non-aligned
results because vertices have different numbers of neighbors. In the
following example of using the edge attribute query of full neighbor
sampling, we illustrate the interfaces and the usage of sparse objects.

Table4: buy edges

==== ==== ======
user item weight
==== ==== ======
1    3    0.2
1    0    0.1
1    2    0.0
2    1    0.1
4    1    0.5
4    2    0.3
==== ==== ======

.. code:: python

   # sampling the 'buy' edges of source nodes ids 1, 2, 3, 4
   res = g.V("user", feed=np.array([1, 2, 3, 4])).outE("buy").sample().by("full").emit()

   # res[0] # Nodes of [1, 2, 3, 4]
   # res[1] # SparseEdges

   res[1].src_ids 
   # array([1, 1, 1, 2, 4, 4])

   res[1].dst_ids 
   # array([3, 0, 2, 1, 1, 2])

   res[1].offsets 
   # [3, 1, 0, 2]
   # the numbers of neighbors of user 1, 2, 3, and 4 are 3, 1, 0, 2 respectively.

   res[1].dense_shape
   # [4, 3]
   # 4 is the number of the source node, 3 is the maximum number of neighbors. 

   res[1].indices
   # [[0, 1], [0, 2], [0, 3], [1, 0], [3, 1], [3, 2]]
   # the corresponding index of source ids in dense Nodes which is similar to the index of destination ids in dense Nodes.
   # the corresponding dst dense Nodes：
   # [[ 3,  0,  2],
   #  [ 1, -1, -1],
   #  [-1, -1, -1],
   #  [ 1,  2, -1]]

   res[1].weights
   # [0.2, 0.1, 0.0, 0.1, 0.5, 0.3]

   # traverse all adjacent edges of each vertex.
   iterate = 0
   for edges in res[1]:
       print("Iterate {}:".format(iterate), edges.dst_ids, edges.weights)
       iterate += 1
   # Iterate 0: [3, 0, 2], [0.2, 0.1, 0.0], [[3, 1, 2]]
   # Iterate 1: [1], [0.1],
   # Iterate 2: [], []
   # Iterate 3: [1, 2], [0.5, 0.3]
