GraphScope Analytical Engine
============================

The analytical engine in GraphScope derives from 
`GRAPE <https://dl.acm.org/doi/10.1145/3282488>`_, 
a graph processing system proposed on SIGMOD-2017. 
GRAPE differs from prior systems in its ability
to parallelize sequential graph algorithms as a whole. In GRAPE,
sequential algorithms can be easily "plugged into"
with only minor changes and get parallelized to handle large graphs efficiently.
In addition to the ease of programming, GRAPE is designed to be
highly efficient and flexible, to cope the scale,
variety and complexity from real-life graph applications.

Built-in Algorithms
---------------------

GraphScope analytical engine provides many common used algorithms, 
including connectivity and path
analysis, community detection, centrality computations.

Built-in algorithms can be easily invoked over loaded graphs. For example, 

.. code:: python

    from graphscope import pagerank
    from graphscope import lpa

    # algorithms defined on property graph can be invoked directly.
    result = lpa(g)

    # some other algorithms may only support evaluate on simple graph
    # hence we need to generate one by selecting a kind of vertices and edges.
    simple_g = g.project(vertices={"users": []}, edges={"follows": []})
    result_pr = pagerank(simple_g)

A full-list of builtin algorithms is shown as below. Whether an algorithm supports
property graph or not is described in its docstring.

.. currentmodule:: graphscope

.. autosummary::

- :func:`bfs`
- :func:`cdlp`
- :func:`clustering`
- :func:`degree_centrality`
- :func:`eigenvector_centrality`
- :func:`hits`
- :func:`k_core`
- :func:`katz_centrality`
- :func:`lpa`
- :func:`pagerank`
- :func:`sssp`
- :func:`triangles`
- :func:`wcc`

The list is continuously growing.


Result Processing
---------------------

When finish a computation, the results are wrapped as :ref:`Context` 
and preserved on the distributed machines.

Users may want to fetch the results to the client, or write to cloud or distributed file systems.

There is a list of supported method to retrieve the results.

.. code:: python

    # fetch to data structures
    result_pr.to_numpy()
    result_pr.to_dataframe()

    # or write to hdfs or oss, or local (local to pod)
    result_pr.output("hdfs://output")
    result_pr.output("oss://id:key@endpoint/bucket/object")
    result_pr.output("file:///tmp/path")

    # or write to client local
    result_pr.output_to_client("local_filename")

    # or seal to vineyard
    result_pr.to_vineyard_dataframe()
    result_pr.to_vineyard_numpy()

In addition, as shown in the Getting_Started, computation results can add back to 
the graph as a new property (column) of the vertices(edges).

.. code:: python

    simple_g = g.project(vertices={"paper": []}, edges={"sites": []})
    ret = graphscope.kcore(simple_g, k=5)

    # add the results as new columns to the citation graph
    subgraph = sub_graph.add_column(ret, {'kcore': 'r'})


Users may assign a :ref:`Selector` to define which parts of the results to write. A selector specifies
which part of the result to preserve. Similarly, the graph data can
also be a part of the result, e.g., the vertex id. We reserve three keywords for selectors.
`r` represents the result, `v` and `e` for vertices and edges, respectively.
Here are some examples for selectors on result processing.

.. code:: python

    # get the results on the vertex
    result_pr.to_numpy('r')

    # to dataframe,
    # using the `id` of vertices (`v.id`) as a column named df_v
    # using the `data` of v (`v.data`) as a column named df_vd
    # and using the result (`r`) as a column named df_result
    result_pr.to_dataframe({'df_v': 'v.id', 'df_vd': 'v.data', 'df_result': 'r'})

    # for results on property graph
    # using `:` as a label selector for v and e
    # using the id for vertices labeled with label0 (`v:label0.id`) as column `id`
    # using the property0 written on vertices with label0 as column `result`
    result.output(fd='hdfs:///gs_data/output', \
            selector={'id': 'v:label0.id', 'result': 'r:label0.property0'})


See more details in :ref:`Context` and :ref:`Selector`.


Writing Your Own Algorithms in PIE
----------------------------------------------

Users may write their own algorithms if the built-in algorithms
do not meet their needs. `graphscope` enables users to write
algorithms in the `PIE <https://dl.acm.org/doi/10.1145/3282488>`_ 
programming model in a pure Python mode.

.. image:: images/pie.png
    :width: 600
    :align: center
    :alt: Workflow of PIE

To implement this, a user just need to fulfill this class.

.. code:: python

    @graphscope.analytical.udf.pie
    class YourAlgorithm(AppAssets):
        @staticmethod
        def Initialize(context, frag):
            pass

        @staticmethod
        def PEval(context, frag):
            pass

        @staticmethod
        def IncEval(context, frag):
            pass

As shown in the code, users need to implement a class decorated with
`@graphscope.analytical.udf.pie` and provides three sequential graph functions.
In the class, the `Initialize` is a function to set the initial status. `PEval` is
a sequential method for partial evaluation, and `IncEval` is a sequential function
for incremental evaluation over the partitioned fragment. The full API of fragment
can be found in :ref:`Cython SDK API`.

Let's take SSSP as example, a user defined SSSP in PIE model may be like this.

.. code:: python

    @graphscope.analytical.udf.pie
    class SSSP:
        @staticmethod
        def Initialize(context, frag):
            v_label_num = frag.vertex_label_num()
            # init every vertex with a max distance representing unreachable.
            for v_label_id in range(v_label_num):
                nodes = frag.nodes(v_label_id)
                context.init_value(nodes, v_label_id, 1000000000.0,
                               PIEAggregateType.kMinAggregate)
            context.register_sync_buffer(MessageStrategy.kSyncOnOuterVertex)

        @staticmethod
        def PEval(context, frag):
            # get the source node from the query, passed from context.
            src = int(context.get_config(b'src'))
            graphscope.declare(graphscope.Vertex, source)
            native_source = False
            v_label_num = frag.vertex_label_num()
            for v_label_id in range(v_label_num):
                if frag.get_inner_node(v_label_id, src, source):
                    native_source = True
                    break
            if native_source:
                context.set_node_value(source, 0)
            else:
                return

            # in the source fragment, run the dijkstra algorithm as partial evaluation.
            e_label_num = frag.edge_label_num()
            for e_label_id in range(e_label_num):
                edges = frag.get_outgoing_edges(source, e_label_id)
                for e in edges:
                    dst = e.neighbor()
                    distv = e.get_int(2)
                    if context.get_node_value(dst) > distv:
                        context.set_node_value(dst, distv)

        @staticmethod
        def IncEval(context, frag):
            v_label_num = frag.vertex_label_num()
            e_label_num = frag.edge_label_num()
            # incremental computation to update the distance.
            for v_label_id in range(v_label_num):
                iv = frag.inner_nodes(v_label_id)
                for v in iv:
                    v_dist = context.get_node_value(v)
                    for e_label_id in range(e_label_num):
                        es = frag.get_outgoing_edges(v, e_label_id)
                        for e in es:
                            u = e.neighbor()
                            u_dist = v_dist + e.get_int(2)
                            if context.get_node_value(u) > u_dist:
                                context.set_node_value(u, u_dist)


As shown in the code, users only need to design and implement sequential algorithm
over a fragment, rather than considering the communication and message passing
in the distributed setting. In this case, the classic dijkstra algorithm and its
incremental version works for large graphs partitioned on a cluster.

Writing Algorithms in Pregel
----------------------------------------------

In addition to the sub-graph based PIE model, 
`graphscope` supports vertex-centric
`Pregel` model as well. 
You may develop an algorithms in `Pregel` model by implementing this.

.. code:: python

    @pregel(vd_type='double', md_type='double')
    class YourPregelAlgorithm(AppAssets):

        @staticmethod
        def Init(v, context):
            pass

        @staticmethod
        def Compute(messages, v, context):
            pass

        @staticmethod
        def Combine(messages):
            pass

Differ from the PIE model, the decorator for this class is @graphscope.analytical.udf.pregel.
And the functions to be implemented is defined on vertex, rather than the fragment.
Take SSSP as example, the algorithm in Pregel model looks like this.

.. code:: python

    # decorator, and assign the types for vertex data, message data.
    @pregel(vd_type='double', md_type='double')
    class SSSP_Pregel(AppAssets):

        @staticmethod
        def Init(v, context):
            v.set_value(1000000000.0)

        @staticmethod
        def Compute(messages, v, context):
            src_id = context.get_config(b"src")
            cur_dist = v.value()
            new_dist = 1000000000.0
            if v.id() == src_id:
                new_dist = 0
            for message in messages:
                new_dist = min(message, new_dist)
            if new_dist < cur_dist:
                v.set_value(new_dist)
                for e_label_id in range(context.edge_label_num()):
                    edges = v.outgoing_edges(e_label_id)
                    for e in edges:
                        v.send(e.vertex(), new_dist + e.get_int(2))
            v.vote_to_halt()

        @staticmethod
        def Combine(messages):
            ret = 1000000000.0
            for m in messages:
                ret = min(ret, m)
            return ret

Using ``math.h`` Functions in Algorithms
----------------------------------------

GraphScope supports using C functions from :code:`math.h` in user-defined algorithms,
via the :code:`context.math` interface. E.g.,

.. code:: python

    @staticmethod
    def Init(v, context):
        v.set_value(context.math.sin(1000000000.0 * context.math.M_PI))

will be translated to the following efficient C code

.. code:: c

    ... Init(...)

        v.set_value(sin(1000000000.0 * M_PI));

Run Your Own Algorithms
-------------------------

To run your own algorithms, you may trigger it in place where you defined it.

.. code:: python

    import graphscope

    sess = graphscope.session()
    g = sess.g()

    # load my algorithm
    my_app = SSSP_Pregel()

    # run my algorithm over a graph and get the result.
    ret = my_app(g, source="0")


After developing and testing, you may want to save it for the future use.

.. code:: python

    SSSP_Pregel.to_gar("file:///var/graphscope/udf/my_sssp_pregel.gar")


Later, you can load your own algorithm from the gar package.

.. code:: python

    import graphscope

    sess = graphscope.session()
    g = graphscope.Session(sess)

    # load my algorithm from a gar package
    my_app = load_app('SSSP_Pregel', 'file:///var/graphscope/udf/my_sssp_pregel.gar')

    # run my algorithm over a graph and get the result.
    ret = my_app(g, src="0")

**Publications**

- Wenfei Fan, Jingbo Xu, Wenyuan Yu, Jingren Zhou, Xiaojian Luo, Ping Lu, Qiang Yin, Yang Cao, and Ruiqi Xu. `Parallelizing Sequential Graph Computations. <https://dl.acm.org/doi/10.1145/3282488>`_, ACM Transactions on Database Systems (TODS) 43(4): 18:1-18:39.

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang. `GRAPE: Parallelizing Sequential Graph Computations. <http://www.vldb.org/pvldb/vol10/p1889-fan.pdf>`_, The 43rd International Conference on Very Large Data Bases (VLDB), demo, 2017 (the Best Demo Award).

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang, Zeyu Zheng, Bohan Zhang, Yang Cao, and Chao Tian. `Parallelizing Sequential Graph Computations. <https://dl.acm.org/doi/10.1145/3035918.3035942>`_, ACM SIG Conference on Management of Data (SIGMOD), 2017 (the Best Paper Award).


Glossory
--------

LoadStrategy
^^^^^^^^^^^^

There are three ways to maintain the nodes crossing different fragments in GraphScope analytical engine.

OnlyOut
"""""""

Each fragment Fi maintains “local” nodes v and a set of “mirrors” for nodes v' in other fragments such that there exists an edge (v, v'). For instance, in addition to local nodes {4, 5, 6, 7}, F1 in graph G also stores a “mirror” node 3 and the edge (5, 3) when using OnlyOut strategy, as shown below.

.. image:: images/onlyout.png
  :alt: OnlyOut

OnlyIn 
""""""

Under this case, each fragment Fi maintains “local” nodes v and a set of “mirrors” for nodes v' in other fragments such that there exists an edge (v', v). In graph G, F1 maintains “mirror” nodes {1, 9, 12} besides its local nodes.

.. image:: images/onlyin.png
  :alt: OnlyIn

BothInOut
"""""""""

Each fragment Fi maintains “local” nodes v and a set of “mirrors” for nodes v' in other fragments such that there exists an edge (v, v') or (v', v). Hence, in graph G, “mirror” nodes {1, 3, 9, 12} are stored in F1 when BothInOut is applied.

.. image:: images/inandout.png
  :alt: BothInOut

PartitionStrategy
^^^^^^^^^^^^^^^^^

Edge Cut
""""""""

An *edge cut* partitioning splits vertices of a graph into roughly equal size clusters. The edges are stored in the same cluster as one or both of its endpoints. Edges with endpoints distributed across different clusters are *crossing edges*.

.. image:: images/ecut.png
  :alt: Edge Cut


Vertex Cut
""""""""""

A *vertex-cut* partitioning divides edges of a graph into roughly equal size fragments. The vertices that hold the endpoints of an edge are also placed in the same fragment as the edge itself. A vertex has to be replicated when its adjacent edges are distributed across different fragments.

.. image:: images/vcut.png
  :alt: Vertex Cut


Vertices on GraphScope analytical engine 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A node v is referred to as an

OuterVertex
"""""""""""

OuterVertex of fragment Fi if it resides at another fragment Fj and there exists a node v' in Fi such that (v, v') or (v', v) is an edge; e.g., nodes {1, 3, 9, 12} are OuterVertex of fragment F1 in graph G;

.. image:: images/outvertex.png
  :alt: OuterVertex

InnerVertex
"""""""""""

InnerVertex of fragment Fi if it is distributed to Fi; e.g. nodes {4, 5, 6, 7} are InnerVertex of fragment F1 in G;

.. image:: images/invertex.png
  :alt: InnerVertex 

InnerVertexWithOutgoingEdge
"""""""""""""""""""""""""""

InnerVertexWithOutgoingEdge of fragment Fi if it is stored in Fi and has an adjacent edge (v, v') outcoming to a node v' in another fragment Fj; e.g., node 5 is an InnerVertexWithOutgoingEdge of F1 in G with the outgoing edge (5, 3);

.. image:: images/invertexout.png
  :alt: InnerVertexWithOutgoingEdge

InnerVertexWithIncomingEdge
"""""""""""""""""""""""""""

InnerVertexWithIncomingEdge of fragment Fi if it is maintained in Fi has an adjacent edge (v', v) incoming from a node v' in another fragment Fj; e.g., nodes {4, 5, 7} are InnerVertexWithIncomingEdge of F1 in G, and (1, 4), (9, 5), and (12, 7) are corresponding incoming edges.

.. image:: images/invertexin.png
  :alt: InnerVertexWithIncomingEdge

MessageStrategy
^^^^^^^^^^^^^^^

Below are some message passing and synchronization strategies adopted by GraphScope analytical engine.  

AlongOutgoingEdgeToOuterVertex
""""""""""""""""""""""""""""""

Here the message is passed along crossing edges from InnerVertexWithOutgoingEdge to OuterVertex. For instance, the message is passed from node 5 to 3 in graph G.   

.. image:: images/intoout.png
  :alt: AlongOutgoingEdgeToOuterVertex

AlongIncomingEdgeToOuterVertex
""""""""""""""""""""""""""""""

Under this case, the message is passed along crossing edges from InnerVertexWithIncomingEdge to OuterVertex. For example, the message is passed from node 5 to 9 in graph G.   

.. image:: images/intoout2.png
  :alt: AlongIncomingEdgeToOuterVertex

AlongEdgeToOuterVertex
""""""""""""""""""""""

Each message is passed along crossing edges from nodes that are both  InnerVertexWithIncomingEdge and InnerVertexWithOutgoingEdge to OuterVertex, e.g., messages are passed from node 5 to 3 and 9 and vice versa in graph G.

.. image:: images/intoout3.png
  :alt: AlongEdgeToOuterVertex

SyncOnOuterVertexAsTarget
"""""""""""""""""""""""""

It is applied in company with the OnlyOut loading strategy. Here each fragment Fi sends the states of its “mirror” node of OuterVertex v to Fj that v resides, if there exists edge (v', v) and v' is “local” node of Fi, for synchronizing different states of v. For instance, the state of “mirror” node 3 is sent from F1 to F0 for synchronization at F0. 

.. image:: images/sync1.png
  :alt: SyncOnOuterVertexAsTarget

SyncOnOuterVertexAsSource
"""""""""""""""""""""""""

It is applied together with the OnlyIn loading strategy. Similar to **SyncStateOnOuterVertexAsTarget**, each fragment Fi sends the states of its “mirror” nodes v to the corresponding fragments for synchronization. The difference is that for each such “mirror”, there exists outgoing edge (v, v') to certain “local” node v' of Fi. For example, the states of “mirror” nodes 1, 9, and 12 are sent from F1 to F0 and F2 for synchronization with other states.

.. image:: images/sync2.png
  :alt: SyncOnOuterVertexAsSource

SyncOnOuterVertex
"""""""""""""""""

This is applied together with the BothInOut loading strategy. Under this case, each fragment Fi sends the states of all its “mirror” nodes v to the corresponding fragments for synchronization, regardless of the directions of edges adjacent to v, e.g., the states of “mirror” nodes 1, 3, 9 and 12 are sent from F1 to F0 and F2 for further synchronization. 

.. image:: images/sync3.png
  :alt: SyncOnOuterVertex


