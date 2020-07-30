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

.. code:: ipython

    from graphscope import pagerank
    from graphscope import lpa

    # algorithms defined on property graph can be invoked directly.
    result = lpa(g)

    # some other algorithms may only support evaluate on simple graph
    # hence we need to generate one by selecting a kind of vertices and edges.
    simple_g = g.project_to_simple(v_label="users", e_label="follows")
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

.. code:: iPython

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

.. code:: iPython

    simple_g = sub_graph.project_to_simple(vlabel="paper", elabel="cites")
    ret = graphscope.kcore(simple_g, k=5)

    # add the results as new columns to the citation graph
    subgraph = sub_graph.add_column(ret, {'kcore': 'r'})


Users may assign a :ref:`Selector` to define which parts of the results to write. A selector specifies
which part of the result to preserve. Similarly, the graph data can
also be a part of the result, e.g., the vertex id. We reserve three keywords for selectors.
`r` represents the result, `v` and `e` for vertices and edges, respectively.
Here are some examples for selectors on result processing.

.. code:: iPython

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

.. code:: ipython

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

.. code:: ipython

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

.. code:: ipython

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

.. code:: ipython

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


Run Your Own Algorithms
-------------------------

To run your own algorithms, you may trigger it in place where you defined it.

.. code:: ipython

    import graphscope

    sess = graphscope.session()
    g = sess.load_from("...")

    # load my algorithm
    my_app = SSSP_Pregel()

    # run my algorithm over a graph and get the result.
    ret = my_app(g, source="0")


After developing and testing, you may want to save it for the future use.

.. code:: ipython

    SSSP_Pregel.to_gar("file:///var/graphscope/udf/my_sssp_pregel.gar")


Later, you can load your own algorithm from the gar package.

.. code:: ipython

    import graphscope

    sess = graphscope.session()
    g = sess.load_from("...")

    # load my algorithm from a gar package
    my_app = load_app('SSSP_Pregel', 'file:///var/graphscope/udf/my_sssp_pregel.gar')

    # run my algorithm over a graph and get the result.
    ret = my_app(g, src="0")


**Publications**

- Wenfei Fan, Jingbo Xu, Wenyuan Yu, Jingren Zhou, Xiaojian Luo, Ping Lu, Qiang Yin, Yang Cao, and Ruiqi Xu. `Parallelizing Sequential Graph Computations. <https://dl.acm.org/doi/10.1145/3282488>`_, ACM Transactions on Database Systems (TODS) 43(4): 18:1-18:39.

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang. `GRAPE: Parallelizing Sequential Graph Computations. <http://www.vldb.org/pvldb/vol10/p1889-fan.pdf>`_, The 43rd International Conference on Very Large Data Bases (VLDB), demo, 2017 (the Best Demo Award).

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang, Zeyu Zheng, Bohan Zhang, Yang Cao, and Chao Tian. `Parallelizing Sequential Graph Computations. <https://dl.acm.org/doi/10.1145/3035918.3035942>`_, ACM SIG Conference on Management of Data (SIGMOD), 2017 (the Best Paper Award).
