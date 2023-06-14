
Tutorial: Develop algorithms in Python
============================

Users may write their own algorithms if the built-in algorithms
do not meet their needs. `graphscope` enables users to write
algorithms both in the `PIE <https://dl.acm.org/doi/10.1145/3282488>`_ 
and in Pregel programming models in a pure Python manner.


Writing Your Own Algorithms in PIE
----------------------------------------------

To implement an algorithm in PIE model, a user just need to fulfill this class.

.. code:: python

    from graphscope.analytical.udf.decorators import pie
    from graphscope.framework.app import AppAssets

    @pie(vd_type="double", md_type="double")
    class YourAlgorithm(AppAssets):
        @staticmethod
        def Init(frag, context):
            pass

        @staticmethod
        def PEval(frag, context):
            pass

        @staticmethod
        def IncEval(frag, context):
            pass

As shown in the code, users need to implement a class decorated with
`@pie` and provides three sequential graph functions.
In the class, the `Init` is a function to set the initial status. `PEval` is
a sequential method for partial evaluation, and `IncEval` is a sequential function
for incremental evaluation over the partitioned fragment. The full API of fragment
can be found in :ref:`Cython SDK API`.

Let's take SSSP as example, a user defined SSSP in PIE model may be like this.

.. code:: python

    from graphscope.analytical.udf.decorators import pie
    from graphscope.framework.app import AppAssets

    @pie(vd_type="double", md_type="double")
    class SSSP_PIE(AppAssets):
        @staticmethod
        def Init(frag, context):
            v_label_num = frag.vertex_label_num()
            for v_label_id in range(v_label_num):
                nodes = frag.nodes(v_label_id)
                context.init_value(
                    nodes, v_label_id, 1000000000.0, PIEAggregateType.kMinAggregate
                )
                context.register_sync_buffer(v_label_id, MessageStrategy.kSyncOnOuterVertex)

        @staticmethod
        def PEval(frag, context):
            src = int(context.get_config(b"src"))
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
            e_label_num = frag.edge_label_num()
            for e_label_id in range(e_label_num):
                edges = frag.get_outgoing_edges(source, e_label_id)
                for e in edges:
                    dst = e.neighbor()
                    # use the third column of edge data as the distance between two vertices
                    distv = e.get_int(2)
                    if context.get_node_value(dst) > distv:
                        context.set_node_value(dst, distv)

        @staticmethod
        def IncEval(frag, context):
            v_label_num = frag.vertex_label_num()
            e_label_num = frag.edge_label_num()
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

In addition to the sub-graph based PIE model, `graphscope` supports vertex-centric
`Pregel` model as well. You may develop an algorithms in `Pregel` model by implementing this.

.. code:: python

    from graphscope.analytical.udf.decorators import pregel
    from graphscope.framework.app import AppAssets

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

Differ from the PIE model, the decorator for this class is ``@pregel``.
And the functions to be implemented is defined on vertex, rather than the fragment.
Take SSSP as example, the algorithm in Pregel model looks like this.

.. code:: python

    from graphscope.analytical.udf import pregel
    from graphscope.framework.app import AppAssets

    # decorator, and assign the types for vertex data, message data.
    @pregel(vd_type="double", md_type="double")
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
    from graphscope.dataset import load_p2p_network

    g = load_p2p_network(generate_eid=False)

    # load my algorithm
    my_app = SSSP_Pregel()

    # run my algorithm over a graph and get the result.
    # Here the `src` is correspondent to the `context.get_config(b"src")`
    ret = my_app(g, src="6")


After developing and testing, you may want to save it for the future use.

.. code:: python

    SSSP_Pregel.to_gar("/tmp/my_sssp_pregel.gar")

Later, you can load your own algorithm from the gar package.

.. code:: python

    from graphscope.framework.app import load_app

    # load my algorithm from a gar package
    my_app = load_app("/tmp/my_sssp_pregel.gar")

    # run my algorithm over a graph and get the result.
    ret = my_app(g, src="6")
