Applications and algorithms
===========================

.. _bound_app:

Bind application to graph
-------------------------

Graph data usually doesn't has fixed data type. For example, road networks may have edges
of integral or floating point data types. A shortest path algorithm should work for both
kinds of graph data and we usually won't be willing to copy-and-paste the algorithm code
for just a minor difference of data type to adapt the algorithm to a new graph.

The *GRAPE* engine is written in C++, its static type system has a huge positive implication
for performance. At the same time, it means an algorithm with fixed data type cannot be
used on graphs with another data type. That means, our algorithm should be *polymorphic*
and to be instantized when data type of given graph workload is known, aka. when *binding*
the applciation to a graph.

In `pygrape`, an algorithm needs to be bound to a graph first before executing queries.
Then the bound application can be used to run different queries on the graph for multiple
times. When it meets a graph with unseen data type, a new instantiation will be generated
during the re-binding process and results a new bound application, which can be used on
graphs of the new data type.

Take the built-in algorithm :code:`SSSP` (aka. single source shortest path algorithm) as
an example. We frist create a session to connects to the *GRAPE* engine:

.. code:: ipython

    >>> import graphscope
    >>> sess = graphscope.Session()

Then create a graph with :code:`double` type edges and apply the :code:`SSSP` algorithm
on that:

.. code:: ipython

    >>> g1 = graphscope.from_numpy(np.array([[1, 2, 3],
                                        [3, 4, 5]]),
                              edge_data=np.array([6, 7, 8]))
    >>> bind_sssp1 = graphscope.sssp(g1)
    >>> r1 = bind_sssp1(src=1)
    >>> sess.run(r1)
    >>>
    >>> r2 = bind_sssp1(src=2)
    >>> sess.run(r2)

The data type of edge data is :code:`int64_t`. You will notice a delay when running
:code:`sess.run(r1)` but not when running :code:`sess.run(r2)`. When the first time
a bound applciation is used, a C++ compilation process will be triggerred behind the
screen. The next time the same bound application is applied, the compilation result
will be resulted.

Now we apply the :code:`SSSP` algorithm on another graph with different data type:

.. code:: ipython

    >>> g2 = graphscope.from_numpy(np.array([[1, 2, 3],
                                        [3, 4, 5]]),
                              edge_data=np.array([6.0, 7.0, 8.0]))
    >>> bind_sssp2 = graphscope.sssp(g2)
    >>> r3 = bind_sssp2(src=1)
    >>> sess.run(r3)
    >>>
    >>> r4 = bind_sssp2(src=2)
    >>> sess.run(r4)

The edge type of graph is :code:`double`, the same application :code:`SSSP` will be
bound to the new graph automatically without copy-and-paste the code and the bind
and compilation process above happens again.

Built-in algorithms
-------------------
