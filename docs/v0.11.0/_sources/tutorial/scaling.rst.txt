Scaling and repartitioning
==========================

Different algorithms or workloads on a graph requires different kinds of hardware
resources (e.g., *CPU*, *memory*, *IO*). During a graph analytic pipeline a graph might
be used to run more than one algorithms. To improve both the resource utilization
as well as the efficiency, scaling in or out and repartition the given graph will be
needed in the whole pipeline.

`pygrape` enables users running a *repartition* algorithm on a given graph:

.. code:: python

    g = ...  # create a graph

    # repartition the graph to run an algorithm
    g1 = g.repartition(strategy='fennel', vertex_balance=4)
    r1 = sssp(g1)

    # repartition the graph to a more suitable structure for another algorithm
    g2 = g1.repartition(strategy='fennel', batch_size=200)
    r2 = sssp(g2)

For more details about how to repartition graphs using other kinds of strategies, see
:meth:`graphscope.Graph.repartition`. All supported repartition strategies are listed as
follows:

.. contents::
    :local:

Fennel
------

The *Fennel* graph partitioning algorithms is ....

XtraPuLP
--------

The *XtraPuLP* graph partitioning algorithms is ....

Hybrid partitioning
-------------------

To combine the advantages of both *VCut* and *ECut* partition strategy and adaptive
to different kinds of algorithms automatically, a hybrid partition strategy is also
supported in GRAPE engine.
