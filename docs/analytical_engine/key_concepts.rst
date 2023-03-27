Key Concepts
--------

In this document, we introduce some important concepts in GraphScope analytical engine, which defines the
graph loading strategies, inner/outer vertices, and message passing and synchronization strategies.

LoadStrategy
^^^^^^^^^^^^

There are three ways to maintain the nodes crossing different fragments in GraphScope analytical engine.

OnlyOut
"""""""

Each fragment $F_{i}$ maintains “local” nodes v and a set of “mirrors” for nodes v' in other fragments such that there exists an edge (v, v'). For instance, in addition to local nodes {4, 5, 6, 7}, $F_{1}$ in graph G also stores a “mirror” node 3 and the edge (5, 3) when using OnlyOut strategy, as shown below.

.. image:: ../images/onlyout.png
  :alt: OnlyOut

OnlyIn 
""""""

Under this case, each fragment $F_{i}$ maintains “local” nodes v and a set of “mirrors” for nodes v' in other fragments such that there exists an edge (v', v). In graph G, $F_{1}$ maintains “mirror” nodes {1, 9, 12} besides its local nodes.

.. image:: ../images/onlyin.png
  :alt: OnlyIn

BothInOut
"""""""""

Each fragment $F_{i}$ maintains “local” nodes v and a set of “mirrors” for nodes v' in other fragments such that there exists an edge (v, v') or (v', v). Hence, in graph G, “mirror” nodes {1, 3, 9, 12} are stored in $F_{1}$ when BothInOut is applied.

.. image:: ../images/inandout.png
  :alt: BothInOut

PartitionStrategy
^^^^^^^^^^^^^^^^^

Edge Cut
""""""""

An *edge cut* partitioning splits vertices of a graph into roughly equal size clusters. The edges are stored in the same cluster as one or both of its endpoints. Edges with endpoints distributed across different clusters are *crossing edges*.

.. image:: ../images/ecut.png
  :alt: Edge Cut


Vertex Cut
""""""""""

A *vertex-cut* partitioning divides edges of a graph into roughly equal size fragments. The vertices that hold the endpoints of an edge are also placed in the same fragment as the edge itself. A vertex has to be replicated when its adjacent edges are distributed across different fragments.

.. image:: ../images/vcut.png
  :alt: Vertex Cut


Vertices on GraphScope analytical engine 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A node v is referred to as an

OuterVertex
"""""""""""

OuterVertex of fragment $F_{i}$ if it resides at another fragment $F_{j}$ and there exists a node v' in $F_{i}$ such that (v, v') or (v', v) is an edge; e.g., nodes {1, 3, 9, 12} are OuterVertex of fragment $F_{1}$ in graph G;

.. image:: ../images/outvertex.png
  :alt: OuterVertex

InnerVertex
"""""""""""

InnerVertex of fragment $F_{i}$ if it is distributed to $F_{i}$; e.g. nodes {4, 5, 6, 7} are InnerVertex of fragment $F_{1}$ in G;

.. image:: ../images/invertex.png
  :alt: InnerVertex 

InnerVertexWithOutgoingEdge
"""""""""""""""""""""""""""

InnerVertexWithOutgoingEdge of fragment $F_{i}$ if it is stored in $F_{i}$ and has an adjacent edge (v, v') outcoming to a node v' in another fragment $F_{j}$; e.g., node 5 is an InnerVertexWithOutgoingEdge of $F_{1}$ in G with the outgoing edge (5, 3);

.. image:: ../images/invertexout.png
  :alt: InnerVertexWithOutgoingEdge

InnerVertexWithIncomingEdge
"""""""""""""""""""""""""""

InnerVertexWithIncomingEdge of fragment $F_{i}$ if it is maintained in $F_{i}$ has an adjacent edge (v', v) incoming from a node v' in another fragment $F_{j}$; e.g., nodes {4, 5, 7} are InnerVertexWithIncomingEdge of $F_{1}$ in G, and (1, 4), (9, 5), and (12, 7) are corresponding incoming edges.

.. image:: ../images/invertexin.png
  :alt: InnerVertexWithIncomingEdge

MessageManager and MessageStrategy
^^^^^^^^^^^^^^^
In each graph application in GAE, a MessageManager is created to manage the messages passed between different fragments. Considering the diversity of graph applications, we provide many message passing strategies, which are defined in MessageStrategy.  

Below are message passing and synchronization strategies supported by GAE.  

AlongOutgoingEdgeToOuterVertex
""""""""""""""""""""""""""""""

Here the message is passed along crossing edges from InnerVertexWithOutgoingEdge to OuterVertex. For instance, the message is passed from node 5 to 3 in graph G.   

.. image:: ../images/intoout.png
  :alt: AlongOutgoingEdgeToOuterVertex

AlongIncomingEdgeToOuterVertex
""""""""""""""""""""""""""""""

Under this case, the message is passed along crossing edges from InnerVertexWithIncomingEdge to OuterVertex. For example, the message is passed from node 5 to 9 in graph G.   

.. image:: ../images/intoout2.png
  :alt: AlongIncomingEdgeToOuterVertex

AlongEdgeToOuterVertex
""""""""""""""""""""""

Each message is passed along crossing edges from nodes that are both  InnerVertexWithIncomingEdge and InnerVertexWithOutgoingEdge to OuterVertex, e.g., messages are passed from node 5 to 3 and 9 and vice versa in graph G.

.. image:: ../images/intoout3.png
  :alt: AlongEdgeToOuterVertex

SyncOnOuterVertexAsTarget
"""""""""""""""""""""""""

It is applied in company with the OnlyOut loading strategy. Here each fragment $F_{i}$ sends the states of its “mirror” node of OuterVertex v to $F_{j}$ that v resides, if there exists edge (v', v) and v' is “local” node of $F_{i}$, for synchronizing different states of v. For instance, the state of “mirror” node 3 is sent from $F_{1}$ to $F_{0}$ for synchronization at $F_{0}$. 

.. image:: ../images/sync1.png
  :alt: SyncOnOuterVertexAsTarget

SyncOnOuterVertexAsSource
"""""""""""""""""""""""""

It is applied together with the OnlyIn loading strategy. Similar to **SyncStateOnOuterVertexAsTarget**, each fragment $F_{i}$ sends the states of its “mirror” nodes v to the corresponding fragments for synchronization. The difference is that for each such “mirror”, there exists outgoing edge (v, v') to certain “local” node v' of $F_{i}$. For example, the states of “mirror” nodes 1, 9, and 12 are sent from $F_{1}$ to $F_{0}$ and $F_{2}$ for synchronization with other states.

.. image:: ../images/sync2.png
  :alt: SyncOnOuterVertexAsSource

SyncOnOuterVertex
"""""""""""""""""

This is applied together with the BothInOut loading strategy. Under this case, each fragment $F_{i}$ sends the states of all its “mirror” nodes v to the corresponding fragments for synchronization, regardless of the directions of edges adjacent to v, e.g., the states of “mirror” nodes 1, 3, 9 and 12 are sent from $F_{1}$ to $F_{0}$ and $F_{2}$ for further synchronization. 

.. image:: ../images/sync3.png
  :alt: SyncOnOuterVertex

