#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file digraph.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/classes/digraph.py
#
# which has the following license:
#
# Copyright (C) 2004-2020, NetworkX Developers
# Aric Hagberg <hagberg@lanl.gov>
# Dan Schult <dschult@colgate.edu>
# Pieter Swart <swart@lanl.gov>
# All rights reserved.
#
# This file is part of NetworkX.
#
# NetworkX is distributed under a BSD license; see LICENSE.txt for more
# information.
#

from copy import deepcopy

from networkx import freeze
from networkx.classes.coreviews import AdjacencyView
from networkx.classes.digraph import DiGraph as RefDiGraph
from networkx.classes.reportviews import DiDegreeView
from networkx.classes.reportviews import InDegreeView
from networkx.classes.reportviews import InEdgeView
from networkx.classes.reportviews import OutDegreeView
from networkx.classes.reportviews import OutEdgeView

from graphscope.framework import dag_utils
from graphscope.framework.dag_utils import copy_graph
from graphscope.framework.errors import check_argument
from graphscope.framework.graph_schema import GraphSchema
from graphscope.nx import NetworkXError
from graphscope.nx.classes.graph import Graph
from graphscope.nx.convert import to_nx_graph
from graphscope.nx.utils.compat import patch_docstring
from graphscope.nx.utils.misc import check_node_is_legal
from graphscope.nx.utils.misc import empty_graph_in_engine
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2


class DiGraph(Graph):
    """
    Base class for directed graphs in graphscope.nx.

    A DiGraph that hold the metadata of a graph, and provide NetworkX-like DiGraph APIs.

    It is worth noticing that the graph is actually stored by the Analytical Engine backend.
    In other words, the graph object holds nothing but metadata of a graph

    DiGraph support nodes and edges with optional data, or attributes.

    DiGraphs support directed edges.  Self loops are allowed but multiple
    (parallel) edges are not.

    Nodes can be some hashable objects including int/str/float/tuple/bool object
    with optional key/value attributes.

    Edges are represented as links between nodes with optional
    key/value attributes.

    See Also
    --------
    Graph

    Examples
    --------
    Create an empty graph structure (a "null graph") with no nodes and
    no edges.

    >>> G = nx.DiGraph()

    G can be grown in several ways.

    **Nodes:**

    Add one node at a time:

    >>> G.add_node(1)

    Add the nodes from any container (a list, dict, set or
    even the lines from a file or the nodes from another graph).

    >>> G.add_nodes_from([2, 3])
    >>> G.add_nodes_from(range(100, 110))
    >>> H = nx.path_graph(10)
    >>> G.add_nodes_from(H)

    In addition integers, strings can represent a node.

    >>> G.add_node('a node')

    **Edges:**

    G can also be grown by adding edges.

    Add one edge,

    >>> G.add_edge(1, 2)

    a list of edges,

    >>> G.add_edges_from([(1, 2), (1, 3)])

    or a collection of edges,

    >>> G.add_edges_from(H.edges)

    If some edges connect nodes not yet in the graph, the nodes
    are added automatically.  There are no errors when adding
    nodes or edges that already exist.

    **Attributes:**

    Each graph, node, and edge can hold key/value attribute pairs
    in an associated attribute dictionary (the keys must be hashable).
    By default these are empty, but can be added or changed using
    add_edge, add_node or direct manipulation of the attribute
    dictionaries named graph, node and edge respectively.

    >>> G = nx.DiGraph(day="Friday")
    >>> G.graph
    {'day': 'Friday'}

    Add node attributes using add_node(), add_nodes_from() or G.nodes

    >>> G.add_node(1, time='5pm')
    >>> G.add_nodes_from([3], time='2pm')
    >>> G.nodes[1]
    {'time': '5pm'}
    >>> G.nodes[1]['room'] = 714
    >>> del G.nodes[1]['room'] # remove attribute
    >>> list(G.nodes(data=True))
    [(1, {'time': '5pm'}), (3, {'time': '2pm'})]

    Add edge attributes using add_edge(), add_edges_from(), subscript
    notation, or G.edges.

    >>> G.add_edge(1, 2, weight=4.7 )
    >>> G.add_edges_from([(3, 4), (4, 5)], color='red')
    >>> G.add_edges_from([(1, 2, {'color':'blue'}), (2, 3, {'weight':8})])
    >>> G[1][2]['weight'] = 4.7
    >>> G.edges[1, 2]['weight'] = 4

    Warning: we protect the graph data structure by making `G.edges[1, 2]` a
    read-only dict-like structure. However, you can assign to attributes
    in e.g. `G.edges[1, 2]`. Thus, use 2 sets of brackets to add/change
    data attributes: `G.edges[1, 2]['weight'] = 4`
    (For multigraphs: `MG.edges[u, v, key][name] = value`).

    **Shortcuts:**

    Many common graph features allow python syntax to speed reporting.

    >>> 1 in G     # check if node in graph
    True
    >>> [n for n in G if n < 3]  # iterate through nodes
    [1, 2]
    >>> len(G)  # number of nodes in graph
    5

    Often the best way to traverse all edges of a graph is via the neighbors.
    The neighbors are reported as an adjacency-dict `G.adj` or `G.adjacency()`

    >>> for n, nbrsdict in G.adjacency():
    ...     for nbr, eattr in nbrsdict.items():
    ...        if 'weight' in eattr:
    ...            # Do something useful with the edges
    ...            pass

    But the edges reporting object is often more convenient:

    >>> for u, v, weight in G.edges(data='weight'):
    ...     if weight is not None:
    ...         # Do something useful with the edges
    ...         pass

    **Reporting:**

    Simple graph information is obtained using object-attributes and methods.
    Reporting usually provides views instead of containers to reduce memory
    usage. The views update as the graph is updated similarly to dict-views.
    The objects `nodes, `edges` and `adj` provide access to data attributes
    via lookup (e.g. `nodes[n], `edges[u, v]`, `adj[u][v]`) and iteration
    (e.g. `nodes.items()`, `nodes.data('color')`,
    `nodes.data('color', default='blue')` and similarly for `edges`)
    Views exist for `nodes`, `edges`, `neighbors()`/`adj` and `degree`.

    For details on these and other miscellaneous methods, see below.
    """

    @patch_docstring(Graph.__init__)
    def __init__(self, incoming_graph_data=None, default_label=None, **attr):
        if self._session is None:
            self._try_to_get_default_session()

        self.graph_attr_dict_factory = self.graph_attr_dict_factory
        self.node_dict_factory = self.node_dict_factory
        self.adjlist_dict_factory = self.adjlist_dict_factory

        self.graph = self.graph_attr_dict_factory()
        self._node = self.node_dict_factory(self)
        self._adj = self.adjlist_dict_factory(self)
        self._pred = self.adjlist_dict_factory(self, types_pb2.PREDS_BY_NODE)
        self._succ = self._adj

        self._key = None
        self._op = None
        self._session_id = None
        self._graph_type = self._graph_type
        self._schema = GraphSchema()
        self._schema.init_nx_schema()

        create_empty_in_engine = attr.pop(
            "create_empty_in_engine", True
        )  # a hidden parameter
        self._distributed = attr.pop("dist", False)
        if incoming_graph_data is not None and self._is_gs_graph(incoming_graph_data):
            # convert from gs graph always use distributed mode
            self._distributed = True
        self._default_label = default_label

        if not self._is_gs_graph(incoming_graph_data) and create_empty_in_engine:
            graph_def = empty_graph_in_engine(
                self, self.is_directed(), self._distributed
            )
            self._key = graph_def.key

        # attempt to load graph with data
        if incoming_graph_data is not None:
            if self._is_gs_graph(incoming_graph_data):
                self._init_with_arrow_property_graph(incoming_graph_data)
            else:
                g = to_nx_graph(incoming_graph_data, create_using=self)
                check_argument(isinstance(g, Graph))

        # load graph attributes (must be after to_nx_graph)
        self.graph.update(attr)
        self._saved_signature = self.signature

    def __repr__(self):
        s = "graphscope.nx.DiGraph\n"
        s += "type: " + self.template_str.split("<")[0]
        s += str(self._schema)
        return s

    @property
    @patch_docstring(RefDiGraph.adj)
    def adj(self):
        return AdjacencyView(self._succ)

    succ = adj

    @property
    @patch_docstring(RefDiGraph.pred)
    def pred(self):
        return AdjacencyView(self._pred)

    @patch_docstring(RefDiGraph.has_predecessor)
    def has_successor(self, u, v):
        return self.has_edge(u, v)

    @patch_docstring(RefDiGraph.has_predecessor)
    def has_predecessor(self, u, v):
        return self.has_edge(v, u)

    @patch_docstring(RefDiGraph.successors)
    def successors(self, n):
        check_node_is_legal(n)
        try:
            return iter(self._succ[n])
        except KeyError:
            raise NetworkXError("The node %s is not in the digraph." % (n,))

    # digraph definitions
    neighbors = successors

    @patch_docstring(RefDiGraph.predecessors)
    def predecessors(self, n):
        check_node_is_legal(n)
        try:
            return iter(self._pred[n])
        except KeyError:
            raise NetworkXError("The node %s is not in the digraph." % (n,))

    @property
    def edges(self):
        """An OutEdgeView of the DiGraph as G.edges or G.edges().

        edges(self, nbunch=None, data=False, default=None)

        The OutEdgeView provides set-like operations on the edge-tuples
        as well as edge attribute lookup. When called, it also provides
        an EdgeDataView object which allows control of access to edge
        attributes (but does not provide set-like operations).
        Hence, `G.edges[u, v]['color']` provides the value of the color
        attribute for edge `(u, v)` while
        `for (u, v, c) in G.edges.data('color', default='red'):`
        iterates through all the edges yielding the color attribute
        with default `'red'` if no color attribute exists.

        Parameters
        ----------
        nbunch : single node, container, or all nodes (default= all nodes)
            The view will only report edges incident to these nodes.
        data : string or bool, optional (default=False)
            The edge attribute returned in 3-tuple (u, v, ddict[data]).
            If True, return edge attribute dict in 3-tuple (u, v, ddict).
            If False, return 2-tuple (u, v).
        default : value, optional (default=None)
            Value used for edges that don't have the requested attribute.
            Only relevant if data is not True or False.

        Returns
        -------
        edges : OutEdgeView
            A view of edge attributes, usually it iterates over (u, v)
            or (u, v, d) tuples of edges, but can also be used for
            attribute lookup as `edges[u, v]['foo']`.

        See Also
        --------
        in_edges, out_edges

        Notes
        -----
        Nodes in nbunch that are not in the graph will be (quietly) ignored.
        For directed graphs this returns the out-edges.

        Examples
        --------
        >>> G = nx.DiGraph()
        >>> nx.add_path(G, [0, 1, 2])
        >>> G.add_edge(2, 3, weight=5)
        >>> [e for e in G.edges]
        [(0, 1), (1, 2), (2, 3)]
        >>> G.edges.data()  # default data is {} (empty dict)
        OutEdgeDataView([(0, 1, {}), (1, 2, {}), (2, 3, {'weight': 5})])
        >>> G.edges.data("weight", default=1)
        OutEdgeDataView([(0, 1, 1), (1, 2, 1), (2, 3, 5)])
        >>> G.edges([0, 2])  # only edges incident to these nodes
        OutEdgeDataView([(0, 1), (2, 3)])
        >>> G.edges(0)  # only edges incident to a single node (use G.adj[0]?)
        OutEdgeDataView([(0, 1)])

        """
        return OutEdgeView(self)

    # alias out_edges to edges
    out_edges = edges

    @property
    @patch_docstring(RefDiGraph.in_edges)
    def in_edges(self):
        return InEdgeView(self)

    @property
    def degree(self):
        """A DegreeView for the Graph as G.degree or G.degree().

        The node degree is the number of edges adjacent to the node.
        The weighted node degree is the sum of the edge weights for
        edges incident to that node.

        This object provides an iterator for (node, degree) as well as
        lookup for the degree for a single node.

        Parameters
        ----------
        nbunch : single node, container, or all nodes (default= all nodes)
            The view will only report edges incident to these nodes.

        weight : string or None, optional (default=None)
           The name of an edge attribute that holds the numerical value used
           as a weight.  If None, then each edge has weight 1.
           The degree is the sum of the edge weights adjacent to the node.

        Returns
        -------
        If a single node is requested
        deg : int
            Degree of the node

        OR if multiple nodes are requested
        nd_iter : iterator
            The iterator returns two-tuples of (node, degree).

        See Also
        --------
        in_degree, out_degree

        Examples
        --------
        >>> G = nx.DiGraph()
        >>> nx.add_path(G, [0, 1, 2, 3])
        >>> G.degree(0) # node 0 with degree 1
        1
        >>> list(G.degree([0, 1, 2]))
        [(0, 1), (1, 2), (2, 2)]

        """
        return DiDegreeView(self)

    @property
    @patch_docstring(RefDiGraph.in_degree)
    def in_degree(self):
        return InDegreeView(self)

    @property
    @patch_docstring(RefDiGraph.out_degree)
    def out_degree(self):
        return OutDegreeView(self)

    @patch_docstring(RefDiGraph.is_directed)
    def is_directed(self):
        return True

    @patch_docstring(RefDiGraph.is_multigraph)
    def is_multigraph(self):
        return False

    @patch_docstring(RefDiGraph.reverse)
    def reverse(self, copy=True):
        self._try_convert_arrow_to_dynamic()

        if not copy:
            g = self.__class__(create_empty_in_engine=False)
            g.graph.update(self.graph)
            op = dag_utils.create_graph_view(self, "reversed")
            graph_def = op.eval()
            g._key = graph_def.key
            g._schema = deepcopy(self._schema)
            g._graph = self
            g._is_client_view = False
            g = freeze(g)
        else:
            g = self.__class__(create_empty_in_engine=False)
            g.graph = self.graph
            g.name = self.name
            g._op = self._op
            op = copy_graph(self, "reverse")
            graph_def = op.eval()
            g._key = graph_def.key
            g._schema = deepcopy(self._schema)
        g._session = self._session
        return g
