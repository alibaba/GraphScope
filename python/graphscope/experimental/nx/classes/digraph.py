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

from networkx.classes.coreviews import AdjacencyView
from networkx.classes.graphviews import reverse_view
from networkx.classes.reportviews import DiDegreeView
from networkx.classes.reportviews import InDegreeView
from networkx.classes.reportviews import InEdgeView
from networkx.classes.reportviews import OutDegreeView
from networkx.classes.reportviews import OutEdgeView

from graphscope.client.session import get_default_session
from graphscope.experimental.nx import NetworkXError
from graphscope.experimental.nx.classes.graph import Graph
from graphscope.experimental.nx.convert import from_gs_graph
from graphscope.experimental.nx.convert import to_nx_graph
from graphscope.experimental.nx.utils.other import empty_graph_in_engine
from graphscope.framework.dag_utils import copy_graph
from graphscope.framework.graph_schema import GraphSchema
from graphscope.proto import types_pb2


class DiGraph(Graph):
    """
    Base class for directed graphs.

    A DiGraph stores nodes and edges with optional data, or attributes.

    DiGraphs hold directed edges.  Self loops are allowed but multiple
    (parallel) edges are not.

    Nodes can be strings or integers objects with optional key/value attributes.

    Edges are represented as links between nodes with optional
    key/value attributes.

    Parameters
    ----------
    incoming_graph_data : input graph (optional, default: None)
        Data to initialize graph. If None (default) an empty
        graph is created.  The data can be any format that is supported
        by the to_networkx_graph() function, currently including edge list,
        dict of dicts, dict of lists, NetworkX graph, NumPy matrix
        or 2d ndarray, SciPy sparse matrix, or a graphscope graph.

    attr : keyword arguments, optional (default= no attributes)
        Attributes to add to graph as key=value pairs.

    See Also
    --------
    Graph
    graphscope.Graph

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

    def __init__(self, incoming_graph_data=None, **attr):
        """Initialize a graph with edges, name, or graph attributes

        Parameters
        ----------
        incoming_graph_data : input graph (optional, default: None)
            Data to initialize graph. If None (default) an empty
            graph is created.  The data can be any format that is supported
            by the to_nx_graph() function, currently including edge list,
            dict of dicts, dict of lists, NetworkX graph, NumPy matrix
            or 2d ndarray, Pandas DataFrame, SciPy sparse matrix, or a graphscope
            graph.

        attr : keyword arguments, optional (default= no attributes)
            Attributes to add to graph as key=value pairs.

        See Also
        --------
        convert

        Examples
        --------
        >>> G = nx.Graph()  # or DiGraph
        >>> G = nx.Graph(name='my graph')
        >>> e = [(1, 2), (2, 3), (3, 4)]  # list of edges
        >>> G = nx.Graph(e)

        Arbitrary graph attribute pairs (key=value) may be assigned

        >>> G = nx.Graph(e, day="Friday")
        >>> G.graph
        {'day': 'Friday'}

        """
        sess = get_default_session()
        if sess is None:
            raise ValueError(
                "Cannot find a default session. "
                "Please register a session using graphscope.session(...).as_default()"
            )
        self._session_id = sess.session_id

        self._key = None
        self._op = None
        self._graph_type = self._graph_type
        self._schema = GraphSchema()
        self._schema.init_nx_schema()
        create_empty_in_engine = attr.pop(
            "create_empty_in_engine", True
        )  # a hidden parameter
        if not self.is_gs_graph(incoming_graph_data) and create_empty_in_engine:
            graph_def = empty_graph_in_engine(self, self.is_directed())
            self._key = graph_def.key

        self.graph_attr_dict_factory = self.graph_attr_dict_factory
        self.node_dict_factory = self.node_dict_factory
        self.adjlist_dict_factory = self.adjlist_dict_factory

        self.graph = self.graph_attr_dict_factory()
        self._node = self.node_dict_factory(self)
        self._adj = self.adjlist_dict_factory(self)
        self._pred = self.adjlist_dict_factory(self, types_pb2.PREDS_BY_NODE)
        self._succ = self._adj
        # attempt to load graph with data
        if incoming_graph_data is not None:
            if self.is_gs_graph(incoming_graph_data):
                graph_def = from_gs_graph(incoming_graph_data, self)
                self._key = graph_def.key
                self._schema.init_nx_schema(incoming_graph_data.schema)
            else:
                to_nx_graph(incoming_graph_data, create_using=self)
        # load graph attributes (must be after to_nx_graph)
        self.graph.update(attr)
        self._saved_signature = self.signature

    @property
    def adj(self):
        """Graph adjacency object holding the successors of each node.

        This object is a read-only dict-like structure with node keys
        and neighbor-dict values.  The neighbor-dict is keyed by neighbor
        to the edge-data-dict.  So `G.succ[3][2]['color'] = 'blue'` sets
        the color of the edge `(3, 2)` to `"blue"`.

        Iterating over G.succ behaves like a dict. Useful idioms include
        `for nbr, datadict in G.succ[n].items():`.  A data-view not provided
        by dicts also exists: `for nbr, foovalue in G.succ[node].data('foo'):`
        and a default can be set via a `default` argument to the `data` method.

        The neighbor information is also provided by subscripting the graph.
        So `for nbr, foovalue in G[node].data('foo', default=1):` works.

        For directed graphs, `G.adj` is identical to `G.succ`.
        """
        return AdjacencyView(self._succ)

    succ = adj

    @property
    def pred(self):
        """Graph adjacency object holding the predecessors of each node.

        This object is a read-only dict-like structure with node keys
        and neighbor-dict values.  The neighbor-dict is keyed by neighbor
        to the edge-data-dict.  So `G.pred[2][3]['color'] = 'blue'` sets
        the color of the edge `(3, 2)` to `"blue"`.

        Iterating over G.pred behaves like a dict. Useful idioms include
        `for nbr, datadict in G.pred[n].items():`.  A data-view not provided
        by dicts also exists: `for nbr, foovalue in G.pred[node].data('foo'):`
        A default can be set via a `default` argument to the `data` method.
        """
        return AdjacencyView(self._pred)

    def is_gs_graph(self, incoming_graph_data):
        return (
            hasattr(incoming_graph_data, "graph_type")
            and incoming_graph_data.graph_type == types_pb2.ARROW_PROPERTY
        )

    def has_successor(self, u, v):
        """Returns True if node u has successor v.

        This is true if graph has the edge u->v.
        """
        return self.has_edge(u, v)

    def has_predecessor(self, u, v):
        """Returns True if node u has predecessor v.

        This is true if graph has the edge u<-v.
        """
        return self.has_edge(v, u)

    def successors(self, n):
        """Returns an iterator over successor nodes of n.

        A successor of n is a node m such that there exists a directed
        edge from n to m.

        Parameters
        ----------
        n : node
           A node in the graph

        Raises
        -------
        KeyError
           If n is not in the graph.

        See Also
        --------
        predecessors

        Notes
        -----
        neighbors() and successors() are the same.
        """
        try:
            return iter(self._succ[n])
        except KeyError:
            raise NetworkXError("The node %s is not in the digraph." % (n,))

    # digraph definitions
    neighbors = successors

    def predecessors(self, n):
        """Returns an iterator over predecessor nodes of n.

        A predecessor of n is a node m such that there exists a directed
        edge from m to n.

        Parameters
        ----------
        n : node
           A node in the graph

        Raises
        -------
        Error
           If n is not in the graph.

        See Also
        --------
        successors
        """
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
        >>> G = nx.DiGraph()   # or MultiDiGraph, etc
        >>> nx.add_path(G, [0, 1, 2])
        >>> G.add_edge(2, 3, weight=5)
        >>> [e for e in G.edges]
        [(0, 1), (1, 2), (2, 3)]
        >>> G.edges.data()  # default data is {} (empty dict)
        OutEdgeDataView([(0, 1, {}), (1, 2, {}), (2, 3, {'weight': 5})])
        >>> G.edges.data('weight', default=1)
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
    def in_edges(self):
        """An InEdgeView of the Graph as G.in_edges or G.in_edges().

        in_edges(self, nbunch=None, data=False, default=None):

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
        in_edges : InEdgeView
            A view of edge attributes, usually it iterates over (u, v)
            or (u, v, d) tuples of edges, but can also be used for
            attribute lookup as `edges[u, v]['foo']`.

        See Also
        --------
        edges
        """
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
        >>> G = nx.DiGraph()   # or MultiDiGraph
        >>> nx.add_path(G, [0, 1, 2, 3])
        >>> G.degree(0) # node 0 with degree 1
        1
        >>> list(G.degree([0, 1, 2]))
        [(0, 1), (1, 2), (2, 2)]

        """
        return DiDegreeView(self)

    @property
    def in_degree(self):
        """An InDegreeView for (node, in_degree) or in_degree for single node.

        The node in_degree is the number of edges pointing to the node.
        The weighted node degree is the sum of the edge weights for
        edges incident to that node.

        This object provides an iteration over (node, in_degree) as well as
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
            In-degree of the node

        OR if multiple nodes are requested
        nd_iter : iterator
            The iterator returns two-tuples of (node, in-degree).

        See Also
        --------
        degree, out_degree

        Examples
        --------
        >>> G = nx.DiGraph()
        >>> nx.add_path(G, [0, 1, 2, 3])
        >>> G.in_degree(0) # node 0 with degree 0
        0
        >>> list(G.in_degree([0, 1, 2]))
        [(0, 0), (1, 1), (2, 1)]
        """
        return InDegreeView(self)

    @property
    def out_degree(self):
        """An OutDegreeView for (node, out_degree)

        The node out_degree is the number of edges pointing out of the node.
        The weighted node degree is the sum of the edge weights for
        edges incident to that node.

        This object provides an iterator over (node, out_degree) as well as
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
            Out-degree of the node

        OR if multiple nodes are requested
        nd_iter : iterator
            The iterator returns two-tuples of (node, out-degree).

        See Also
        --------
        degree, in_degree

        Examples
        --------
        >>> G = nx.DiGraph()
        >>> nx.add_path(G, [0, 1, 2, 3])
        >>> G.out_degree(0) # node 0 with degree 1
        1
        >>> list(G.out_degree([0, 1, 2]))
        [(0, 1), (1, 1), (2, 1)]
        """
        return OutDegreeView(self)

    def is_directed(self):
        """Returns True if graph is directed, False otherwise."""
        return True

    def is_multigraph(self):
        return False

    def reverse(self, copy=True):
        """Returns the reverse of the graph.

        The reverse is a graph with the same nodes and edges
        but with the directions of the edges reversed.

        Parameters
        ----------
        copy : bool optional (default=True)
            If True, return a new DiGraph holding the reversed edges.
            If False, the reverse graph is created using a view of
            the original graph.
        """
        if not copy:
            return reverse_view(self)
        g = self.__class__(create_empty_in_engine=False)
        g.graph = self.graph
        g.name = self.name
        g._op = self._op
        op = copy_graph(self, "reverse")
        graph_def = op.eval()
        g._key = graph_def.key
        g._schema = deepcopy(self._schema)
        return g
