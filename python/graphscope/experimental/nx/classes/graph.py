#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file graph.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/classes/graph.py
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

import copy
import hashlib
import json

from networkx.classes.coreviews import AdjacencyView
from networkx.classes.graphviews import generic_graph_view
from networkx.classes.reportviews import DegreeView
from networkx.classes.reportviews import EdgeView
from networkx.classes.reportviews import NodeView

from graphscope.client.session import default_session
from graphscope.client.session import get_default_session
from graphscope.client.session import get_session_by_id
from graphscope.experimental import nx
from graphscope.experimental.nx import NetworkXError
from graphscope.experimental.nx.classes.dicts import AdjDict
from graphscope.experimental.nx.classes.dicts import NodeDict
from graphscope.experimental.nx.convert import from_gs_graph
from graphscope.experimental.nx.convert import to_networkx_graph
from graphscope.experimental.nx.convert import to_nx_graph
from graphscope.experimental.nx.utils.other import empty_graph_in_engine
from graphscope.experimental.nx.utils.other import parse_ret_as_dict
from graphscope.framework import dag_utils
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.framework.graph_schema import GraphSchema
from graphscope.proto import types_pb2


class Graph(object):
    """
    Base class for undirected graphs.

    A Graph stores nodes and edges with optional data, or attributes.

    Graphs hold undirected edges. Self loops are allowed but multiple
    (parallel) edges are not.

    Nodes can be strings or integers objects with optional key/value attributes.

    Edges are represented as links between nodes with optional
    key/value attributes

    Parameters
    ----------
    incoming_graph_data : input graph (optional, default: None)
        Data to initialize graph. If None (default) an empty
        graph is created.  The data can be any format that is supported
        by the to_nx_graph() function, currently including edge list,
        dict of dicts, dict of lists, NetworkX graph, NumPy matrix
        or 2d ndarray, SciPy sparse matrix, or a graphscope graph.

    attr : keyword arguments, optional (default= no attributes)
        Attributes to add to graph as key=value pairs.

    See Also
    --------
    DiGraph
    graphscope.Graph

    Examples
    --------
    Create an empty graph structure (a "null graph") with no nodes and
    no edges.

    >>> G = nx.Graph()

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
    in an associated attribute dictionary (the keys must be string).
    By default these are empty, but can be added or changed using
    add_edge, add_node or direct manipulation of the attribute
    dictionaries named graph, node and edge respectively.

    >>> G = nx.Graph(day="Friday")
    >>> G.graph
    {'day': 'Friday'}

    Add node attributes using add_node(), add_nodes_from() or G.nodes

    >>> G.add_node(1, time='5pm')
    >>> G.add_nodes_from([3], time='2pm')
    >>> G.nodes[1]
    {'time': '5pm'}
    >>> G.nodes[1]['room'] = 714  # node must exist already to use G.nodes
    >>> del G.nodes[1]['room']  # remove attribute
    >>> list(G.nodes(data=True))
    [(1, {'time': '5pm'}), (3, {'time': '2pm'})]

    Add edge attributes using add_edge(), add_edges_from(), subscript
    notation, or G.edges.

    >>> G.add_edge(1, 2, weight=4.7 )
    >>> G.add_edges_from([(3, 4), (4, 5)], color='red')
    >>> G.add_edges_from([(1, 2, {'color': 'blue'}), (2, 3, {'weight': 8})])
    >>> G[1][2]['weight'] = 4.7
    >>> G.edges[1, 2]['weight'] = 4

    Warning: we protect the graph data structure by making `G.edges` a
    read-only dict-like structure. However, you can assign to attributes
    in e.g. `G.edges[1, 2]`. Thus, use 2 sets of brackets to add/change
    data attributes: `G.edges[1, 2]['weight'] = 4`

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

    But the edges() method is often more convenient:

    >>> for u, v, weight in G.edges.data('weight'):
    ...     if weight is not None:
    ...         # Do something useful with the edges
    ...         pass

    **Reporting:**

    Simple graph information is obtained using object-attributes and methods.
    Reporting typically provides views instead of containers to reduce memory
    usage. The views update as the graph is updated similarly to dict-views.
    The objects `nodes, `edges` and `adj` provide access to data attributes
    via lookup (e.g. `nodes[n], `edges[u, v]`, `adj[u][v]`) and iteration
    (e.g. `nodes.items()`, `nodes.data('color')`,
    `nodes.data('color', default='blue')` and similarly for `edges`)
    Views exist for `nodes`, `edges`, `neighbors()`/`adj` and `degree`.

    For details on these and other miscellaneous methods, see below.
    """

    node_dict_factory = NodeDict
    adjlist_dict_factory = AdjDict
    graph_attr_dict_factory = dict
    _graph_type = types_pb2.DYNAMIC_PROPERTY

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

        Examples
        --------
        >>> G = nx.Graph()  # or DiGraph, etc
        >>> G = nx.Graph(name='my graph')
        >>> e = [(1, 2), (2, 3), (3, 4)]  # list of edges
        >>> G = nx.Graph(e)

        Arbitrary graph attribute pairs (key=value) may be assigned

        >>> G = nx.Graph(e, day="Friday")
        >>> G.graph
        {'day': 'Friday'}

        graphscope graph can convert to nx.Graph throught incomming_graph_data.

        >>> g = Graph()
        >>> g.load_from(vertices={}, edges={})
        >>> G = nx.Graph(g)  # or DiGraph, etc

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

        # attempt to load graph with data
        if incoming_graph_data is not None:
            if self.is_gs_graph(incoming_graph_data):
                graph_def = from_gs_graph(incoming_graph_data, self)
                self._key = graph_def.key
                self._schema.init_nx_schema(incoming_graph_data.schema)
            else:
                g = to_nx_graph(incoming_graph_data, create_using=self)
                check_argument(isinstance(g, Graph))

        # load graph attributes (must be after to_nx_graph)
        self.graph.update(attr)
        self._saved_signature = self.signature

    def is_gs_graph(self, incoming_graph_data):
        return (
            hasattr(incoming_graph_data, "graph_type")
            and incoming_graph_data.graph_type == types_pb2.ARROW_PROPERTY
        )

    def to_directed_class(self):
        """Returns the class to use for empty directed copies.

        If you subclass the base classes, use this to designate
        what directed class to use for `to_directed()` copies.
        """
        return nx.DiGraph

    def to_undirected_class(self):
        """Returns the class to use for empty undirected copies.

        If you subclass the base classes, use this to designate
        what directed class to use for `to_directed()` copies.
        """
        return Graph

    @property
    def op(self):
        """The DAG op of this graph."""
        return self._op

    @property
    def session_id(self):
        return self._session_id

    @property
    def key(self):
        """String key of the coresponding engine graph."""
        if hasattr(self, "_graph"):
            return self._graph.key  # this graph is a graph view, use host graph key
        return self._key

    @property
    def schema(self):
        return self._schema

    @property
    def template_sigature(self):
        if self._key is None:
            raise RuntimeError("graph should be registered in remote.")
        return hashlib.sha256(
            "{}.{}.{}.{}.{}".format(
                self._graph_type,
                self._schema.oid_type,
                self._schema.vid_type,
                self._schema.vdata_type,
                self._schema.edata_type,
            ).encode("utf-8")
        ).hexdigest()

    @property
    def graph_type(self):
        return self._graph_type

    @property
    def name(self):
        """String identifier of the graph.

        This graph attribute appears in the attribute dict G.graph
        keyed by the string `"name"`. as well as an attribute (technically
        a property) `G.name`. This is entirely user controlled.
        """
        return self.graph.get("name", "")

    @name.setter
    def name(self, s):
        self.graph["name"] = s

    def __str__(self):
        """Returns the graph name.

        Returns
        -------
        name : string
           The name of the graph.

        Examples
        --------
        >>> G = nx.Graph(name='foo')
        >>> str(G)
        'foo'
        """
        return self.name

    def __copy__(self):
        raise NetworkXError("not support shallow copy.")

    def __deepcopy__(self, memo):
        return self.copy()

    def __iter__(self):
        """Iterate over the nodes. Use: 'for n in G'.

        Returns
        -------
        niter : iterator
            An iterator over all nodes in the graph.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> [n for n in G]
        [0, 1, 2, 3]
        >>> list(G)
        [0, 1, 2, 3]
        """
        return iter(self._node)

    def __contains__(self, n):
        """Returns True if n is a node, False otherwise. Use: 'n in G'.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> 1 in G
        True
        """
        return self.has_node(n)

    def __len__(self):
        """Returns the number of nodes in the graph. Use: 'len(G)'.

        Returns
        -------
        nnodes : int
            The number of nodes in the graph.

        See Also
        --------
        number_of_nodes, order  which are identical

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> len(G)
        4
        """
        return self.number_of_nodes()

    def __getitem__(self, n):
        """Return a dict of neighbors of node n.  Use: 'G[n]'.

        Parameters
        ----------
        n : node
           A node in the graph

        Returns
        -------
        adj_dict : dictionary
           The adjacency dictionary for nodes connected to n.

        Notes
        -----
        G[n] is the same as G.adj[n] and similar to G.neighbors(n)
        (which is an iterator over G.adj[n])

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G[0]
        NbrsView({1: {}})
        """
        if not isinstance(n, (int, str)):
            raise TypeError(n)
        return self.adj[n]

    @property
    def signature(self):
        """Generate a signature of the current graph"""
        return self._key

    def add_node(self, node_for_adding, **attr):
        """Add a single node `node_for_adding` and update node attributes.

        Parameters
        ----------
        node_for_adding : node
            A node can be int or str object.

        attr : keyword arguments, optional
            Set or change node attributes using key=value.

        See Also
        --------
        add_nodes_from

        Examples
        --------
        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_node(1)
        >>> G.add_node(2)
        >>> G.number_of_nodes()
        2

        Use keywords set/change node attributes:

        >>> G.add_node(1, size=10)
        >>> G.add_node(3, weight=0.4, type='apple')

        Notes
        -----
        nx.Graph support int or str object of nodes.
        """
        return self.add_nodes_from([node_for_adding], **attr)

    def add_nodes_from(self, nodes_for_adding, **attr):
        """Add multiple nodes.

        Parameters
        ----------
        nodes_for_adding : iterable container
            A container of nodes (list, dict, set, etc.).
            OR
            A container of (node, attribute dict) tuples.
            Node attributes are updated using the attribute dict.
        attr : keyword arguments, optional (default= no attributes)
            Update attributes for all nodes in nodes.
            Node attributes specified in nodes as a tuple take
            precedence over attributes specified via keyword arguments.

        See Also
        --------
        add_node

        Examples
        --------
        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_nodes_from([1, 2, 3, 4, 5])
        >>> G.number_of_nodes()
        5

        Use keywords to update specific node attributes for every node.

        >>> G.add_nodes_from([1, 2], size=10)
        >>> G.add_nodes_from([3, 4], weight=0.4)

        Use (node, attrdict) tuples to update attributes for specific nodes.

        >>> G.add_nodes_from([(1, dict(size=11)), (2, {'color':'blue'})])
        >>> G.nodes[1]['size']
        11
        >>> H = nx.Graph()
        >>> H.add_nodes_from(G.nodes(data=True))
        >>> H.nodes[1]['size']
        11
        """
        nodes = []
        for n in nodes_for_adding:
            data = dict(attr)
            try:
                nn, dd = n
                data.update(dd)
                node = [nn, data]
            except (TypeError, ValueError):
                node = [n, data]
            if not isinstance(node[0], (int, str)):
                continue
            if self._schema.add_vertex_properties(data):
                nodes.append(json.dumps(node))
        self._op = dag_utils.modify_vertices(self, types_pb2.ADD_NODES, nodes)
        return self._op.eval()

    def remove_node(self, n):
        """Remove node n.

        Removes the node n and all adjacent edges.

        Parameters
        ----------
        n: node
           If the node is not in the graph it is silently ignored.

        See Also
        --------
        remove_nodes_from

        Examples
        --------
        >>> G = nx.path_graph(3)  # or DiGraph
        >>> list(G.edges)
        [(0, 1), (1, 2)]
        >>> G.remove_node(1)
        >>> list(G.edges)
        []
        """
        if not self.has_node(n):
            # NetworkXError if n not in self
            raise NetworkXError("The node %s is not in the graph." % (n,))
        return self.remove_nodes_from([n])

    def remove_nodes_from(self, nodes_for_removing):
        """Remove multiple nodes.

        Parameters
        ----------
        nodes_for_removing : iterable container
            A container of nodes (list, dict, set, etc.).  If a node
            in the container is not in the graph it is silently
            ignored.

        See Also
        --------
        remove_node

        Examples
        --------
        >>> G = nx.path_graph(3)  # or DiGraph
        >>> e = list(G.nodes)
        >>> e
        [0, 1, 2]
        >>> G.remove_nodes_from(e)
        >>> list(G.nodes)
        []

        """
        nodes = []
        for n in nodes_for_removing:
            nodes.append(json.dumps([n]))
        self._op = dag_utils.modify_vertices(self, types_pb2.DEL_NODES, nodes)
        return self._op.eval()

    @property
    def nodes(self):
        """A NodeView of the Graph as G.nodes or G.nodes().

        Can be used as `G.nodes` for data lookup and for set-like operations.
        Can also be used as `G.nodes(data='color', default=None)` to return a
        NodeDataView which reports specific node data but no set operations.
        It presents a dict-like interface as well with `G.nodes.items()`
        iterating over `(node, nodedata)` 2-tuples and `G.nodes[3]['foo']`
        providing the value of the `foo` attribute for node `3`. In addition,
        a view `G.nodes.data('foo')` provides a dict-like interface to the
        `foo` attribute of each node. `G.nodes.data('foo', default=1)`
        provides a default for nodes that do not have attribute `foo`.

        Parameters
        ----------
        data : string or bool, optional (default=False)
            The node attribute returned in 2-tuple (n, ddict[data]).
            If True, return entire node attribute dict as (n, ddict).
            If False, return just the nodes n.

        default : value, optional (default=None)
            Value used for nodes that don't have the requested attribute.
            Only relevant if data is not True or False.

        Returns
        -------
        NodeView
            Allows set-like operations over the nodes as well as node
            attribute dict lookup and calling to get a NodeDataView.
            A NodeDataView iterates over `(n, data)` and has no set operations.
            A NodeView iterates over `n` and includes set operations.

            When called, if data is False, an iterator over nodes.
            Otherwise an iterator of 2-tuples (node, attribute value)
            where the attribute is specified in `data`.
            If data is True then the attribute becomes the
            entire data dictionary.

        Notes
        -----
        If your node data is not needed, it is simpler and equivalent
        to use the expression ``for n in G``, or ``list(G)``.

        Examples
        --------
        There are two simple ways of getting a list of all nodes in the graph:

        >>> G = nx.path_graph(3)
        >>> list(G.nodes)
        [0, 1, 2]
        >>> list(G)
        [0, 1, 2]

        To get the node data along with the nodes:

        >>> G.add_node(1, time='5pm')
        >>> G.nodes[0]['foo'] = 'bar'
        >>> list(G.nodes(data=True))
        [(0, {'foo': 'bar'}), (1, {'time': '5pm'}), (2, {})]
        >>> list(G.nodes.data())
        [(0, {'foo': 'bar'}), (1, {'time': '5pm'}), (2, {})]

        >>> list(G.nodes(data='foo'))
        [(0, 'bar'), (1, None), (2, None)]
        >>> list(G.nodes.data('foo'))
        [(0, 'bar'), (1, None), (2, None)]

        >>> list(G.nodes(data='time'))
        [(0, None), (1, '5pm'), (2, None)]
        >>> list(G.nodes.data('time'))
        [(0, None), (1, '5pm'), (2, None)]

        >>> list(G.nodes(data='time', default='Not Available'))
        [(0, 'Not Available'), (1, '5pm'), (2, 'Not Available')]
        >>> list(G.nodes.data('time', default='Not Available'))
        [(0, 'Not Available'), (1, '5pm'), (2, 'Not Available')]

        If some of your nodes have an attribute and the rest are assumed
        to have a default attribute value you can create a dictionary
        from node/attribute pairs using the `default` keyword argument
        to guarantee the value is never None::

            >>> G = nx.Graph()
            >>> G.add_node(0)
            >>> G.add_node(1, weight=2)
            >>> G.add_node(2, weight=3)
            >>> dict(G.nodes(data='weight', default=1))
            {0: 1, 1: 2, 2: 3}

        """
        nodes = NodeView(self)
        self.__dict__["nodes"] = nodes
        return nodes

    @parse_ret_as_dict
    def get_node_data(self, n):
        """Returns the attribute dictionary of node n.

        This is identical to `G[n]`.

        Parameters
        ----------
        n : nodes

        Returns
        -------
        node_dict : dictionary
            The node attribute dictionary.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph etc
        >>> G[0]
        {}

        Warning: Assigning to `G[n]` is not permitted.
        But it is safe to assign attributes `G[n]['foo']`

        >>> G[0]['weight'] = 7
        >>> G[0]['weight']
        7

        >>> G = nx.path_graph(4)  # or DiGraph etc
        >>> G.get_node_data(0, 1)
        {}

        """
        op = dag_utils.report_graph(self, types_pb2.NODE_DATA, node=json.dumps([n]))
        return op.eval()

    def number_of_nodes(self):
        """Returns the number of nodes in the graph.

        Returns
        -------
        nnodes : int
            The number of nodes in the graph.

        See Also
        --------
        order, __len__ which are identical

        Examples
        --------
        >>> G = nx.path_graph(3)  # or DiGraph
        >>> G.number_of_nodes()
        3
        """
        op = dag_utils.report_graph(self, types_pb2.NODE_NUM)
        return int(op.eval())

    def order(self):
        """Returns the number of nodes in the graph.

        Returns
        -------
        nnodes : int
            The number of nodes in the graph.

        See Also
        --------
        number_of_nodes, __len__ which are identical

        Examples
        --------
        >>> G = nx.path_graph(3)  # or DiGraph
        >>> G.order()
        3
        """
        return self.number_of_nodes()

    def has_node(self, n):
        """Returns True if the graph contains the node n.

        Identical to 'n in G'

        Parameters
        ----------
        n : node

        Examples
        --------
        >>> G = nx.path_graph(3)  # or DiGraph
        >>> G.has_node(0)
        True

        It is more readable and simpler to use

        >>> 0 in G
        True
        """
        if not isinstance(n, (int, str)):
            return False
        op = dag_utils.report_graph(self, types_pb2.HAS_NODE, node=json.dumps([n]))
        return int(op.eval())

    def add_edge(self, u_of_edge, v_of_edge, **attr):
        """Add an edge between u and v.

        The nodes u and v will be automatically added if they are
        not already in the graph.

        Edge attributes can be specified with keywords or by directly
        accessing the edge's attribute dictionary. See examples below.

        Parameters
        ----------
        u_of_edge, v_of_edge : nodes
            Nodes can be integer or string objects.
        attr : keyword arguments, optional
            Edge data (or labels or objects) can be assigned using
            keyword arguments.

        See Also
        --------
        add_edges_from : add a collection of edges

        Notes
        -----
        Adding an edge that already exists updates the edge data.

        Many algorithms designed for weighted graphs use
        an edge attribute (by default `weight`) to hold a numerical value.

        Examples
        --------
        The following all add the edge e=(1, 2) to graph G:

        >>> G = nx.Graph()   # or DiGraph
        >>> e = (1, 2)
        >>> G.add_edge(1, 2)           # explicit two-node form
        >>> G.add_edge(*e)             # single edge as tuple of two nodes
        >>> G.add_edges_from([(1, 2)])  # add edges from iterable container

        Associate data to edges using keywords:

        >>> G.add_edge(1, 2, weight=3)
        >>> G.add_edge(1, 3, weight=7, capacity=15, length=342.7)
        """
        return self.add_edges_from([(u_of_edge, v_of_edge)], **attr)

    def add_edges_from(self, ebunch_to_add, **attr):
        """Add all the edges in ebunch_to_add

        Parameters
        ----------
        ebunch_to_add : container of edges
            Each edge given in the container will be added to the
            graph. The edges must be given as as 2-tuples (u, v) or
            3-tuples (u, v, d) where d is a dictionary containing edge data.
        attr : keyword arguments, optional
            Edge data (or labels or objects) can be assigned using
            keyword arguments.

        See Also
        --------
        add_edge : add a single edge
        add_weighted_edges_from : convenient way to add weighted edges

        Notes
        -----
        Adding the same edge twice has no effect but any edge data
        will be updated when each duplicate edge is added.

        Edge attributes specified in an ebunch take precedence over
        attributes specified via keyword arguments.

        Examples
        --------
        >>> G = nx.Graph()   # or DiGraph
        >>> G.add_edges_from([(0, 1), (1, 2)]) # using a list of edge tuples
        >>> e = zip(range(0, 3), range(1, 4))
        >>> G.add_edges_from(e) # Add the path graph 0-1-2-3

        Associate data to edges

        >>> G.add_edges_from([(1, 2), (2, 3)], weight=3)
        >>> G.add_edges_from([(3, 4), (1, 4)], label='WN2898')
        """
        edges = []
        for e in ebunch_to_add:
            ne = len(e)
            data = dict(attr)
            if ne == 3:
                u, v, dd = e
                # make attributes specified in ebunch take precedence to attr
                data.update(dd)
            elif ne == 2:
                u, v = e
            else:
                raise NetworkXError(
                    "Edge tuple %s must be a 2-tuple or 3-tuple." % (e,)
                )
            if not isinstance(u, (int, str)) or not isinstance(v, (int, str)):
                continue
            # FIXME: support dynamic data type in same property
            self._schema.add_edge_properties(data)
            edge = [u, v, data]
            edges.append(json.dumps(edge))
            if len(edges) > 10000:  # make sure messages size not larger than rpc max
                op = dag_utils.modify_edges(self, types_pb2.ADD_EDGES, edges)
                op.eval()
                edges.clear()
        if len(edges) > 0:
            op = dag_utils.modify_edges(self, types_pb2.ADD_EDGES, edges)
            op.eval()

    def add_weighted_edges_from(self, ebunch_to_add, weight="weight", **attr):
        """Add weighted edges in `ebunch_to_add` with specified weight attr

        Parameters
        ----------
        ebunch_to_add : container of edges
            Each edge given in the list or container will be added
            to the graph. The edges must be given as 3-tuples (u, v, w)
            where w is a number.
        weight : string, optional (default= 'weight')
            The attribute name for the edge weights to be added.
        attr : keyword arguments, optional (default= no attributes)
            Edge attributes to add/update for all edges.

        See Also
        --------
        add_edge : add a single edge
        add_edges_from : add multiple edges

        Notes
        -----
        Adding the same edge twice for Graph/DiGraph simply updates
        the edge data.

        Examples
        --------
        >>> G = nx.Graph()   # or DiGraph
        >>> G.add_weighted_edges_from([(0, 1, 3.0), (1, 2, 7.5)])
        """
        return self.add_edges_from(
            ((u, v, {weight: d}) for u, v, d in ebunch_to_add), **attr
        )

    def remove_edge(self, u, v):
        """Remove the edge between u and v.

        Parameters
        ----------
        u, v : nodes
            Remove the edge between nodes u and v. If there is not an edge between
            u and v, just silently ignore.

        See Also
        --------
        remove_edges_from : remove a collection of edges

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G.remove_edge(0, 1)
        >>> e = (1, 2)
        >>> G.remove_edge(*e) # unpacks e from an edge tuple
        >>> e = (2, 3, {'weight':7}) # an edge with attribute data
        >>> G.remove_edge(*e[:2]) # select first part of edge tuple
        """
        if not self.has_edge(u, v):
            raise NetworkXError("The edge %s-%s is not in the graph" % (u, v))
        return self.remove_edges_from([(u, v)])

    def remove_edges_from(self, ebunch):
        """Remove all edges specified in ebunch.

        Parameters
        ----------
        ebunch: list or container of edge tuples
            Each edge given in the list or container will be removed
            from the graph. The edges can be:

                - 2-tuples (u, v) edge between u and v.
                - 3-tuples (u, v, k) where k is ignored.

        See Also
        --------
        remove_edge : remove a single edge

        Notes
        -----
        Will fail silently if an edge in ebunch is not in the graph.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> ebunch=[(1, 2), (2, 3)]
        >>> G.remove_edges_from(ebunch)
        """
        edges = []
        for e in ebunch:
            ne = len(e)
            if ne < 2:
                raise ValueError("Edge tuple %s must be a 2-tuple or 3-tuple." % (e,))
            edges.append(json.dumps(e[:2]))  # ignore edge data if present
        self._op = dag_utils.modify_edges(self, types_pb2.DEL_EDGES, edges)
        return self._op.eval()

    def set_edge_data(self, u, v, data):
        """Set edge data of edge (u, v).

        Parameters
        ----------
        u, v : nodes
            Nodes can be string or integer objects.
        data: dict
            Edge data to set to edge (u, v)

        See Also
        --------
        set_node_data : set node data of node

        Notes
        -----
            the method is called when to set_items in AdjEdgeAttr

        Examples:
        --------
        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_edge(1, 2)
        >>> dd = {'foo': 'bar'}
        >>> G[1][2] = dd  # call G.set_edge_data(1, 2, dd)
        >>> G[1][2]
        {'foo': 'bar'}

        """
        edge = [json.dumps((u, v, data))]
        self._op = dag_utils.modify_edges(self, types_pb2.UPDATE_EDGES, edge)
        return self._op.eval()

    def set_node_data(self, n, data):
        """Set data of node.

        Parameters
        ----------
        n : node
            node can be string or integer object which is existed in graph.
        data : dict
            data to set to n

        See Also
        --------
        set_edge_data : set data of edge

        Notes
        -----
            the method is called when to set_items in NodeAttr

        Examples:
        --------
        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_node(1)
        >>> dd = {'weight': 3}
        >>> G.nodes[1] = dd  # call G.set_node_data(1, dd)
        >>> G.nodes[1]
        {'weight': 3}

        """
        node = [json.dumps((n, data))]
        self._op = dag_utils.modify_vertices(self, types_pb2.UPDATE_NODES, node)
        return self._op.eval()

    def update(self, edges=None, nodes=None):
        """Update the graph using nodes/edges/graphs as input.

        Like dict.update, this method takes a graph as input, adding the
        graph's nodes and edges to this graph. It can also take two inputs:
        edges and nodes. Finally it can take either edges or nodes.
        To specify only nodes the keyword `nodes` must be used.

        The collections of edges and nodes are treated similarly to
        the add_edges_from/add_nodes_from methods. When iterated, they
        should yield 2-tuples (u, v) or 3-tuples (u, v, datadict).

        Parameters
        ----------
        edges : Graph object, collection of edges, or None
            The first parameter can be a graph or some edges. If it has
            attributes `nodes` and `edges`, then it is taken to be a
            Graph-like object and those attributes are used as collections
            of nodes and edges to be added to the graph.
            If the first parameter does not have those attributes, it is
            treated as a collection of edges and added to the graph.
            If the first argument is None, no edges are added.
        nodes : collection of nodes, or None
            The second parameter is treated as a collection of nodes
            to be added to the graph unless it is None.
            If `edges is None` and `nodes is None` an exception is raised.
            If the first parameter is a Graph, then `nodes` is ignored.

        Examples
        --------
        >>> G = nx.path_graph(5)
        >>> G.update(nx.complete_graph(range(4,10)))
        >>> from itertools import combinations
        >>> edges = ((u, v, {'power': u * v})
        ...          for u, v in combinations(range(10, 20), 2)
        ...          if u * v < 225)
        >>> nodes = [1000]  # for singleton, use a container
        >>> G.update(edges, nodes)

        Notes
        -----
        It you want to update the graph using an adjacency structure
        it is straightforward to obtain the edges/nodes from adjacency.
        The following examples provide common cases, your adjacency may
        be slightly different and require tweaks of these examples.

        >>> # dict-of-set/list/tuple
        >>> adj = {1: {2, 3}, 2: {1, 3}, 3: {1, 2}}
        >>> e = [(u, v) for u, nbrs in adj.items() for v in  nbrs]
        >>> G.update(edges=e, nodes=adj)

        >>> DG = nx.DiGraph()
        >>> # dict-of-dict-of-attribute
        >>> adj = {1: {2: 1.3, 3: 0.7}, 2: {1: 1.4}, 3: {1: 0.7}}
        >>> e = [(u, v, {'weight': d}) for u, nbrs in adj.items()
        ...      for v, d in nbrs.items()]
        >>> DG.update(edges=e, nodes=adj)

        >>> # dict-of-dict-of-dict
        >>> adj = {1: {2: {'weight': 1.3}, 3: {'color': 0.7, 'weight':1.2}}}
        >>> e = [(u, v, {'weight': d}) for u, nbrs in adj.items()
        ...      for v, d in nbrs.items()]
        >>> DG.update(edges=e, nodes=adj)

        >>> # predecessor adjacency (dict-of-set)
        >>> pred = {1: {2, 3}, 2: {3}, 3: {3}}
        >>> e = [(v, u) for u, nbrs in pred.items() for v in nbrs]

        See Also
        --------
        add_edges_from: add multiple edges to a graph
        add_nodes_from: add multiple nodes to a graph
        """
        if edges is not None:
            if nodes is not None:
                self.add_nodes_from(nodes)
                self.add_edges_from(edges)
            else:
                try:
                    graph_nodes = edges.nodes
                    graph_edges = edges.edges
                except AttributeError:
                    self.add_edges_from(edges)
                else:  # edges is Graph-like
                    self.add_nodes_from(graph_nodes.data())
                    self.add_edges_from(graph_edges.data())
                    self.graph.update(edges.graph)
        elif nodes is not None:
            self.add_nodes_from(nodes)
        else:
            raise NetworkXError("update needs nodes or edges input")

    def size(self, weight=None):
        """Returns the number of edges or total of all edge weights.

        See Also
        --------
        number_of_edges
        """
        if weight:
            return sum(d for v, d in self.degree(weight=weight)) / 2
        else:
            return sum(d for v, d in self.degree(weight=weight)) // 2
        # TODO: make the selfloop edge number correct.
        # else:
        #     config = dict()
        #     config['graph_name'] = self._graph_name
        #     config['graph_type'] = self._graph_type
        #     config['report_type'] = 'edge_num'
        #     op = report_graph(self, config=config)
        #     return int(get_default_session().run(op)) // 2

    def number_of_edges(self, u=None, v=None):
        """Returns the number of edges between two nodes.

        Parameters
        ----------
        u, v : nodes, optional (default=all edges)
            If u and v are specified, return the number of edges between
            u and v. Otherwise return the total number of all edges.

        Returns
        -------
        nedges : int
            The number of edges in the graph.  If nodes `u` and `v` are
            specified return the number of edges between those nodes. If
            the graph is directed, this only returns the number of edges
            from `u` to `v`.

        See Also
        --------
        size

        Examples
        --------
        For undirected graphs, this method counts the total number of
        edges in the graph:

        >>> G = nx.path_graph(4)
        >>> G.number_of_edges()
        3

        If you specify two nodes, this counts the total number of edges
        joining the two nodes:

        >>> G.number_of_edges(0, 1)
        1

        For directed graphs, this method can count the total number of
        directed edges from `u` to `v`:

        >>> G = nx.DiGraph()
        >>> G.add_edge(0, 1)
        >>> G.add_edge(1, 0)
        >>> G.number_of_edges(0, 1)
        1

        """
        if u is None:
            return int(self.size())
        elif self.has_edge(u, v):
            return 1
        else:
            return 0

    def has_edge(self, u, v):
        """Returns True if the edge (u, v) is in the graph.

        Parameters:
        -----------
        u, v: nodes
            Nodes can be, for example, strings or numbers.

        Returns
        -------
        edge_ind : bool
            True if edge is in the graph, False otherwise.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.has_edge(0, 1)  # using two nodes
        True
        >>> e = (0, 1)
        >>> G.has_edge(*e)  #  e is a 2-tuple (u, v)
        True
        >>> e = (0, 1, {'weight':7})
        >>> G.has_edge(*e[:2])  # e is a 3-tuple (u, v, data_dictionary)
        True

        The following syntax are equivalent:

        >>> G.has_edge(0, 1)
        True
        >>> 1 in G[0]  # though this gives KeyError if 0 not in G
        True

        """
        # check the node type
        # if isinstance(u, self.node_type) and isinstance(v, self.node_type):
        #     op = report_graph(self, types_pb2.HAS_EDGE, u=u, v=v, key='')
        #     return int(op.eval())
        # else:
        #     return False
        try:
            return v in self._adj[u]
        except KeyError:
            return False

    def neighbors(self, n):
        """Returns an iterator over all neighbors of node n.

        This is identical to `iter(G[n])`

        Parameters
        ----------
        n : node
           A node in the graph

        Returns
        -------
        neighbors : iterator
            An iterator over all neighbors of node n

        Raises
        ------
        KeyError
            If the node n is not in the graph.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> [n for n in G.neighbors(0)]
        [1]

        Notes
        -----
        Alternate ways to access the neighbors are ``G.adj[n]`` or ``G[n]``:

        >>> G = nx.Graph(node_type=str)   # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.add_edge('a', 'b', weight=7)
        >>> G['a']
        NbrsView({'b': {'weight': 7}})
        >>> G = nx.path_graph(4)
        >>> [n for n in G[0]]
        [1]
        """
        try:
            return iter(self._adj[n])
        except KeyError:
            raise NetworkXError("The node %s is not in the graph." % (n,))

    @property
    def edges(self):
        """An EdgesView of the Graph as G.edges or G.edges().

        edges(self, nbunch=None, data=False, default=None)

        The EdgesView provides set-like operations on the edge-tuples
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
        edges : EdgesView
            A view of edge attributes, usually it iterates over (u, v)
            or (u, v, d) tuples of edges, but can also be used for
            attribute lookup as `edges[u, v]['foo']`.

        Notes
        -----
        Nodes in nbunch that are not in the graph will be (quietly) ignored.
        For directed graphs this returns the out-edges.

        Examples
        --------
        >>> G = nx.path_graph(3)   # or MultiGraph, etc
        >>> G.add_edge(2, 3, weight=5)
        >>> [e for e in G.edges]
        [(0, 1), (1, 2), (2, 3)]
        >>> G.edges.data()  # default data is {} (empty dict)
        EdgeDataView([(0, 1, {}), (1, 2, {}), (2, 3, {'weight': 5})])
        >>> G.edges.data('weight', default=1)
        EdgeDataView([(0, 1, 1), (1, 2, 1), (2, 3, 5)])
        >>> G.edges([0, 3])  # only edges incident to these nodes
        EdgeDataView([(0, 1), (3, 2)])
        >>> G.edges(0)  # only edges incident to a single node (use G.adj[0]?)
        EdgeDataView([(0, 1)])
        """
        return EdgeView(self)

    def get_edge_data(self, u, v, default=None):
        """Returns the attribute dictionary associated with edge (u, v).

        This is identical to `G[u][v]` except the default is returned
        instead of an exception if the edge doesn't exist.

        Parameters
        ----------
        u, v : nodes
        default:  any Python object (default=None)
            Value to return if the edge (u, v) is not found.

        Returns
        -------
        edge_dict : dictionary
            The edge attribute dictionary.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G[0][1]
        {}

        Warning: Assigning to `G[u][v]` is not permitted.
        But it is safe to assign attributes `G[u][v]['foo']`

        >>> G[0][1]['weight'] = 7
        >>> G[0][1]['weight']
        7
        >>> G[1][0]['weight']
        7

        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.get_edge_data(0, 1)  # default edge data is {}
        {}
        >>> e = (0, 1)
        >>> G.get_edge_data(*e)  # tuple form
        {}
        >>> G.get_edge_data('a', 'b', default=0)  # edge not in graph, return 0
        0
        """
        if self.has_edge(u, v):
            op = dag_utils.report_graph(
                self, types_pb2.EDGE_DATA, edge=json.dumps((u, v)), key=""
            )
            return json.loads(op.eval())
        else:
            return default

    @property
    def adj(self):
        """Graph adjacency object holding the neighbors of each node.

        This object is a read-only dict-like structure with node keys
        and neighbor-dict values.  The neighbor-dict is keyed by neighbor
        to the edge-data-dict.  So `G.adj[3][2]['color'] = 'blue'` sets
        the color of the edge `(3, 2)` to `"blue"`.

        Iterating over G.adj behaves like a dict. Useful idioms include
        `for nbr, datadict in G.adj[n].items():`.

        The neighbor information is also provided by subscripting the graph.
        So `for nbr, foovalue in G[node].data('foo', default=1):` works.

        For directed graphs, `G.adj` holds outgoing (successor) info.
        """
        return AdjacencyView(self._adj)

    def adjacency(self):
        """Returns an iterator over (node, adjacency dict) tuples for all nodes.

        For directed graphs, only outgoing neighbors/adjacencies are included.

        Returns
        -------
        adj_iter : iterator
           An iterator over (node, adjacency dictionary) for all nodes in
           the graph.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> [(n, nbrdict) for n, nbrdict in G.adjacency()]
        [(0, {1: {}}), (1, {0: {}, 2: {}}), (2, {1: {}, 3: {}}), (3, {2: {}})]

        """
        return iter(self._adj.items())

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
        nd_view : A DegreeView object capable of iterating (node, degree) pairs

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.degree[0]  # node 0 has degree 1
        1
        >>> list(G.degree([0, 1, 2]))
        [(0, 1), (1, 2), (2, 2)]
        """
        return DegreeView(self)

    def clear(self):
        """Remove all nodes and edges from the graph."""
        # unload graph in grape, then create a new empty graph.
        op = dag_utils.unload_graph(self)
        op.eval()
        self.graph.clear()
        self.schema.clear()
        graph_def = empty_graph_in_engine(self, self.is_directed())
        self._key = graph_def.key
        self.schema.init_nx_schema()

    def is_directed(self):
        """Returns True if graph is directed, False otherwise."""
        return False

    def is_multigraph(self):
        """Returns True if graph is a multigraph, False otherwise."""
        return False

    def nbunch_iter(self, nbunch=None):
        """Returns an iterator over nodes contained in nbunch that are
        also in the graph.

        The nodes in nbunch are checked for membership in the graph
        and if not are silently ignored.

        Parameters
        ----------
        nbunch : single node, container, or all nodes (default= all nodes)
            The view will only report edges incident to these nodes.

        Returns
        -------
        niter : iterator
            An iterator over nodes in nbunch that are also in the graph.
            If nbunch is None, iterate over all nodes in the graph.

        Raises
        ------
        TypeError
            If nbunch is not a node or or sequence of nodes.
            If a node in nbunch is not hashable.

        See Also
        --------
        Graph.__iter__

        Notes
        -----
        When nbunch is an iterator, the returned iterator yields values
        directly from nbunch, becoming exhausted when nbunch is exhausted.

        To test whether nbunch is a single node, one can use
        "if nbunch in self:", even after processing with this routine.

        If nbunch is not a node or a (possibly empty) sequence/iterator
        or None, a :exc:`NetworkXError` is raised.  Also, if any object in
        nbunch is not hashable, a :exc:`NetworkXError` is raised.
        """
        if nbunch is None:  # include all nodes via iterator
            bunch = iter(self.nodes)
        elif (
            isinstance(nbunch, (int, str)) and nbunch in self
        ):  # if nbunch is a single node
            bunch = iter([nbunch])
        else:  # if nbunch is a sequence of nodes

            def bunch_iter(nlist, adj):
                try:
                    for n in nlist:
                        if not isinstance(n, (int, str)):
                            raise TypeError("invalid node")
                        if n in adj:
                            yield n
                except TypeError as e:
                    message = e.args[0]
                    # capture error for non-sequence/iterator nbunch.
                    if "iter" in message:
                        msg = "nbunch is not a node or a sequence of nodes."
                        raise NetworkXError(msg)
                    # capture error for invalid node.
                    elif "invalid" in message:
                        msg = "Node {} in sequence nbunch is not a valid node."
                        raise NetworkXError(msg.format(n))
                    else:
                        raise

            bunch = bunch_iter(nbunch, self._adj)
        return bunch

    def copy(self, as_view=False):
        """Returns a copy of the graph.

        The copy method by default returns an independent deep copy
        of the graph and attributes.

        If `as_view` is True then a view is returned instead of a copy.

        Notes
        -----
        All copies reproduce the graph structure, but data attributes
        may be handled in different ways. There are four types of copies
        of a graph that people might want.

        Deepcopy -- A "deepcopy" copies the graph structure as well as
        all data attributes and any objects they might contain.
        The entire graph object is new so that changes in the copy
        do not affect the original object. (see Python's copy.deepcopy)

        Fresh Data -- For fresh data, the graph structure is copied while
        new empty data attribute dicts are created. The resulting graph
        is independent of the original and it has no edge, node or graph
        attributes. Fresh copies are not enabled. Instead use:

            >>> H = G.__class__()
            >>> H.add_nodes_from(G)
            >>> H.add_edges_from(G.edges)

        View -- Inspired by dict-views, graph-views act like read-only
        versions of the original graph, providing a copy of the original
        structure without requiring any memory for copying the information.

        See the Python copy module for more information on shallow
        and deep copies, https://docs.python.org/2/library/copy.html.

        Parameters
        ----------
        as_view : bool, optional (default=False)
            If True, the returned graph-view provides a read-only view
            of the original graph without actually copying any data.

        Returns
        -------
        G : Graph
            A copy of the graph.

        See Also
        --------
        to_directed: return a directed copy of the graph.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> H = G.copy()

        """
        if as_view:
            return generic_graph_view(self)
        g = self.__class__(create_empty_in_engine=False)
        g.graph.update(self.graph)
        op = dag_utils.copy_graph(self, "identical")
        graph_def = op.eval()
        g._key = graph_def.key
        g._schema = copy.deepcopy(self._schema)
        return g

    def to_undirected(self, as_view=False):
        """Returns an undirected copy of the graph.

        Parameters
        ----------
        as_view : bool (optional, default=False)
          If True return a view of the original undirected graph.

        Returns
        -------
        G : Graph
            A deepcopy of the graph.

        See Also
        --------
        Graph, copy, add_edge, add_edges_from

        Notes
        -----
        This returns a "deepcopy" of the edge, node, and
        graph attributes which attempts to completely copy
        all of the data and references.

        Warning: If you have subclassed DiGraph to use dict-like objects
        in the data structure, those changes do not transfer to the
        Graph created by this method.

        Examples
        --------
        >>> G = nx.path_graph(2)
        >>> H = G.to_directed()
        >>> list(H.edges)
        [(0, 1), (1, 0)]
        >>> G2 = H.to_undirected()
        >>> list(G2.edges)
        [(0, 1)]
        """
        if self.is_directed():
            if as_view:
                graph_class = self.to_undirected_class()
                return generic_graph_view(self, graph_class)
            else:
                # NB: fallback, maybe slow, here should be deecopy
                fallback_G = to_networkx_graph(self)
                return fallback_G.to_undirected(as_view=as_view)
        else:
            return self.copy(as_view=as_view)

    def to_directed(self, as_view=False):
        """Returns a directed representation of the graph.

        Returns
        -------
        G : DiGraph
            A directed graph with the same name, same nodes, and with
            each edge (u, v, data) replaced by two directed edges
            (u, v, data) and (v, u, data).

        Notes
        -----
        This returns a "deepcopy" of the edge, node, and
        graph attributes which attempts to completely copy
        all of the data and references.

        Warning: If you have subclassed Graph to use dict-like objects
        in the data structure, those changes do not transfer to the
        DiGraph created by this method.

        Examples
        --------
        >>> G = nx.Graph()  # or MultiGraph, etc
        >>> G.add_edge(0, 1)
        >>> H = G.to_directed()
        >>> list(H.edges)
        [(0, 1), (1, 0)]

        If already directed, return a (deep) copy

        >>> G = nx.DiGraph()
        >>> G.add_edge(0, 1)
        >>> H = G.to_directed()
        >>> list(H.edges)
        [(0, 1)]
        """
        if self.is_directed():
            return self.copy(as_view=as_view)
        else:
            if as_view:
                graph_class = self.to_directed_class()
                return generic_graph_view(self, graph_class)
            else:
                # NB: fallback, maybe slow
                fallback_G = to_networkx_graph(self)
                return fallback_G.to_directed(as_view=as_view)

    def subgraph(self, nodes):
        """Returns a SubGraph view of the subgraph induced on `nodes`.

        The induced subgraph of the graph contains the nodes in `nodes`
        and the edges between those nodes.

        Parameters
        ----------
        nodes : list, iterable
            A container of nodes which will be iterated through once.

        Returns
        -------
        G : SubGraph View
            A subgraph view of the graph. The graph structure cannot be
            changed but node/edge attributes can and are shared with the
            original graph.

        Notes
        -----
        The graph, edge and node attributes are shared with the original graph.
        Changes to the graph structure is ruled out by the view, but changes
        to attributes are reflected in the original graph.

        To create a subgraph with its own copy of the edge/node attributes use:
        G.subgraph(nodes).copy()

        For an inplace reduction of a graph to a subgraph you can remove nodes:
        G.remove_nodes_from([n for n in G if n not in set(nodes)])

        Subgraph views are sometimes NOT what you want. In most cases where
        you want to do more than simply look at the induced edges, it makes
        more sense to just create the subgraph as its own graph with code like:

        ::

            # Create a subgraph SG based on a (possibly multigraph) G
            SG = G.__class__()
            SG.add_nodes_from((n, G.nodes[n]) for n in largest_wcc)
            if SG.is_multigraph:
                SG.add_edges_from((n, nbr, key, d)
                    for n, nbrs in G.adj.items() if n in largest_wcc
                    for nbr, keydict in nbrs.items() if nbr in largest_wcc
                    for key, d in keydict.items())
            else:
                SG.add_edges_from((n, nbr, d)
                    for n, nbrs in G.adj.items() if n in largest_wcc
                    for nbr, d in nbrs.items() if nbr in largest_wcc)
            SG.graph.update(G.graph)

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> H = G.subgraph([0, 1, 2])
        >>> list(H.edges)
        [(0, 1), (1, 2)]
        """
        # NB: fallback subgraph
        ng = to_networkx_graph(self)
        return ng.subgraph(nodes)

    def edge_subgraph(self, edges):
        """Returns the subgraph induced by the specified edges.

        The induced subgraph contains each edge in `edges` and each
        node incident to any one of those edges.

        Parameters
        ----------
        edges : iterable
            An iterable of edges in this graph.

        Returns
        -------
        G : Graph
            An edge-induced subgraph of this graph with the same edge
            attributes.

        Notes
        -----
        The graph, edge, and node attributes in the returned subgraph
        view are references to the corresponding attributes in the original
        graph. The view is read-only.

        To create a full graph version of the subgraph with its own copy
        of the edge or node attributes, use::

            >>> G.edge_subgraph(edges).copy()  # doctest: +SKIP

        Examples
        --------
        >>> G = nx.path_graph(5)
        >>> H = G.edge_subgraph([(0, 1), (3, 4)])
        >>> list(H.nodes)
        [0, 1, 3, 4]
        >>> list(H.edges)
        [(0, 1), (3, 4)]

        """
        # NB: fallback edge subgraph
        ng = to_networkx_graph(self)
        return ng.edge_subgraph(edges)

    @parse_ret_as_dict
    def batch_get_node(self, location):
        """Get node by location in batch.

        In grape engine, it will start fetch from location, and return a batch of nodes.

        Parameters
        ----------
        location: tuple
            location of start node, a tuple with fragment id and local id.

        Returns
        -------
        nodes_dict_with_status: dict
            the return contain three parts:
                ret['status']: bool, success or failed.
                ret['next']: tuple, next location.
                ret['batch']: list, the batch nodes id list.

        Example:
        >>> g = nx.Graph()
        >>> g.add_nodes_from([1, 2, 3])
        >>> g.batch_get_node((0, 0))  # start from frag-0, lid-0, mpirun np=1
        {'status': True, 'next': [1, 0], 'batch': [1, 2, 3]}
        """
        op = dag_utils.report_graph(
            self, types_pb2.NODES_BY_LOC, fid=location[0], lid=location[1]
        )
        return op.eval()

    @parse_ret_as_dict
    def get_nbrs(self, n, report_type=types_pb2.SUCCS_BY_NODE):
        """Get the neighbors of node.

        Parameters
        ----------
        n: node
            the node to get neighbors.
        report_type:
            the report type of report graph operation,
                types_pb2.SUCCS_BY_NODE: get the successors of node,
                types_pb2.PREDS_BY_NODE: get the predecessors of node,
                types_pb2.NEIGHBORS_BY_NODE: get all neighbors of node,

        Returns
        -------
        neighbors: dict

        Raises
        ------
        Raise NetworkxError if node not in graph.

        Examples
        --------
        >>> g = nx.Graph()
        >>> g.add_edges_from([(0, 1), (0, 2)])
        >>> g.get_nbrs(0)
        {0: {}, 2: {}}
        """
        if n not in self:
            raise NetworkXError("The node %s is not in the graph." % (n,))
        op = dag_utils.report_graph(self, report_type, node=json.dumps([n]))
        return op.eval()

    def batch_get_nbrs(self, location, report_type=types_pb2.SUCCS_BY_LOC):
        """Get neighbors of nodes by location in batch.

        In grape engine, it will start fetch from location, and return a batch of nodes' neighbors.

        Parameters
        ----------
        location: tuple
            location of start node, a tuple with fragment id and local id.
        report_type:
            the report type of report graph operation,
                types_pb2.SUCCS_BY_LOC: get the successors,
                types_pb2.PREDS_BY_LOC: get the predecessors,
                types_pb2.NEIGHBORS_BY_LOC: get all neighbors,

        Returns
        -------
        dict_with_status: dict
            the return contain three parts:
                ret['status']: bool, success or failed.
                ret['next']: tuple, next location.
                ret['batch']: list, the batch list.

        Examples:
        >>> # mpirun np=1
        >>> g = nx.Graph()
        >>> g.add_edges_from([(0, 1), (0, 2)])
        >>> g.batch_get_nbrs((0, 0))  # start from frag-0, lid-0
        {'status': True, 'next': [1, 0],
        'batch': [{'node': 0, 'nbrs': {'1': {}, '2': {}}}], [{'node': 1 .....}]}
        """
        op = dag_utils.report_graph(self, report_type, fid=location[0], lid=location[1])
        return op.eval()

    def get_degree(self, n, weight=None, report_type=types_pb2.OUT_DEG_BY_NODE):
        """Get degree of node.

        Parameters
        ----------
        n: node
        weight: the edge attribute to get degree. if is None, default 1
        report_type:
            the report type of report graph operation,
            types_pb2.OUT_DEG_BY_NODE: get the out degree of node,
            types_pb2.IN_DEG_BY_NODE: get the in degree of node,
            types_pb2.DEG_BY_NODE: get the degree of node,

        Returns
        -------
            degree: float or int

        Raises
        -----
        Raise NetworkxError if node not in graph.
        """
        op = dag_utils.report_graph(self, report_type, node=json.dumps([n]), key=weight)
        degree = float(op.eval())
        return degree if weight is not None else int(degree)

    def batch_get_degree(
        self, location, weight=None, report_type=types_pb2.OUT_DEG_BY_LOC
    ):
        """Get degree of nodes by location in batch.

        In grape engine, it will start fetch from location, and return a batch of nodes' degree.

        Parameters
        ----------
        location: tuple
            location of start node, a tuple with fragment id and local id.
        report_type:
            the report type of report graph operation,
                types_pb2.OUT_DEG_BY_LOC: get the out degree,
                types_pb2.IN_DEG_BY_LOC: get the in degree,
                types_pb2.DEG_BY_LOC: get degree,

        Returns
        -------
        dict_with_status: dict
            the return contain three parts:
                ret['status']: bool, success or failed.
                ret['next']: tuple, next location.
                ret['batch']: list, the degree list.

        Examples
        >>> # mpirun np=1
        >>> g = nx.Graph()
        >>> g.add_edges_from([(0, 1), (0, 2)])
        >>> g.batch_get_degree((0, 0))  # start from frag-0, lid-0
        {'status': True, 'next': [1, 0],
        'batch': [
            {'node': 0, 'degree': 2},
            {'node':1, 'degree': 1},
            {'node':2, 'degree': 1},
        ]}
        """
        op = dag_utils.report_graph(
            self, report_type, fid=location[0], lid=location[1], key=weight
        )
        return op.eval()

    def project_to_simple(self, v_prop=None, e_prop=None):
        """Project nx graph to a simple graph to run builtin alogorithms.

        A simple graph is a accesser wrapper of property graph that only single edge
        attribute and single node attribute are available.

        Parameters
        ----------
        v_prop: the node attribute key to project, (optional, default None)
        e_prop: the edge attribute key to project, (optional, default None)

        Returns
        -------
        simple_graph: nx.Graph or nx.DiGraph
            A nx.Graph object that hold a simple graph projected by host property graph.

        Notes
        -------
            the method is implicit called in builtin apps.
        """
        if hasattr(self, "_graph"):
            raise TypeError("graph view can't project to simple graph")

        if v_prop is None:
            v_prop = str(v_prop)
            v_prop_type = types_pb2.NULL
        else:
            check_argument(isinstance(v_prop, str))
            if v_prop in self._schema.vertex_properties[0]:
                v_prop_type = self._schema.vertex_properties[0][v_prop]
            else:
                raise InvalidArgumentError(
                    "graph not contains the vertex property {}".format(v_prop)
                )

        if e_prop is None:
            e_prop = str(e_prop)
            e_prop_type = types_pb2.NULL
        else:
            check_argument(isinstance(e_prop, str))
            if e_prop in self._schema.edge_properties[0]:
                e_prop_type = self._schema.edge_properties[0][e_prop]
            else:
                raise InvalidArgumentError(
                    "graph not contains the edge property {}".format(e_prop)
                )
        op = dag_utils.project_dynamic_property_graph(
            self, v_prop, e_prop, v_prop_type, e_prop_type
        )
        graph_def = op.eval()
        sess = get_session_by_id(self._session_id)
        with default_session(sess):
            graph = self.__class__(create_empty_in_engine=False)
        graph = nx.freeze(graph)
        graph._graph_type = types_pb2.DYNAMIC_PROJECTED
        graph._key = graph_def.key
        graph.schema.get_schema_from_def(graph_def.schema_def)
        graph._saved_signature = self._saved_signature
        return graph
