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

import orjson as json
from networkx import freeze
from networkx.classes.graph import Graph as RefGraph
from networkx.classes.reportviews import DegreeView

from graphscope import nx
from graphscope.client.session import get_default_session
from graphscope.client.session import get_session_by_id
from graphscope.framework import dag_utils
from graphscope.framework import utils
from graphscope.framework.graph_schema import GraphSchema
from graphscope.nx import NetworkXError
from graphscope.nx.classes.cache import Cache
from graphscope.nx.classes.coreviews import AdjacencyView
from graphscope.nx.classes.dict_factory import AdjListDict
from graphscope.nx.classes.dict_factory import NeighborAttrDict
from graphscope.nx.classes.dict_factory import NeighborDict
from graphscope.nx.classes.dict_factory import NodeAttrDict
from graphscope.nx.classes.dict_factory import NodeDict
from graphscope.nx.classes.graphviews import generic_graph_view
from graphscope.nx.classes.reportviews import EdgeView
from graphscope.nx.classes.reportviews import NodeView
from graphscope.nx.convert import to_networkx_graph
from graphscope.nx.utils.compat import patch_docstring
from graphscope.nx.utils.misc import clear_mutation_cache
from graphscope.nx.utils.misc import init_empty_graph_in_engine
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2

__all__ = ["Graph"]


class _GraphBase(object):
    """
    Base class for networkx module.
    This is an empty class use to classify networkx graph.
    """

    pass


class Graph(_GraphBase):
    """
    Base class for undirected graphs.

    A Graph that holds the metadata of a graph, and provides NetworkX-like Graph APIs.

    It is worth noticing that the graph is actually stored by the Analytical Engine backend.
    In other words, the Graph object holds nothing but metadata of a graph.

    Graph support nodes and edges with optional data, or attributes.

    Graphs support undirected edges. Self loops are allowed but multiple
    (parallel) edges are not.

    Nodes can be arbitrary int/str/float/bool objects with optional
    key/value attributes.

    Edges are represented as links between nodes with optional
    key/value attributes.

    Graph support node label if it's created from a GraphScope graph object.
    nodes are identified by `(label, id)` tuple.

    Parameters
    ----------
    incoming_graph_data : input graph (optional, default: None)
        Data to initialize graph. If None (default) an empty
        graph is created.  The data can be any format that is supported
        by the to_networkx_graph() function, currently including edge list,
        dict of dicts, dict of lists, NetworkX graph, 2D NumPy array, SciPy
        sparse matrix or a GraphScope graph object.

    default_label : default node label (optional, default: None)
        if incoming_graph_data is a GraphScope graph object, default label means
        the nodes of the label can be identified by id directly, other label nodes
        need to use `(label, id)` to identify.

    attr : keyword arguments, optional (default= no attributes)
        Attributes to add to graph as key=value pairs.

    See Also
    --------
    DiGraph

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

    In addition to integers, strings/floats/bool can represent a node too.

    >>> G.add_node('a node')
    >>> G.add_node(3.14)
    >>> G.add_node(True)

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

    **Transformation**

    Create a graph with GraphScope graph object. First we init a GraphScope graph
    with two node labels: person and comment`

    >>> g = graphscope.g(directed=False).add_vertice("persion.csv", label="person").add_vertice("comment.csv", label="comment")

    create a graph with g, set default_label to 'person'

    >>> G = nx.Graph(g, default_label="person")

    `person` label nodes can be identified by id directly, for `comment` label,
    we has to use tuple `("comment", id)` identify. Like, add a person label
    node and a comment label node

    >>> G.add_node(0, type="person")
    >>> G.add_node(("comment", 0), type="comment")

    print property of two nodes

    >>> G.nodes[0]
    {"type", "person"}
    >>> G.nodes[("comment", 0)]
    {"type", "comment"}

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
    node_attr_dict_factory = NodeAttrDict
    adjlist_outer_dict_factory = AdjListDict
    adjlist_inner_dict_factory = NeighborDict
    edge_attr_dict_factory = NeighborAttrDict
    graph_attr_dict_factory = dict
    graph_cache_factory = Cache
    _graph_type = graph_def_pb2.DYNAMIC_PROPERTY

    def to_directed_class(self):
        return nx.DiGraph

    @patch_docstring(RefGraph.to_undirected_class)
    def to_undirected_class(self):
        return Graph

    def __init__(self, incoming_graph_data=None, default_label=None, **attr):
        """Initialize a graph with graph, edges, name, or graph attributes

        Parameters
        ----------
        incoming_graph_data : input graph (optional, default: None)
            Data to initialize graph. If None (default) an empty
            graph is created. The data can be an edge list, any
            NetworkX graph object or any GraphScope graph object.
            If the corresponding optional Python packages are installed
            the data can also be a 2D NumPy array, a SciPy sparse matrix

        default_label : default node label (optional, default: "_")
            if incoming_graph_data is a GraphScope graph object, default label means
            the nodes of the label can be accessed by id directly, other label nodes
            need to use `(label, id)` to access.

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

        Created from a GraphScope graph object

        >>> g = graphscope.g(directed=False)  # if transform to DiGraph, directed=True
        >>> g.add_vertices("person.csv", label="person").add_vertices("comment.csv", label="comment").add_edges(...)
        >>> G = nx.Graph(g, default_label="person") # or DiGraph

        """
        self.graph_attr_dict_factory = self.graph_attr_dict_factory
        self.node_dict_factory = self.node_dict_factory
        self.adjlist_outer_dict_factory = self.adjlist_outer_dict_factory
        self.cache = self.graph_cache_factory(self)

        # init node and adj (must be after cache)
        self.graph = self.graph_attr_dict_factory()
        self._node = self.node_dict_factory(self)
        self._adj = self.adjlist_outer_dict_factory(self)

        self._key = None
        self._op = None
        self._schema = GraphSchema()

        # buffer caches for add_node and add_edge
        self._add_node_cache = []
        self._add_edge_cache = []
        self._remove_node_cache = []
        self._remove_edge_cache = []

        create_empty_in_engine = attr.pop(
            "create_empty_in_engine", True
        )  # a hidden parameter
        self._distributed = attr.pop("dist", False)
        if incoming_graph_data is not None and self._is_gs_graph(incoming_graph_data):
            # convert from gs graph always use distributed mode
            if self._session is None:
                self._session = get_session_by_id(incoming_graph_data.session_id)
        self._default_label = default_label
        self._default_label_id = -1

        if self._session is None:
            self._session = get_default_session()

        if not self._is_gs_graph(incoming_graph_data) and create_empty_in_engine:
            graph_def = init_empty_graph_in_engine(
                self, self.is_directed(), self._distributed
            )
            self._key = graph_def.key

        # attempt to load graph with data
        if incoming_graph_data is not None:
            to_networkx_graph(incoming_graph_data, create_using=self)
            self.cache.warmup()

        # load graph attributes (must be after to_networkx_graph)
        self.graph.update(attr)
        self._saved_signature = self.signature
        self._is_client_view = False

        # statically create the unload op
        #
        # networkx operations update the op, but keep the key as same, thus
        # the unload op don't need to be refreshed.
        if self.op is None:
            self._unload_op = None
        else:
            self._unload_op = dag_utils.unload_graph(self)

    def _is_gs_graph(self, incoming_graph_data):
        return (
            hasattr(incoming_graph_data, "graph_type")
            and incoming_graph_data.graph_type == graph_def_pb2.ARROW_PROPERTY
        )

    def __del__(self):
        if self._key is None or self._session.disconnected:
            return

        if self.cache.enable_iter_cache:
            try:
                self.cache.shutdown()
            except:  # noqa: E722, pylint: disable=bare-except
                pass
        self.cache.shutdown_executor()

        if not self._is_client_view and self._unload_op is not None:
            self._unload_op.eval()
        self._key = None

    @property
    def op(self):
        """The DAG op of this graph."""
        return self._op

    @property
    def session(self):
        """Get the session of graph.

        Returns:
            Return session that the graph belongs to.
        """
        if hasattr(self, "_graph") and self._is_client_view:
            return (
                self._graph.session
            )  # this graph is a client side graph view, use host graph session
        return self._session

    @property
    def session_id(self):
        """Get session's id of graph.

        Returns:
            str: Return session id that the graph belongs to.
        """
        if hasattr(self, "_graph") and self._is_client_view:
            return (
                self._graph.session_id
            )  # this graph is a client side graph view, use host graph session_id
        return self._session.session_id

    @property
    def key(self):
        """Key of the coresponding engine graph."""
        if hasattr(self, "_graph") and self._is_client_view:
            return (
                self._graph.key
            )  # this graph is a client side graph view, use host graph key
        return self._key

    @property
    def signature(self):
        """Generate a signature of the current graph"""
        return self._key

    @property
    def schema(self):
        """Schema of the graph.

        Returns:
            :class:`GraphSchema`: the schema of the graph
        """
        return self._schema

    @property
    def template_str(self):
        if self._key is None:
            raise RuntimeError("graph should be registered in remote.")
        s = ""
        if self._graph_type == graph_def_pb2.DYNAMIC_PROPERTY:
            s = "gs::DynamicFragment"
        elif self._graph_type == graph_def_pb2.DYNAMIC_PROJECTED:
            vdata_type = utils.data_type_to_cpp(self._schema.vdata_type)
            edata_type = utils.data_type_to_cpp(self._schema.edata_type)
            s = f"gs::DynamicProjectedFragment<{vdata_type},{edata_type}>"
        elif self._graph_type == graph_def_pb2.ARROW_PROPERTY:
            oid_type = utils.normalize_data_type_str(
                utils.data_type_to_cpp(self._schema.oid_type)
            )
            vid_type = self._schema.vid_type
            s = f"vineyard::ArrowFragment<{oid_type},{vid_type}>"
        elif self._graph_type == graph_def_pb2.ARROW_FLATTENED:
            oid_type = utils.normalize_data_type_str(
                utils.data_type_to_cpp(self._schema.oid_type)
            )
            vid_type = self._schema.vid_type
            vdata_type = utils.data_type_to_cpp(self._schema.vdata_type)
            edata_type = utils.data_type_to_cpp(self._schema.edata_type)
            s = f"gs::ArrowFlattenedFragment<{oid_type},{vid_type},{vdata_type},{edata_type}>"
        elif self._graph_type == graph_def_pb2.ARROW_PROJECTED:
            oid_type = utils.normalize_data_type_str(
                utils.data_type_to_cpp(self._schema.oid_type)
            )
            vid_type = self._schema.vid_type
            vdata_type = utils.data_type_to_cpp(self._schema.vdata_type)
            edata_type = utils.data_type_to_cpp(self._schema.edata_type)
            s = f"gs::ArrowProjectedFragment<{oid_type},{vid_type},{vdata_type},{edata_type}>"
        else:
            raise ValueError(f"Unsupported graph type: {self._graph_type}")
        return s

    @property
    def graph_type(self):
        """The type of the graph object.

        Returns:
            type (`types_pb2.GraphType`): the type of the graph.
        """
        return self._graph_type

    @property
    @patch_docstring(RefGraph.name)
    def name(self):
        return self.graph.get("name", "")

    @name.setter
    def name(self, s):
        self.graph["name"] = s

    def loaded(self):
        return self.key is not None

    @clear_mutation_cache
    @patch_docstring(RefGraph.__str__)
    def __str__(self):
        if self.graph_type in (
            graph_def_pb2.ARROW_PROPERTY,
            graph_def_pb2.DYNAMIC_PROPERTY,
        ):
            return "".join(
                [
                    type(self).__name__,
                    f" named {self.name!r}" if self.name else "",
                    f" with {self.number_of_nodes()} nodes and {self.number_of_edges()} edges",
                ]
            )
        return f"graphscope.nx.Graph\n{graph_def_pb2.GraphTypePb.Name(self.graph_type)}"

    def __copy__(self):
        """override default __copy__"""
        raise NetworkXError("graphscope.nx not support shallow copy.")

    def __deepcopy__(self, memo):
        """override default __deepcopy__"""
        return self.copy()

    @clear_mutation_cache
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

    @clear_mutation_cache
    def __contains__(self, n):
        """Returns True if n is a node, False otherwise. Use: 'n in G'.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> 1 in G
        True
        """
        try:
            if self.graph_type == graph_def_pb2.ARROW_PROPERTY:
                n = self._convert_to_label_id_tuple(n)
            op = dag_utils.report_graph(self, types_pb2.HAS_NODE, node=json.dumps(n))
            archive = op.eval()
            return archive.get_bool()
        except (TypeError, NetworkXError, KeyError):
            return False

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
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> len(G)
        4

        """
        return self.number_of_nodes()

    def __getitem__(self, n):
        """Returns a dict of neighbors of node n.  Use: 'G[n]'.

        Parameters
        ----------
        n : node
           A node in the graph.

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
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G[0]
        AtlasView({1: {}})
        """
        return self.adj[n]

    @clear_mutation_cache
    def add_node(self, node_for_adding, **attr):
        """Add a single node `node_for_adding` and update node attributes.

        Parameters
        ----------
        node_for_adding : node
            A node can be int, float, str, tuple or bool object.

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
        nx.Graph support int, float, str, tuple or bool object of nodes.
        """
        self._convert_arrow_to_dynamic()
        self._add_node_cache.append(
            (node_for_adding, attr) if attr else node_for_adding
        )

    @clear_mutation_cache
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
        >>> G.add_nodes_from("Hello")
        >>> K3 = nx.Graph([(0, 1), (1, 2), (2, 0)])
        >>> G.add_nodes_from(K3)
        >>> sorted(G.nodes(), key=str)
        [0, 1, 2, 'H', 'e', 'l', 'o']

        Use keywords to update specific node attributes for every node.

        >>> G.add_nodes_from([1, 2], size=10)
        >>> G.add_nodes_from([3, 4], weight=0.4)

        Use (node, attrdict) tuples to update attributes for specific nodes.

        >>> G.add_nodes_from([(1, dict(size=11)), (2, {"color": "blue"})])
        >>> G.nodes[1]["size"]
        11
        >>> H = nx.Graph()
        >>> H.add_nodes_from(G.nodes(data=True))
        >>> H.nodes[1]["size"]
        11

        """
        for n in nodes_for_adding:
            data = dict(attr)
            try:
                nn, dd = n
                data.update(dd)
                self.add_node(nn, **data)
            except (TypeError, ValueError):
                self.add_node(n, **data)

    @clear_mutation_cache
    def remove_node(self, n):
        """Remove node n.

        Removes the node n and all adjacent edges.
        Attempting to remove a non-existent node will raise an exception.

        Parameters
        ----------
        n : node
           A node in the graph

        Raises
        -------
        NetworkXError
           If n is not in the graph.

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
        self._convert_arrow_to_dynamic()
        self._remove_node_cache.append(n)

    @clear_mutation_cache
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
        for n in nodes_for_removing:
            self.remove_node(n)

    @property
    @clear_mutation_cache
    @patch_docstring(RefGraph.nodes)
    def nodes(self):
        nodes = NodeView(self)
        self.__dict__["nodes"] = nodes
        return nodes

    @clear_mutation_cache
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
        archive = op.eval()
        return archive.get_size()

    def order(self):
        """Returns the number of nodes in the graph.

        Returns
        -------
        nnodes : int
            The number of nodes in the graph.

        See Also
        --------
        number_of_nodes, __len__  which are identical

        Examples
        --------
        >>> G = nx.path_graph(3)  # or DiGraph
        >>> G.order()
        3
        """
        return self.number_of_nodes()

    def has_node(self, n):
        """Returns True if the graph contains the node n.

        Identical to `n in G`

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
        return n in self

    @clear_mutation_cache
    def add_edge(self, u_of_edge, v_of_edge, **attr):
        """Add an edge between u and v.

        The nodes u and v will be automatically added if they are
        not already in the graph.

        Edge attributes can be specified with keywords or by directly
        accessing the edge's attribute dictionary. See examples below.

        Parameters
        ----------
        u, v : nodes
            Nodes can be, for example, strings or numbers.
            Nodes must be int/string/float/tuple/bool hashable Python objects.
        attr : keyword arguments, optional
            Edge data can be assigned using
            keyword arguments.

        See Also
        --------
        add_edges_from : add a collection of edges

        Notes
        -----
        Adding an edge that already exists updates the edge data.

        Many networkx algorithms designed for weighted graphs use
        an edge attribute (by default `weight`) to hold a numerical value.

        Examples
        --------
        The following all add the edge e=(1, 2) to graph G:

        >>> G = nx.Graph()  # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> e = (1, 2)
        >>> G.add_edge(1, 2)  # explicit two-node form
        >>> G.add_edge(*e)  # single edge as tuple of two nodes
        >>> G.add_edges_from([(1, 2)])  # add edges from iterable container

        Associate data to edges using keywords:

        >>> G.add_edge(1, 2, weight=3)
        >>> G.add_edge(1, 3, weight=7, capacity=15, length=342.7)

        For non-string attribute keys, use subscript notation.

        >>> G.add_edge(1, 2)
        >>> G[1][2].update({0: 5})
        >>> G.edges[1, 2].update({0: 5})
        """
        self._convert_arrow_to_dynamic()
        self._add_edge_cache.append(
            (u_of_edge, v_of_edge, attr) if attr else (u_of_edge, v_of_edge)
        )

    @clear_mutation_cache
    def add_edges_from(self, ebunch_to_add, **attr):
        """Add all the edges in ebunch_to_add.

        Parameters
        ----------
        ebunch_to_add : container of edges
            Each edge given in the container will be added to the
            graph. The edges must be given as as 2-tuples (u, v) or
            3-tuples (u, v, d) where d is a dictionary containing edge data.
        attr : keyword arguments, optional
            Edge data can be assigned using
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
        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_edges_from([(0, 1), (1, 2)])  # using a list of edge tuples
        >>> e = zip(range(0, 3), range(1, 4))
        >>> G.add_edges_from(e)  # Add the path graph 0-1-2-3

        Associate data to edges

        >>> G.add_edges_from([(1, 2), (2, 3)], weight=3)
        >>> G.add_edges_from([(3, 4), (1, 4)], label="WN2898")
        """
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
            self.add_edge(u, v, **data)

    @clear_mutation_cache
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
        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_weighted_edges_from([(0, 1, 3.0), (1, 2, 7.5)])
        """
        for u, v, d in ebunch_to_add:
            # make attributes specified in ebunch take precedence to attr
            attr[weight] = d
            self.add_edge(u, v, **attr)

    @clear_mutation_cache
    @patch_docstring(RefGraph.remove_edge)
    def remove_edge(self, u, v):
        self._convert_arrow_to_dynamic()
        self._remove_edge_cache.append((u, v))

    @clear_mutation_cache
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
        >>> ebunch = [(1, 2), (2, 3)]
        >>> G.remove_edges_from(ebunch)
        """
        for e in ebunch:
            ne = len(e)
            if ne < 2:
                raise ValueError("Edge tuple %s must be a 2-tuple or 3-tuple." % (e,))
            self.remove_edge(e[0], e[1])

    @clear_mutation_cache
    def set_edge_data(self, u, v, data):
        """Set edge data of edge (u, v).

        Parameters
        ----------
        u, v : nodes
            Nodes can be int, str, float, tuple, bool hashable Python objects.
        data: dict
            Edge data to set to edge (u, v)

        See Also
        --------
        set_node_data : set node data of node

        Notes
        -----
            the method is called when to set_items in AdjEdgeAttr

        Examples
        --------
        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_edge(1, 2)
        >>> dd = {'foo': 'bar'}
        >>> G[1][2] = dd  # call G.set_edge_data(1, 2, dd)
        >>> G[1][2]
        {'foo': 'bar'}

        """
        self._convert_arrow_to_dynamic()

        edge = json.dumps(((u, v, data),), option=json.OPT_SERIALIZE_NUMPY)
        self._op = dag_utils.modify_edges(self, types_pb2.NX_UPDATE_EDGES, edge)
        self._op.eval(leaf=False)
        self.cache.clear_neighbor_attr_cache()

    @clear_mutation_cache
    def set_node_data(self, n, data):
        """Set data of node.

        Parameters
        ----------
        n : node
            node can be int, str, float, tuple, bool hashable Python object which is existed in graph.
        data : dict
            data to set to n

        See Also
        --------
        set_edge_data : set data of edge

        Notes
        -----
            the method is called when to set_items in NodeAttr

        Examples
        --------
        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_node(1)
        >>> dd = {'weight': 3}
        >>> G.nodes[1] = dd  # call G.set_node_data(1, dd)
        >>> G.nodes[1]
        {'weight': 3}

        """
        self._convert_arrow_to_dynamic()

        node = json.dumps(((n, data),), option=json.OPT_SERIALIZE_NUMPY)
        self._op = dag_utils.modify_vertices(self, types_pb2.NX_UPDATE_NODES, node)
        self._op.eval(leaf=False)
        self.cache.clear_node_attr_cache()

    @clear_mutation_cache
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
        >>> G.update(nx.complete_graph(range(4, 10)))
        >>> from itertools import combinations
        >>> edges = (
        ...     (u, v, {"power": u * v})
        ...     for u, v in combinations(range(10, 20), 2)
        ...     if u * v < 225
        ... )
        >>> nodes = [1000]  # for singleton, use a container
        >>> G.update(edges, nodes)

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

    @clear_mutation_cache
    def size(self, weight=None):
        """Returns the number of edges or total of all edge weights.

        Parameters
        ----------
        weight : string or None, optional (default=None)
            The edge attribute that holds the numerical value used
            as a weight. If None, then each edge has weight 1.

        Returns
        -------
        size : numeric
            The number of edges or
            (if weight keyword is provided) the total weight sum.

            If weight is None, returns an int. Otherwise a float
            (or more general numeric if the weights are more general).

        See Also
        --------
        number_of_edges

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G.size()
        3

        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_edge("a", "b", weight=2)
        >>> G.add_edge("b", "c", weight=4)
        >>> G.size()
        2
        >>> G.size(weight="weight")
        6.0
        """
        if weight:
            return sum(d for v, d in self.degree(weight=weight)) / 2
        op = dag_utils.report_graph(self, types_pb2.EDGE_NUM)
        archive = op.eval()
        return archive.get_size() // 2

    @clear_mutation_cache
    @patch_docstring(RefGraph.number_of_edges)
    def number_of_edges(self, u=None, v=None):
        edges_num = 0
        if u is None:
            edges_num = self.size()
        elif self.has_edge(u, v):
            edges_num = 1
        return edges_num

    @clear_mutation_cache
    def number_of_selfloops(self):
        op = dag_utils.report_graph(self, types_pb2.SELFLOOPS_NUM)
        archive = op.eval()
        return archive.get_size()

    @clear_mutation_cache
    def has_edge(self, u, v):
        """Returns True if the edge (u, v) is in the graph.

        This is the same as `v in G[u]` without KeyError exceptions.

        Parameters
        ----------
        u, v : nodes
            Nodes can be, for example, strings or numbers.
            Nodes must be int, str, float, tuple, bool  hashable Python objects.

        Returns
        -------
        edge_ind : bool
            True if edge is in the graph, False otherwise.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G.has_edge(0, 1)  # using two nodes
        True
        >>> e = (0, 1)
        >>> G.has_edge(*e)  #  e is a 2-tuple (u, v)
        True
        >>> e = (0, 1, {"weight": 7})
        >>> G.has_edge(*e[:2])  # e is a 3-tuple (u, v, data_dictionary)
        True

        The following syntax are equivalent:

        >>> G.has_edge(0, 1)
        True
        >>> 1 in G[0]  # though this gives KeyError if 0 not in G
        True

        """
        try:
            return v in self._adj[u]
        except KeyError:
            return False

    @clear_mutation_cache
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
        NetworkXError
            If the node n is not in the graph.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> [n for n in G.neighbors(0)]
        [1]

        Notes
        -----
        Alternate ways to access the neighbors are ``G.adj[n]`` or ``G[n]``:

        >>> G = nx.Graph()  # or DiGraph
        >>> G.add_edge("a", "b", weight=7)
        >>> G["a"]
        AtlasView({'b': {'weight': 7}})
        >>> G = nx.path_graph(4)
        >>> [n for n in G[0]]
        [1]
        """
        try:
            return iter(self._adj[n])
        except KeyError:
            raise NetworkXError("The node %s is not in the graph." % (n,))

    @property
    @clear_mutation_cache
    def edges(self):
        """An EdgeView of the Graph as G.edges or G.edges().

        edges(self, nbunch=None, data=False, default=None)

        The EdgeView provides set-like operations on the edge-tuples
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
        edges : EdgeView
            A view of edge attributes, usually it iterates over (u, v)
            or (u, v, d) tuples of edges, but can also be used for
            attribute lookup as `edges[u, v]['foo']`.

        Notes
        -----
        Nodes in nbunch that are not in the graph will be (quietly) ignored.
        For directed graphs this returns the out-edges.

        Examples
        --------
        >>> G = nx.path_graph(3)  # or DiGraph
        >>> G.add_edge(2, 3, weight=5)
        >>> [e for e in G.edges]
        [(0, 1), (1, 2), (2, 3)]
        >>> G.edges.data()  # default data is {} (empty dict)
        EdgeDataView([(0, 1, {}), (1, 2, {}), (2, 3, {'weight': 5})])
        >>> G.edges.data("weight", default=1)
        EdgeDataView([(0, 1, 1), (1, 2, 1), (2, 3, 5)])
        >>> G.edges([0, 3])  # only edges incident to these nodes
        EdgeDataView([(0, 1), (3, 2)])
        >>> G.edges(0)  # only edges incident to a single node (use G.adj[0]?)
        EdgeDataView([(0, 1)])
        """
        return EdgeView(self)

    @clear_mutation_cache
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
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G[0][1]
        {}

        Warning: Assigning to `G[u][v]` is not permitted.
        But it is safe to assign attributes `G[u][v]['foo']`

        >>> G[0][1]["weight"] = 7
        >>> G[0][1]["weight"]
        7
        >>> G[1][0]["weight"]
        7

        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G.get_edge_data(0, 1)  # default edge data is {}
        {}
        >>> e = (0, 1)
        >>> G.get_edge_data(*e)  # tuple form
        {}
        >>> G.get_edge_data("a", "b", default=0)  # edge not in graph, return 0
        0
        """
        if self.has_edge(u, v):
            if self.graph_type == graph_def_pb2.ARROW_PROPERTY:
                u = self._convert_to_label_id_tuple(u)
                v = self._convert_to_label_id_tuple(v)
            op = dag_utils.report_graph(
                self,
                types_pb2.EDGE_DATA,
                edge=json.dumps((u, v)),
                key="",
            )
            archive = op.eval()
            return json.loads(archive.get_bytes())
        else:
            return default

    @property
    @clear_mutation_cache
    @patch_docstring(RefGraph.adj)
    def adj(self):
        return AdjacencyView(self._adj)

    @clear_mutation_cache
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
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> [(n, nbrdict) for n, nbrdict in G.adjacency()]
        [(0, {1: {}}), (1, {0: {}, 2: {}}), (2, {1: {}, 3: {}}), (3, {2: {}})]

        """
        return iter(self._adj.items())

    @property
    @clear_mutation_cache
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
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G.degree[0]  # node 0 has degree 1
        1
        >>> list(G.degree([0, 1, 2]))
        [(0, 1), (1, 2), (2, 2)]
        """
        return DegreeView(self)

    def clear(self):
        """Remove all nodes and edges from the graph.

        This also removes the name, and all graph, node, and edge attributes.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G.clear()
        >>> list(G.nodes)
        []
        >>> list(G.edges)
        []

        """
        if self._graph_type == graph_def_pb2.ARROW_PROPERTY:
            # create an empty graph, no need to convert arrow to dynamic
            self._graph_type = graph_def_pb2.DYNAMIC_PROPERTY
            graph_def = init_empty_graph_in_engine(
                self, self.is_directed(), self._distributed
            )
            self._key = graph_def.key
        else:
            op = dag_utils.clear_graph(self)
            op.eval()

        self.graph.clear()
        self.schema.clear()
        self._add_node_cache.clear()
        self._add_edge_cache.clear()
        self._remove_node_cache.clear()
        self._remove_edge_cache.clear()
        self.cache.clear()

    @clear_mutation_cache
    def clear_edges(self):
        """Remove all edges from the graph without altering nodes.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> G.clear_edges()
        >>> list(G.nodes)
        [0, 1, 2, 3]
        >>> list(G.edges)
        []
        """
        self._convert_arrow_to_dynamic()
        self._op = dag_utils.clear_edges(self)
        self._op.eval(leaf=False)
        self.cache.clear()

    @patch_docstring(RefGraph.is_directed)
    def is_directed(self):
        return False

    @patch_docstring(RefGraph.is_multigraph)
    def is_multigraph(self):
        return False

    @clear_mutation_cache
    @patch_docstring(RefGraph.nbunch_iter)
    def nbunch_iter(self, nbunch=None):
        if nbunch is None:  # include all nodes via iterator
            bunch = iter(self.nodes)
        elif nbunch in self:  # if nbunch is a single node
            bunch = iter([nbunch])
        else:  # if nbunch is a sequence of nodes

            def bunch_iter(nlist, adj):
                try:
                    for n in nlist:
                        if n in adj:
                            yield n
                except TypeError as e:
                    message = e.args[0]
                    # capture error for non-sequence/iterator nbunch.
                    if "iter" in message:
                        msg = "nbunch is not a node or a sequence of nodes."
                        raise NetworkXError(msg) from e
                    # capture error for invalid node.
                    elif "hashable" in message:
                        msg = "Node {} in sequence nbunch is not a valid node."
                        raise NetworkXError(msg) from e
                    else:
                        raise

            bunch = bunch_iter(nbunch, self._adj)
        return bunch

    @clear_mutation_cache
    def copy(self, as_view=False):
        """Returns a copy of the graph.

        The copy method by default returns an independent deep copy
        of the graph and attributes.

        If `as_view` is True then a view is returned instead of a copy.

        Notes
        -----
        All copies reproduce the graph structure, but data attributes
        may be handled in different ways. There are three types of copies
        of a graph that people might want.

        Deepcopy -- A "deepcopy" copies the graph structure as well as
        all data attributes and any objects they might contain in Engine backend.
        The entire graph object is new so that changes in the copy
        do not affect the original object.

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
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> H = G.copy()

        """
        if as_view:
            g = generic_graph_view(self)
            g._is_client_view = True
            g._op = self._op
            g.cache = self.cache
        else:
            self._convert_arrow_to_dynamic()
            g = self.__class__(create_empty_in_engine=False)
            g.graph = copy.deepcopy(self.graph)
            op = dag_utils.copy_graph(self, "identical")
            graph_def = op.eval(leaf=False)
            g._op = op
            g._key = graph_def.key
            g.cache.warmup()
        g._session = self._session
        return g

    @clear_mutation_cache
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
        self._convert_arrow_to_dynamic()

        if self.is_directed():
            graph_class = self.to_undirected_class()
            g = graph_class(create_empty_in_engine=False)
            if as_view:
                g.graph.update(self.graph)
                g._graph = self
                g._is_client_view = False
                g = freeze(g)
            else:
                g.graph = copy.deepcopy(self.graph)
            op = dag_utils.to_undirected(self)
            graph_def = op.eval(leaf=False)
            g._op = op
            g._key = graph_def.key
            g._session = self._session
            g.cache.warmup()
            return g
        return self.copy(as_view=as_view)

    @clear_mutation_cache
    def to_directed(self, as_view=False):
        """Returns a directed representation of the graph.

        Parameters
        ----------
        as_view : bool, optional (default=False)
            If True return a view of the original directed graph.

        Returns
        -------
        G : DiGraph
            A directed graph with the same name, same nodes, and with
            each edge (u, v, data) replaced by two directed edges
            (u, v, data) and (v, u, data).

        Notes
        -----
        This by default returns a "deepcopy" of the edge, node, and
        graph attributes which attempts to completely copy
        all of the data and references.

        Examples
        --------
        >>> G = nx.Graph()
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
        self._convert_arrow_to_dynamic()

        if self.is_directed():
            return self.copy(as_view=as_view)
        graph_class = self.to_directed_class()
        if as_view:
            g = generic_graph_view(self, graph_class)
            g._op = self._op
            g._key = self._key
            g._session = self._session
            g._is_client_view = True
            return g
        g = graph_class(create_empty_in_engine=False)
        g.graph = copy.deepcopy(self.graph)
        op = dag_utils.to_directed(self)
        graph_def = op.eval(leaf=False)
        g._key = graph_def.key
        g._session = self._session
        g._op = op
        g.cache.warmup()
        return g

    @clear_mutation_cache
    def subgraph(self, nodes):
        """Returns a independent deep copy subgraph induced on `nodes`.

        The induced subgraph of the graph contains the nodes in `nodes`
        and the edges between those nodes.

        Parameters
        ----------
        nodes : list, iterable
            A container of nodes which will be iterated through once.

        Returns
        -------
        G : Graph
            A subgraph of the graph.

        Notes
        -----
        Unlike NetowrkX return a view, here return a independent deep copy subgraph.

        Examples
        --------
        >>> G = nx.path_graph(4)  # or DiGraph
        >>> H = G.subgraph([0, 1, 2])
        >>> list(H.edges)
        [(0, 1), (1, 2)]
        """
        self._convert_arrow_to_dynamic()

        induced_nodes = json.dumps(list(nodes))
        g = self.__class__(create_empty_in_engine=False)
        g.graph.update(self.graph)
        op = dag_utils.create_subgraph(self, nodes=induced_nodes)
        graph_def = op.eval(leaf=False)
        g._key = graph_def.key
        g._session = self._session
        g._op = op
        g.cache.warmup()
        return g

    @clear_mutation_cache
    def edge_subgraph(self, edges):
        """Returns a independent deep copy subgraph induced by the specified edges.

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
        Unlike NetworkX return a view, here return a independent deep copy subgraph.

        Examples
        --------
        >>> G = nx.path_graph(5) # or DiGraph
        >>> H = G.edge_subgraph([(0, 1), (3, 4)])
        >>> list(H.nodes)
        [0, 1, 3, 4]
        >>> list(H.edges)
        [(0, 1), (3, 4)]

        """
        self._convert_arrow_to_dynamic()

        induced_edges = json.dumps(list(edges))
        g = self.__class__(create_empty_in_engine=False)
        g.graph.update(self.graph)
        op = dag_utils.create_subgraph(self, edges=induced_edges)
        graph_def = op.eval(leaf=False)
        g._key = graph_def.key
        g._session = self._session
        g._op = op
        g.cache.warmup()
        return g

    @clear_mutation_cache
    def _project_to_simple(self, v_prop=None, e_prop=None):
        """Project nx graph to a simple graph to run builtin algorithms.

        A simple graph is a wrapper of property graph that only single edge
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
        if hasattr(self, "_graph") and self._is_client_view:
            # is a graph view, project the original graph(just for copy)
            graph = self._graph
            while hasattr(graph, "_graph"):
                graph = graph._graph
            return graph._project_to_simple(v_prop=v_prop, e_prop=e_prop)

        graph = self.__class__(create_empty_in_engine=False)
        graph = nx.freeze(graph)

        op = dag_utils.project_to_simple(self, str(v_prop), str(e_prop))
        graph_def = op.eval(leaf=False)
        graph._graph_type = graph_def.graph_type
        graph._key = graph_def.key
        graph._schema.from_graph_def(graph_def)
        graph._default_label_id = self._default_label_id
        graph._saved_signature = graph._key
        graph._op = op
        graph._session = self._session
        graph._graph = self  # projected graph also can report nodes.
        graph._is_client_view = False
        return graph

    def _clear_adding_cache(self):
        reset_cache = bool(
            len(self._add_node_cache) > 0 or len(self._add_edge_cache) > 0
        )
        if self._add_node_cache:
            nodes_to_modify = json.dumps(
                self._add_node_cache, option=json.OPT_SERIALIZE_NUMPY
            )
            self._op = dag_utils.modify_vertices(
                self, types_pb2.NX_ADD_NODES, nodes_to_modify
            )
            self._op.eval(leaf=False)
            self._add_node_cache.clear()

        if self._add_edge_cache:
            edges_to_modify = json.dumps(
                self._add_edge_cache, option=json.OPT_SERIALIZE_NUMPY
            )
            self._op = dag_utils.modify_edges(
                self, types_pb2.NX_ADD_EDGES, edges_to_modify
            )
            self._op.eval(leaf=False)
            self._add_edge_cache.clear()

        if reset_cache:
            self.cache.clear()

    def _clear_removing_cache(self):
        reset_cache = bool(
            len(self._remove_node_cache) > 0 or len(self._remove_edge_cache) > 0
        )
        if self._remove_node_cache:
            nodes_to_modify = json.dumps(
                self._remove_node_cache, option=json.OPT_SERIALIZE_NUMPY
            )
            self._op = dag_utils.modify_vertices(
                self, types_pb2.NX_DEL_NODES, nodes_to_modify
            )
            self._op.eval(leaf=False)
            self._remove_node_cache.clear()

        if self._remove_edge_cache:
            edges_to_modify = json.dumps(
                self._remove_edge_cache, option=json.OPT_SERIALIZE_NUMPY
            )
            self._op = dag_utils.modify_edges(
                self, types_pb2.NX_DEL_EDGES, edges_to_modify
            )
            self._op.eval(leaf=False)
            self._remove_edge_cache.clear()

        if reset_cache:
            self.cache.clear()

    def _convert_arrow_to_dynamic(self):
        """Try to convert the hosted graph from arrow_property to dynamic_property.

        Notes
        -------
            the method is implicit called by modification and graph view methods.
        """
        if self.graph_type == graph_def_pb2.ARROW_PROPERTY:
            self._op = dag_utils.arrow_to_dynamic(self)
            graph_def = self._op.eval(leaf=False)
            self._key = graph_def.key
            self._graph_type = graph_def.graph_type

    def _convert_to_label_id_tuple(self, n):
        """Convert the node to (label_id, id) format.
        The input node may be id or (label, id), convert the node
        to tuple (label_id, id) format.

        Notes
        -------
            the method is implicit called by report methods and the hosted graph is
        arrow_property graph.
        """
        if isinstance(n, tuple):
            label_id = n[1]
            new_n = (self._schema.get_vertex_label_id(n[0]), n[1])
            if new_n[0] == self._default_label_id:
                raise KeyError("default label's node must be non-tuple format.")
        elif self._default_label_id == -1:
            # the n is non-tuple, but default id is -1
            raise KeyError("default label id is -1.")
        else:
            label_id = n
            new_n = (self._default_label_id, n)
        if not isinstance(label_id, utils.data_type_to_python(self._schema.oid_type)):
            # id is not oid type
            raise KeyError("the node type is not arrow_property oid_type.")
        return new_n
