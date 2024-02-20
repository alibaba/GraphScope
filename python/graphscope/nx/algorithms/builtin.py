# -*- coding: utf-8 -*-
#
# This file is referred and derived from project NetworkX
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

import functools
import inspect
import json

import networkx.algorithms as nxa
from networkx.utils.decorators import not_implemented_for

import graphscope
from graphscope import nx
from graphscope.framework.app import AppAssets
from graphscope.framework.errors import InvalidArgumentError
from graphscope.nx.utils.compat import patch_docstring
from graphscope.proto import graph_def_pb2


# decorator function
def project_to_simple(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        graph = args[0]
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Unsupported graph to project to simple.")
        elif graph.graph_type in (
            graph_def_pb2.DYNAMIC_PROPERTY,
            graph_def_pb2.ARROW_PROPERTY,
        ):
            weight = None
            attribute = None
            if "attribute" in inspect.getfullargspec(func)[0]:
                attribute = kwargs.get("attribute", None)
            if "weight" in inspect.getfullargspec(func)[0]:
                # func has 'weight' argument
                weight = kwargs.get("weight", None)
            graph = graph._project_to_simple(v_prop=attribute, e_prop=weight)
        return func(graph, *args[1:], **kwargs)

    return wrapper


def context_to_dict(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ctx = func(*args, **kwargs)
        graph = args[0]
        if graph.graph_type == graph_def_pb2.ARROW_FLATTENED:
            d = dict()
            df = ctx.to_dataframe(
                {"label_id": "v.label_id", "id": "v.id", "value": "r"}
            )
            vertex_labels = graph.schema.vertex_labels
            for row in df.itertuples():
                if row.label_id != graph._default_label_id:
                    d[(vertex_labels[row.label_id], row.id)] = row.value
                else:
                    d[row.id] = row.value
            return d
        return (
            ctx.to_dataframe({"id": "v.id", "value": "r"})
            .set_index("id")["value"]
            .to_dict()
        )

    return wrapper


@context_to_dict
@project_to_simple
@not_implemented_for("multigraph")
def pagerank(G, alpha=0.85, max_iter=100, tol=1.0e-6, weight="weight"):
    """Returns the PageRank of the nodes in the graph.

    PageRank computes a ranking of the nodes in the graph G based on
    the structure of the incoming links. It was originally designed as
    an algorithm to rank web pages.

    Parameters
    ----------
    G : graph
      A networkx directed graph.

    alpha : float, optional
      Damping parameter for PageRank, default=0.85.

    max_iter : integer, optional
      Maximum number of iterations in power method eigenvalue solver.

    tol : float, optional
      Error tolerance used to check convergence in power method solver.

    Returns
    -------
    pagerank : dataframe
       Dataframe of nodes with PageRank as the value.

    Examples
    --------
    >>> G = nx.DiGraph(nx.path_graph(4))
    >>> pr = nx.pagerank(G, alpha=0.9)

    Notes
    -----
    The eigenvector calculation is done by the power iteration method
    and has no guarantee of convergence.  The iteration will stop after
    an error tolerance of ``len(G) * tol`` has been reached. If the
    number of iterations exceed `max_iter`, computation just complete and
    return the current result.

    The PageRank algorithm was designed for directed graphs but this
    algorithm does not check if the input graph is directed.

    References
    ----------
    .. [1] A. Langville and C. Meyer,
       "A survey of eigenvector methods of web information retrieval."
       http://citeseer.ist.psu.edu/713792.html
    .. [2] Page, Lawrence; Brin, Sergey; Motwani, Rajeev and Winograd, Terry,
       The PageRank citation ranking: Bringing order to the Web. 1999
       http://dbpubs.stanford.edu:8090/pub/showDoc.Fulltext?lang=en&doc=1999-66&format=pdf

    """
    return graphscope.pagerank_nx(G, alpha, max_iter, tol)


@not_implemented_for("multigraph")
@patch_docstring(nxa.hits)
def hits(G, max_iter=100, tol=1.0e-8, nstart=None, normalized=True):
    # TODO(@weibin): raise PowerIterationFailedConvergence if hits fails to converge
    # within the specified number of iterations.
    @project_to_simple
    def _hits(G, max_iter=100, tol=1.0e-8, normalized=True):
        ctx = graphscope.hits(
            G, tolerance=tol, max_round=max_iter, normalized=normalized
        )
        df = ctx.to_dataframe({"id": "v.id", "auth": "r.auth", "hub": "r.hub"})
        return (
            df.set_index("id")["hub"].to_dict(),
            df.set_index("id")["auth"].to_dict(),
        )

    if nstart is not None:
        # forward
        return nxa.hits(G, max_iter, tol, nstart, normalized)
    if max_iter == 0:
        raise nx.PowerIterationFailedConvergence(max_iter)
    if len(G) == 0:
        return {}, {}
    return _hits(G, max_iter, tol, normalized)


hits_scipy = hits


@context_to_dict
@project_to_simple
@patch_docstring(nxa.degree_centrality)
def degree_centrality(G):
    return graphscope.degree_centrality(G, centrality_type="both")


@context_to_dict
@project_to_simple
@not_implemented_for("undirected")
@patch_docstring(nxa.in_degree_centrality)
def in_degree_centrality(G):
    return graphscope.degree_centrality(G, centrality_type="in")


@context_to_dict
@project_to_simple
@not_implemented_for("undirected")
@patch_docstring(nxa.out_degree_centrality)
def out_degree_centrality(G):
    return graphscope.degree_centrality(G, centrality_type="out")


@not_implemented_for("multigraph")
@patch_docstring(nxa.eigenvector_centrality)
def eigenvector_centrality(G, max_iter=100, tol=1e-06, nstart=None, weight=None):
    # TODO(@weibin): raise PowerIterationFailedConvergence if eigenvector fails to converge
    # within the specified number of iterations.
    @context_to_dict
    @project_to_simple
    def _eigenvector_centrality(G, max_iter=100, tol=1e-06, weight=None):
        return graphscope.eigenvector_centrality(
            G, tolerance=tol, max_round=max_iter, weight=weight
        )

    if nstart is not None:
        # forward the nxa.eigenvector_centrality
        return nxa.eigenvector_centrality(G, max_iter, tol, nstart, weight)
    if len(G) == 0:
        raise nx.NetworkXPointlessConcept(
            "cannot compute centrality for the null graph"
        )
    if max_iter == 0:
        raise nx.PowerIterationFailedConvergence(max_iter)
    return _eigenvector_centrality(G, max_iter=max_iter, tol=tol, weight=weight)


eigenvector_centrality_numpy = eigenvector_centrality


@not_implemented_for("multigraph")
@patch_docstring(nxa.katz_centrality)
def katz_centrality(
    G,
    alpha=0.1,
    beta=1.0,
    max_iter=100,
    tol=1e-06,
    nstart=None,
    normalized=True,
    weight=None,
):
    # TODO(@weibin): raise PowerIterationFailedConvergence if katz fails to converge
    # within the specified number of iterations.
    @context_to_dict
    @project_to_simple
    def _katz_centrality(
        G,
        alpha=0.1,
        beta=1.0,
        max_iter=100,
        tol=1e-06,
        normalized=True,
        weight=None,
    ):
        return graphscope.katz_centrality(
            G,
            alpha=alpha,
            beta=beta,
            tolerance=tol,
            max_round=max_iter,
            normalized=normalized,
        )

    if nstart is not None or isinstance(beta, dict):
        # forward the nxa.katz_centrality
        return nxa.katz_centrality(
            G, alpha, beta, max_iter, tol, nstart, normalized, weight
        )
    if len(G) == 0:
        return {}
    if not isinstance(beta, (int, float)):
        raise nx.NetworkXError("beta should be number, not {}".format(type(beta)))
    if max_iter == 0:
        raise nx.PowerIterationFailedConvergence(max_iter)
    return _katz_centrality(
        G,
        alpha=alpha,
        beta=beta,
        tol=tol,
        max_iter=max_iter,
        normalized=normalized,
        weight=weight,
    )


@project_to_simple
@patch_docstring(nxa.has_path)
def has_path(G, source, target):
    ctx = AppAssets(algo="sssp_has_path", context="tensor")(G, source, target)
    return ctx.to_numpy("r", axis=0)[0]


@project_to_simple
@patch_docstring(nxa.shortest_path)
def shortest_path(G, source=None, target=None, weight=None):
    return AppAssets(algo="sssp_path", context="tensor")(G, source)


@context_to_dict
@project_to_simple
def single_source_dijkstra_path_length(G, source, weight=None):
    """Find shortest weighted path lengths in G from a source node.

    Compute the shortest path length between source and all other
    reachable nodes for a weighted graph.

    Parameters
    ----------
    G : networkx graph

    source : node label
       Starting node for path

    weight : string
       the edge weights will be accessed via the
       edge attribute with this key (that is, the weight of the edge
       joining `u` to `v` will be ``G.edges[u, v][weight]``).

    Returns
    -------
    length : dataframe
        Dataframe by node to shortest path length from source.

    Examples
    --------
    >>> G = nx.path_graph(5)
    >>> length = nx.single_source_dijkstra_path_length(G, 0)

    Notes
    -----
    Edge weight attributes must be numerical.
    Distances are calculated as sums of weighted edges traversed.

    """
    return AppAssets(algo="sssp_projected", context="vertex_data")(G, source)


@patch_docstring(nxa.average_shortest_path_length)
def average_shortest_path_length(G, weight=None, method=None):
    @project_to_simple
    def _average_shortest_path_length(G, weight=None):
        return graphscope.average_shortest_path_length(G, weight)

    if method is not None:
        return nxa.average_shortest_path_length(G, weight, method)
    n = len(G)
    # For the special case of the null graph. raise an exception, since
    # there are no paths in the null graph.
    if n == 0:
        msg = (
            "the null graph has no paths, thus there is no average"
            "shortest path length"
        )
        raise nx.NetworkXPointlessConcept(msg)
    # For the special case of the trivial graph, return zero immediately.
    if n == 1:
        return 0

    return _average_shortest_path_length(G, weight=weight)


@project_to_simple
def bfs_edges(G, source, depth_limit=None):
    """edges in a breadth-first-search starting at source.

    Parameters
    ----------
    G : networkx graph

    source : node
       Specify starting node for breadth-first search; this function
       iterates over only those edges in the component reachable from
       this node.

    depth_limit : int, optional(default=len(G))
        Specify the maximum search depth

    Returns
    -------
    edges: list
       A list of edges in the breadth-first-search.

    Examples
    --------
    To get the edges in a breadth-first search::

        >>> G = nx.path_graph(3)
        >>> list(nx.bfs_edges(G, 0))
        [(0, 1), (1, 2)]
        >>> list(nx.bfs_edges(G, source=0, depth_limit=1))
        [(0, 1)]

    """
    # FIXME: reverse not support.
    depth_limit = -1 if depth_limit is None else depth_limit
    ctx = AppAssets(algo="bfs_generic", context="tensor")(
        G, source, depth_limit, format="edges"
    )
    return ctx.to_numpy("r", axis=0).tolist()


@project_to_simple
@patch_docstring(nxa.bfs_predecessors)
def bfs_predecessors(G, source, depth_limit=None):
    return AppAssets(algo="bfs_generic", context="tensor")(
        G, source, depth_limit, format="predecessors"
    )


@project_to_simple
@patch_docstring(nxa.bfs_successors)
def bfs_successors(G, source, depth_limit=None):
    return AppAssets(algo="bfs_generic", context="tensor")(
        G, source, depth_limit, format="successors"
    )


@project_to_simple
def all_pairs_shortest_path_length(G, weight=None):
    """Compute shortest path lengths between all nodes in a graph.

    Parameters
    ----------
    G : networkx graph

    weight : string (defualt=None)
       edge weights will be accessed via the edge attribute with this
       key (that is, the weight of the edge joining `u` to `v` will be
       ``G.edges[u, v][weight]``). If is None, every edge is assume to be one.

    Returns
    -------
     :class:`DynamicVertexDataContext`: A context with each vertex assigned with the shortest distance.
        One can use the context to access node's distance result or iterate by nodes.

    Examples
    --------
    >>> G = nx.path_graph(5)
    >>> length = dict(nx.all_pairs_dijkstra_path_length(G))
    >>> for node in [0, 1, 2, 3, 4]:
    ...     print(f"1 - {node}: {length[1][node]}")
    1 - 0: 1
    1 - 1: 0
    1 - 2: 1
    1 - 3: 2
    1 - 4: 3
    >>> length[3][2]
    1
    >>> length[2][2]
    0

    Notes
    -----
    Edge weight attributes must be numerical.
    Distances are calculated as sums of weighted edges traversed.

    """
    return AppAssets(algo="all_pairs_shortest_path_length", context="vertex_data")(G)


@patch_docstring(nxa.closeness_centrality)
def closeness_centrality(G, u=None, distance=None, wf_improved=True):
    @context_to_dict
    @project_to_simple
    def _closeness_centrality(G, weight=None, wf_improved=True):
        return AppAssets(algo="closeness_centrality", context="vertex_data")(
            G, wf_improved
        )

    if u is not None:
        # forward
        return nxa.closeness_centrality(G, u, distance, wf_improved)
    return _closeness_centrality(G, weight=distance, wf_improved=wf_improved)


@patch_docstring(nxa.bfs_tree)
def bfs_tree(G, source, reverse=False, depth_limit=None):
    """Returns an oriented tree constructed from of a breadth-first-search
    starting at source.

    Parameters
    ----------
    G : networkx graph

    source : node
       Specify starting node for breadth-first search

    depth_limit : int, optional(default=len(G))
        Specify the maximum search depth

    Returns
    -------
    T: networkx DiGraph
       An oriented tree

    Notes
    -----
    Based on http://www.ics.uci.edu/~eppstein/PADS/BFS.py
    by D. Eppstein, July 2004. The modifications
    to allow depth limits based on the Wikipedia article
    "`Depth-limited-search`_".

    .. _Depth-limited-search: https://en.wikipedia.org/wiki/Depth-limited_search

    """
    T = nx.DiGraph()
    T.add_node(source)
    edges_gen = bfs_edges(G, source, depth_limit=depth_limit)
    T.add_edges_from(edges_gen)
    return T


@project_to_simple
def k_core(G, k=None, core_number=None):
    """Returns the k-core of G.

    A k-core is a maximal subgraph that contains nodes of degree k or more.

    Parameters
    ----------
    G : networkx graph
      A graph or directed graph
    k : int, optional
      The order of the core. If not specified return the main core.

    Returns
    -------
    :class:`VertexDataContext`: A context with each vertex assigned with a boolean:
    1 if the vertex satisfies k-core, otherwise 0.

    References
    ----------
    .. [1] An O(m) Algorithm for Cores Decomposition of Networks
       Vladimir Batagelj and Matjaz Zaversnik,  2003.
       https://arxiv.org/abs/cs.DS/0310049
    """

    # FIXME: core number not support.
    return graphscope.k_core(G, k)


@patch_docstring(nxa.clustering)
def clustering(G, nodes=None, weight=None):
    @context_to_dict
    @project_to_simple
    def _clustering(G):
        return graphscope.clustering(G)

    if weight or not isinstance(G, (nx.Graph, nx.DiGraph, graphscope.Graph)):
        # forward networkx.clustering
        return nxa.clustering(G, nodes, weight)
    clusterc = _clustering(G)
    if nodes is not None:
        if not isinstance(nodes, list) and nodes in clusterc:
            return clusterc[nodes]
        else:
            return {n: clusterc[n] for n in nodes}
    return clusterc


@not_implemented_for("directed")
@patch_docstring(nxa.triangles)
def triangles(G, nodes=None):
    @context_to_dict
    @project_to_simple
    def _triangles(G):
        return graphscope.triangles(G)

    if not isinstance(G, (nx.Graph, nx.DiGraph, graphscope.Graph)):
        # forward networkx.triangles
        return nxa.triangles(G, nodes)

    tricnt = _triangles(G)
    if nodes is not None:
        if not isinstance(nodes, list) and nodes in tricnt:
            return tricnt[nodes]
        else:
            return {n: tricnt[n] for n in nodes}
    return tricnt


@project_to_simple
@patch_docstring(nxa.transitivity)
def transitivity(G):
    ctx = AppAssets(algo="transitivity", context="tensor")(G)
    return ctx.to_numpy("r")[0]


@patch_docstring(nxa.average_clustering)
def average_clustering(G, nodes=None, weight=None, count_zeros=True):
    @project_to_simple
    def _average_clustering(G):
        ctx = AppAssets(algo="avg_clustering", context="tensor")(
            G, degree_threshold=1000000000
        )
        return ctx.to_numpy("r")[0]

    if weight is not None:
        # forward to networkx.average_clustering
        return nxa.average_clustering(G, nodes, weight, count_zeros)
    if nodes or not count_zeros or not G.is_directed():
        c = clustering(G, nodes=nodes).values()
        if not count_zeros:
            c = [v for v in c if abs(v) > 0]
        return sum(c) / len(c)
    return _average_clustering(G)


@context_to_dict
@project_to_simple
def weakly_connected_components(G):
    """Generate weakly connected components of G.

    Parameters
    ----------
    G : networkx graph
        A directed graph

    Returns
    -------
    comp :class:`VertexDataContext`: A context with each vertex assigned with a boolean:
        1 if the vertex satisfies k-core, otherwise 0.

    """
    return AppAssets(algo="wcc_projected", context="vertex_data")(G)


@project_to_simple
def degree_assortativity_coefficient(G, x="out", y="in", weight=None):
    """Compute degree assortativity of graph.

    Assortativity measures the similarity of connections
    in the graph with respect to the node degree.

    Parameters
    ----------
    G : NetworkX graph

    x: string ('in','out')
       The degree type for source node (directed graphs only).

    y: string ('in','out')
       The degree type for target node (directed graphs only).

    weighted: bool (True, False)
        weighted graph or unweighted graph

    Returns
    -------
    r : float
       Assortativity of graph by degree.

    Examples
    --------
    >>> G = nx.path_graph(4)
    >>> r = nx.builtin.degree_assortativity_coefficient(G)
    >>> print(f"{r:3.1f}")
    -0.5

    See Also
    --------
    attribute_assortativity_coefficient

    Notes
    -----
    This computes Eq. (21) in Ref. [1]_ , where e is the joint
    probability distribution (mixing matrix) of the degrees.  If G is
    directed than the matrix e is the joint probability of the
    user-specified degree type for the source and target.

    References
    ----------
    .. [1] M. E. J. Newman, Mixing patterns in networks,
       Physical Review E, 67 026126, 2003
    .. [2] Foster, J.G., Foster, D.V., Grassberger, P. & Paczuski, M.
       Edge direction and the structure of networks, PNAS 107, 10815-20 (2010).
    """
    return graphscope.degree_assortativity_coefficient(G, x, y, weight)


@patch_docstring(nxa.node_boundary)
def node_boundary(G, nbunch1, nbunch2=None):
    @project_to_simple
    def _node_boundary(G, nbunch1, nbunch2=None):
        n1json = json.dumps(list(nbunch1))
        if nbunch2 is not None:
            n2json = json.dumps(list(nbunch2))
        else:
            n2json = ""
        ctx = AppAssets(algo="node_boundary", context="tensor")(G, n1json, n2json)
        return set(ctx.to_numpy("r", axis=0).tolist())

    if G.is_multigraph():
        # forward to the NetworkX node_boundary
        return nxa.node_boundary(G, nbunch1, nbunch2)
    return _node_boundary(G, nbunch1, nbunch2)


@patch_docstring(nxa.edge_boundary)
def edge_boundary(G, nbunch1, nbunch2=None, data=False, keys=False, default=None):
    @project_to_simple
    def _boundary(G, nbunch1, nbunch2=None):
        n1json = json.dumps(list(nbunch1))
        if nbunch2:
            n2json = json.dumps(list(nbunch2))
        else:
            n2json = ""
        ctx = AppAssets(algo="edge_boundary", context="tensor")(G, n1json, n2json)
        ret = ctx.to_numpy("r", axis=0).tolist()
        for e in ret:
            yield (e[0], e[1])

    if G.is_multigraph():
        # forward the NetworkX edge boundary
        return nxa.edge_boundary(G, nbunch1, nbunch2, data, keys, default)
    return _boundary(G, nbunch1, nbunch2)


@project_to_simple
def average_degree_connectivity(G, source="in+out", target="in+out", weight=None):
    """Compute the average degree connectivity of graph.

    The average degree connectivity is the average nearest neighbor degree of
    nodes with degree k. For weighted graphs, an analogous measure can
    be computed using the weighted average neighbors degree defined in
    [1]_, for a node `i`, as

    .. math::

        k_{nn,i}^{w} = \frac{1}{s_i} \sum_{j \in N(i)} w_{ij} k_j

    where `s_i` is the weighted degree of node `i`,
    `w_{ij}` is the weight of the edge that links `i` and `j`,
    and `N(i)` are the neighbors of node `i`.

    Parameters
    ----------
    G : NetworkX graph

    source :  "in"|"out"|"in+out" (default:"in+out")
       Directed graphs only. Use "in"- or "out"-degree for source node.

    target : "in"|"out"|"in+out" (default:"in+out"
       Directed graphs only. Use "in"- or "out"-degree for target node.

    weight : string or None, optional (default=None)
       The edge attribute that holds the numerical value used as a weight.
       If None, then each edge has weight 1.

    Returns
    -------
    d : dict
       A dictionary keyed by degree k with the value of average connectivity.

    Raises
    ------
    ValueError
        If either `source` or `target` are not one of 'in',
        'out', or 'in+out'.


    Examples
    --------
    >>> G = nx.Graph()
    >>> G.add_edge(1, 2, weight=3)
    >>> G.add_edges_from([(0, 1), (2, 3)], weight=1)
    >>> nx.builtin.average_degree_connectivity(G)
    {1: 2.0, 2: 1.5}
    >>> nx.builtin.average_degree_connectivity(G, weight="weight")
    {1: 2.0, 2: 1.75}

    References
    ----------
    .. [1] A. Barrat, M. Barthélemy, R. Pastor-Satorras, and A. Vespignani,
       "The architecture of complex weighted networks".
       PNAS 101 (11): 3747–3752 (2004).
    """
    return graphscope.average_degree_connectivity(G, source, target, weight)


@project_to_simple
def attribute_assortativity_coefficient(G, attribute):
    """Compute assortativity for node attributes.

    Assortativity measures the similarity of connections
    in the graph with respect to the given attribute.

    Parameters
    ----------
    G : NetworkX graph

    attribute : string
        Node attribute key

    Returns
    -------
    r: float
       Assortativity of graph for given attribute

    Examples
    --------
    >>> G = nx.Graph()
    >>> G.add_nodes_from([0, 1], color="red")
    >>> G.add_nodes_from([2, 3], color="blue")
    >>> G.add_edges_from([(0, 1), (2, 3)])
    >>> print(nx.builtin.attribute_assortativity_coefficient(G, "color"))
    1.0

    Notes
    -----
    This computes Eq. (2) in Ref. [1]_ , (trace(M)-sum(M^2))/(1-sum(M^2)),
    where M is the joint probability distribution (mixing matrix)
    of the specified attribute.

    References
    ----------
    .. [1] M. E. J. Newman, Mixing patterns in networks,
       Physical Review E, 67 026126, 2003
    """
    return graphscope.attribute_assortativity_coefficient(G, attribute)


@project_to_simple
def numeric_assortativity_coefficient(G, attribute):
    """Compute assortativity for numerical node attributes.

    Assortativity measures the similarity of connections
    in the graph with respect to the given numeric attribute.

    Parameters
    ----------
    G : NetworkX graph

    attribute : string
        Node attribute key.

    Returns
    -------
    r: float
       Assortativity of graph for given attribute

    Examples
    --------
    >>> G = nx.Graph()
    >>> G.add_nodes_from([0, 1], size=2)
    >>> G.add_nodes_from([2, 3], size=3)
    >>> G.add_edges_from([(0, 1), (2, 3)])
    >>> print(nx.builtin.numeric_assortativity_coefficient(G, "size"))
    1.0

    Notes
    -----
    This computes Eq. (21) in Ref. [1]_ , for the mixing matrix
    of the specified attribute.

    References
    ----------
    .. [1] M. E. J. Newman, Mixing patterns in networks
           Physical Review E, 67 026126, 2003
    """
    return graphscope.numeric_assortativity_coefficient(G, attribute)


@patch_docstring(nxa.is_simple_path)
def is_simple_path(G, nodes):
    @project_to_simple
    def _is_simple_path(G, nodes):
        return graphscope.is_simple_path(G, nodes)

    if G.is_multigraph():
        # forward the networkx.is_simple_graph
        return nxa.is_simple_path(G, nodes)
    return _is_simple_path(G, nodes)


def get_all_simple_paths(G, source, target_nodes, cutoff):
    @project_to_simple
    def _all_simple_paths(G, source, target_nodes, cutoff):
        targets_json = json.dumps(target_nodes)
        return AppAssets(algo="all_simple_paths", context="tensor")(
            G, source, targets_json, cutoff
        )

    if not isinstance(target_nodes, list):
        target_nodes = [target_nodes]
    if source not in G or len(target_nodes) != len(list(G.nbunch_iter(target_nodes))):
        raise ValueError("nx.NodeNotFound")
    if cutoff is None:
        cutoff = len(G) - 1
    if cutoff < 1 or source in target_nodes:
        return []
    ctx = _all_simple_paths(G, source, list(set(target_nodes)), cutoff)
    paths = ctx.to_numpy("r", axis=0).tolist()
    if len(paths) == 1:
        if not isinstance(paths[0], list):
            return []
    return paths


def all_simple_paths(G, source, target_nodes, cutoff=None):
    """Generate all simple paths in the graph G from source to target.
    A simple path is a path with no repeated nodes.
    Parameters
    ----------
    G : NetworkX graph
    source : node
       Starting node for path
    target : nodes
       Single node or iterable of nodes at which to end path
    cutoff : integer, optional
        Depth to stop the search. Only paths of length <= cutoff are returned.
    Returns
    -------
    paths: list
       A list that produces lists of simple paths.  If there are no paths
       between the source and target within the given cutoff the list
       is empty.
    Examples
    --------
        >>> G = nx.complete_graph(4)
        >>> print(nx.builtin.all_simple_paths(G, 0, 3))
        ...
        [0, 1, 2, 3]
        [0, 1, 3]
        [0, 2, 1, 3]
        [0, 2, 3]
        [0, 3]

    """

    paths = get_all_simple_paths(G, source, target_nodes, cutoff)
    # delete path tail padding
    for path in paths:
        for i in range(len(path) - 1, -1, -1):
            if path[i] == -1:
                path.pop(i)
            else:
                break
    return paths


def all_simple_edge_paths(G, source, target_nodes, cutoff=None):
    """Generate lists of edges for all simple paths in G from source to target.
    A simple path is a path with no repeated nodes.
    Parameters
    ----------
    G : NetworkX graph
    source : node
       Starting node for path
    target : nodes
       Single node or iterable of nodes at which to end path
    cutoff : integer, optional
        Depth to stop the search. Only paths of length <= cutoff are returned.
    Returns
    -------
    paths: list
       A list that produces lists of simple edge paths.  If there are no paths
       between the source and target within the given cutoff the list
       is empty.
    Examples
    --------
    Print the simple path edges of a Graph::
        >>> g = nx.Graph([(1, 2), (2, 4), (1, 3), (3, 4)])
        >>> print(nx.builtin.all_simple_paths(G, 1, 4))
        [(1, 2), (2, 4)]
        [(1, 3), (3, 4)]

    """

    paths = get_all_simple_paths(G, source, target_nodes, cutoff)
    for path in paths:
        a = ""
        b = ""
        for i in range(len(path) - 1, -1, -1):
            if path[i] == -1:
                a = path.pop(i)
            else:
                b = path.pop(i)
                if a != -1 and a != "":
                    path.insert(i, (b, a))
                a = b
    return paths


def betweenness_centrality(
    G, k=None, normalized=True, weight=None, endpoints=False, seed=None
):
    r"""Compute the shortest-path betweenness centrality for nodes.

    Betweenness centrality of a node $v$ is the sum of the
    fraction of all-pairs shortest paths that pass through $v$

    .. math::

       c_B(v) =\sum_{s,t \in V} \frac{\sigma(s, t|v)}{\sigma(s, t)}

    where $V$ is the set of nodes, $\sigma(s, t)$ is the number of
    shortest $(s, t)$-paths,  and $\sigma(s, t|v)$ is the number of
    those paths  passing through some  node $v$ other than $s, t$.
    If $s = t$, $\sigma(s, t) = 1$, and if $v \in {s, t}$,
    $\sigma(s, t|v) = 0$ [2]_.

    Parameters
    ----------
    G : graph
      A NetworkX graph.

    normalized : bool, optional
      If True the betweenness values are normalized by `2/((n-1)(n-2))`
      for graphs, and `1/((n-1)(n-2))` for directed graphs where `n`
      is the number of nodes in G.

    weight : None or string, optional (default=None)
      If None, all edge weights are considered equal.
      Otherwise holds the name of the edge attribute used as weight.
      Weights are used to calculate weighted shortest paths, so they are
      interpreted as distances.

    endpoints : bool, optional
      If True include the endpoints in the shortest path counts.

    Returns
    -------
    nodes : dictionary
       Dictionary of nodes with betweenness centrality as the value.

    See Also
    --------
    edge_betweenness_centrality
    load_centrality

    Notes
    -----
    The algorithm is from Ulrik Brandes [1]_.
    See [4]_ for the original first published version and [2]_ for details on
    algorithms for variations and related metrics.

    For approximate betweenness calculations set k=#samples to use
    k nodes ("pivots") to estimate the betweenness values. For an estimate
    of the number of pivots needed see [3]_.

    For weighted graphs the edge weights must be greater than zero.
    Zero edge weights can produce an infinite number of equal length
    paths between pairs of nodes.

    The total number of paths between source and target is counted
    differently for directed and undirected graphs. Directed paths
    are easy to count. Undirected paths are tricky: should a path
    from "u" to "v" count as 1 undirected path or as 2 directed paths?

    For betweenness_centrality we report the number of undirected
    paths when G is undirected.

    For betweenness_centrality_subset the reporting is different.
    If the source and target subsets are the same, then we want
    to count undirected paths. But if the source and target subsets
    differ -- for example, if sources is {0} and targets is {1},
    then we are only counting the paths in one direction. They are
    undirected paths but we are counting them in a directed way.
    To count them as undirected paths, each should count as half a path.

    References
    ----------
    .. [1] Ulrik Brandes:
       A Faster Algorithm for Betweenness Centrality.
       Journal of Mathematical Sociology 25(2):163-177, 2001.
       https://doi.org/10.1080/0022250X.2001.9990249
    .. [2] Ulrik Brandes:
       On Variants of Shortest-Path Betweenness
       Centrality and their Generic Computation.
       Social Networks 30(2):136-145, 2008.
       https://doi.org/10.1016/j.socnet.2007.11.001
    .. [3] Ulrik Brandes and Christian Pich:
       Centrality Estimation in Large Networks.
       International Journal of Bifurcation and Chaos 17(7):2303-2318, 2007.
       https://dx.doi.org/10.1142/S0218127407018403
    .. [4] Linton C. Freeman:
       A set of measures of centrality based on betweenness.
       Sociometry 40: 35–41, 1977
       https://doi.org/10.2307/3033543
    """

    @context_to_dict
    @project_to_simple
    def _betweenness_centrality(
        G, k=None, normalized=True, weight=None, endpoints=False, seed=None
    ):
        algorithm = "betweenness_centrality"
        if weight is not None:
            algorithm = "betweenness_centrality_generic"
        return AppAssets(algo=algorithm, context="vertex_data")(
            G, normalized=normalized, endpoints=endpoints
        )

    if not isinstance(G, nx.Graph) or seed is not None:
        return nxa.betweenness_centrality(G, k, normalized, weight, endpoints, seed)
    return _betweenness_centrality(
        G, k=k, normalized=normalized, weight=weight, endpoints=endpoints, seed=seed
    )


@project_to_simple
@not_implemented_for("multigraph")
def voterank(G, num_of_nodes=0):
    """Select a list of influential nodes in a graph using VoteRank algorithm

    VoteRank [1]_ computes a ranking of the nodes in a graph G based on a
    voting scheme. With VoteRank, all nodes vote for each of its in-neighbours
    and the node with the highest votes is elected iteratively. The voting
    ability of out-neighbors of elected nodes is decreased in subsequent turns.

    Note: We treat each edge independently in case of multigraphs.

    Parameters
    ----------
    G : graph
      A networkx directed graph.

    number_of_nodes : integer, optional
        Number of ranked nodes to extract (default all nodes).

    Returns
    -------
    voterank : list
       Ordered list of computed seeds.
       Only nodes with positive number of votes are returned.

    Examples
    --------
    >>> G = nx.DiGraph(nx.path_graph(4))
    >>> pr = nx.voterank(G, num_of_nodes=2)

    References
    ----------
    .. [1] Zhang, J.-X. et al. (2016).
        Identifying a set of influential spreaders in complex networks.
        Sci. Rep. 6, 27823; doi: 10.1038/srep27823.

    """

    ctx = graphscope.voterank(G, num_of_nodes)
    r = ctx.to_dataframe({"id": "v.id", "result": "r"})
    r = r[r["result"] != 0].sort_values(by=["result"])
    return r["id"].tolist()
