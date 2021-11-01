#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import functools
import inspect
import json

import networkx.algorithms as nxa
from networkx.utils.decorators import not_implemented_for

import graphscope
from graphscope import nx
from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.errors import InvalidArgumentError
from graphscope.nx.utils.compat import patch_docstring
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2


# decorator function
def project_to_simple(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        graph = args[0]
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Missing graph_type attribute in graph object.")
        elif graph.graph_type in (
            graph_def_pb2.DYNAMIC_PROPERTY,
            graph_def_pb2.ARROW_PROPERTY,
        ):
            if (
                "weight" in inspect.getfullargspec(func)[0]
            ):  # func has 'weight' argument
                weight = kwargs.get("weight", None)
                graph = graph._project_to_simple(e_prop=weight)
            elif "attribute" in inspect.getfullargspec(func)[0]:
                attribute = kwargs.get("attribute", None)
                graph = graph._project_to_simple(v_prop=attribute)
            else:
                graph = graph._project_to_simple()
        return func(graph, *args[1:], **kwargs)

    return wrapper


@project_to_simple
def pagerank(G, alpha=0.85, max_iter=100, tol=1.0e-6):
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
    ctx = graphscope.pagerank_nx(G, alpha, max_iter, tol)
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@project_to_simple
def hits(G, max_iter=100, tol=1.0e-8, normalized=True):
    """Returns HITS hubs and authorities values for nodes.

    The HITS algorithm computes two numbers for a node.
    Authorities estimates the node value based on the incoming links.
    Hubs estimates the node value based on outgoing links.

    Parameters
    ----------
    G : graph
      A networkx graph

    max_iter : integer, optional
      Maximum number of iterations in power method.

    tol : float, optional
      Error tolerance used to check convergence in power method iteration.

    normalized : bool (default=True)
       Normalize results by the sum of all of the values.

    Returns
    -------
    (node, hubs,authorities) : three-column of dataframe
        node containing the hub and authority
       values.


    Examples
    --------
    >>> G = nx.path_graph(4)
    >>> nx.hits(G)

    References
    ----------
    .. [1] A. Langville and C. Meyer,
       "A survey of eigenvector methods of web information retrieval."
       http://citeseer.ist.psu.edu/713792.html
    .. [2] Jon Kleinberg,
       Authoritative sources in a hyperlinked environment
       Journal of the ACM 46 (5): 604-32, 1999.
       doi:10.1145/324133.324140.
       http://www.cs.cornell.edu/home/kleinber/auth.pdf.
    """
    ctx = graphscope.hits(G, tolerance=tol, max_round=max_iter, normalized=normalized)
    df = ctx.to_dataframe({"id": "v.id", "auth": "r.auth", "hub": "r.hub"})
    return (df.set_index("id")["hub"].to_dict(), df.set_index("id")["auth"].to_dict())


@project_to_simple
def degree_centrality(G):
    """Compute the degree centrality for nodes.

    The degree centrality for a node v is the fraction of nodes it
    is connected to.

    Parameters
    ----------
    G : graph
      A networkx graph

    Returns
    -------
    nodes : dataframe
       Dataframe of nodes with degree centrality as the value.

    See Also
    --------
    eigenvector_centrality

    Notes
    -----
    The degree centrality values are normalized by dividing by the maximum
    possible degree in a simple graph n-1 where n is the number of nodes in G.
    """
    ctx = graphscope.degree_centrality(G, centrality_type="both")
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@not_implemented_for("undirected")
@project_to_simple
def in_degree_centrality(G):
    """Compute the in-degree centrality for nodes.

    The in-degree centrality for a node v is the fraction of nodes its
    incoming edges are connected to.

    Parameters
    ----------
    G : graph
        A networkx graph

    Returns
    -------
    nodes : dataframe
        Dataframe of nodes with in-degree centrality as values.

    Raises
    ------
    NetworkXNotImplemented
        If G is undirected.

    See Also
    --------
    degree_centrality, out_degree_centrality

    Notes
    -----
    The degree centrality values are normalized by dividing by the maximum
    possible degree in a simple graph n-1 where n is the number of nodes in G.
    """
    ctx = graphscope.degree_centrality(G, centrality_type="in")
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@not_implemented_for("undirected")
@project_to_simple
def out_degree_centrality(G):
    """Compute the out-degree centrality for nodes.

    The out-degree centrality for a node v is the fraction of nodes its
    outgoing edges are connected to.

    Parameters
    ----------
    G : graph
        A networkx graph

    Returns
    -------
    nodes : dataframe
        Dataframe of nodes with out-degree centrality as values.

    Raises
    ------
    NetworkXNotImplemented
        If G is undirected.

    See Also
    --------
    degree_centrality, in_degree_centrality

    Notes
    -----
    The degree centrality values are normalized by dividing by the maximum
    possible degree in a simple graph n-1 where n is the number of nodes in G.
    """
    ctx = graphscope.degree_centrality(G, centrality_type="out")
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@project_to_simple
def eigenvector_centrality(G, max_iter=100, tol=1e-06, weight=None):
    r"""Compute the eigenvector centrality for the graph `G`.

    Eigenvector centrality computes the centrality for a node based on the
    centrality of its neighbors. The eigenvector centrality for node $i$ is
    the $i$-th element of the vector $x$ defined by the equation

    .. math::

        Ax = \lambda x

    where $A$ is the adjacency matrix of the graph `G` with eigenvalue
    $\lambda$. By virtue of the Perron–Frobenius theorem, there is a unique
    solution $x$, all of whose entries are positive, if $\lambda$ is the
    largest eigenvalue of the adjacency matrix $A$ ([2]_).

    Parameters
    ----------
    G : graph
      A networkx graph

    max_iter : integer, optional (default=100)
      Maximum number of iterations in power method.

    tol : float, optional (default=1.0e-6)
      Error tolerance used to check convergence in power method iteration.

    weight : None or string, optional (default=None)
      If None, that take it as edge attribute 'weight'
      Otherwise holds the name of the edge attribute used as weight.

    Returns
    -------
    nodes : dataframe
       Dataframe of nodes with eigenvector centrality as the value.

    Examples
    --------
    >>> G = nx.path_graph(4)
    >>> centrality = nx.eigenvector_centrality(G)

    See Also
    --------
    eigenvector_centrality_numpy
    hits
    """
    ctx = graphscope.eigenvector_centrality(G, tolerance=tol, max_round=max_iter)
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@project_to_simple
def katz_centrality(
    G,
    alpha=0.1,
    beta=1.0,
    max_iter=100,
    tol=1e-06,
    normalized=True,
    weight=None,
):
    r"""Compute the Katz centrality for the nodes of the graph G.

    Katz centrality computes the centrality for a node based on the centrality
    of its neighbors. It is a generalization of the eigenvector centrality. The
    Katz centrality for node $i$ is

    .. math::

        x_i = \alpha \sum_{j} A_{ij} x_j + \beta,

    where $A$ is the adjacency matrix of graph G with eigenvalues $\lambda$.

    The parameter $\beta$ controls the initial centrality and

    .. math::

        \alpha < \frac{1}{\lambda_{\max}}.

    Katz centrality computes the relative influence of a node within a
    network by measuring the number of the immediate neighbors (first
    degree nodes) and also all other nodes in the network that connect
    to the node under consideration through these immediate neighbors.

    Extra weight can be provided to immediate neighbors through the
    parameter $\beta$.  Connections made with distant neighbors
    are, however, penalized by an attenuation factor $\alpha$ which
    should be strictly less than the inverse largest eigenvalue of the
    adjacency matrix in order for the Katz centrality to be computed
    correctly. More information is provided in [1]_.

    Parameters
    ----------
    G : graph
      A networkx graph.

    alpha : float
      Attenuation factor

    beta : scalar or dictionary, optional (default=1.0)
      Weight attributed to the immediate neighborhood. If not a scalar, the
      dictionary must have an value for every node.

    max_iter : integer, optional (default=1000)
      Maximum number of iterations in power method.

    tol : float, optional (default=1.0e-6)
      Error tolerance used to check convergence in power method iteration.

    normalized : bool, optional (default=True)
      If True normalize the resulting values.

    weight : None or string, optional (default=None)
      If None, that take it as edge attribute 'weight'.
      Otherwise holds the name of the edge attribute used as weight.

    Returns
    -------
    nodes : dataframe
       Dataframe of nodes with Katz centrality as the value.

    Examples
    --------
    >>> import math
    >>> G = nx.path_graph(4)
    >>> phi = (1 + math.sqrt(5)) / 2.0  # largest eigenvalue of adj matrix
    >>> centrality = nx.katz_centrality(G, 1 / phi - 0.01)

    """
    ctx = graphscope.katz_centrality(
        G,
        alpha=alpha,
        beta=beta,
        tolerance=tol,
        max_round=max_iter,
        normalized=normalized,
    )
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@project_to_simple
def has_path(G, source, target):
    """Returns *True* if *G* has a path from *source* to *target*.

    Parameters
    ----------
    G : networkx graph

    source : node
       Starting node for path

    target : node
       Ending node for path
    """
    ctx = AppAssets(algo="sssp_has_path", context="tensor")(G, source, target)
    return ctx.to_numpy("r", axis=0)[0]


@project_to_simple
@patch_docstring(nxa.shortest_path)
def shortest_path(G, source=None, target=None, weight=None):
    return AppAssets(algo="sssp_path", context="tensor")(G, source)


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
    ctx = AppAssets(algo="sssp_projected", context="vertex_data")(G, source)
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@project_to_simple
def average_shortest_path_length(G, weight=None):
    """Returns the average shortest path length.

    The average shortest path length is

    .. math::

       a =\sum_{s,t \in V} \frac{d(s, t)}{n(n-1)}

    where `V` is the set of nodes in `G`,
    `d(s, t)` is the shortest path from `s` to `t`,
    and `n` is the number of nodes in `G`.

    Parameters
    ----------
    G : networkx graph

    weight : None or string, optional (default = None)
       If None, default take as 'weight'.
       If a string, use this edge attribute as the edge weight.


    Examples
    --------
    >>> G = nx.path_graph(5)
    >>> nx.average_shortest_path_length(G)
    2.0

    """
    return graphscope.average_shortest_path_length(G)


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


@project_to_simple
def closeness_centrality(G, weight=None, wf_improved=True):
    r"""Compute closeness centrality for nodes.

    Closeness centrality [1]_ of a node `u` is the reciprocal of the
    average shortest path distance to `u` over all `n-1` reachable nodes.

    .. math::

        C(u) = \frac{n - 1}{\sum_{v=1}^{n-1} d(v, u)},

    where `d(v, u)` is the shortest-path distance between `v` and `u`,
    and `n` is the number of nodes that can reach `u`. Notice that the
    closeness distance function computes the incoming distance to `u`
    for directed graphs. To use outward distance, act on `G.reverse()`.

    Notice that higher values of closeness indicate higher centrality.

    Wasserman and Faust propose an improved formula for graphs with
    more than one connected component. The result is "a ratio of the
    fraction of actors in the group who are reachable, to the average
    distance" from the reachable actors [2]_. You might think this
    scale factor is inverted but it is not. As is, nodes from small
    components receive a smaller closeness value. Letting `N` denote
    the number of nodes in the graph,

    .. math::

        C_{WF}(u) = \frac{n-1}{N-1} \frac{n - 1}{\sum_{v=1}^{n-1} d(v, u)},

    Parameters
    ----------
    G : graph
      A networkx graph

    weight : edge attribute key, optional (default=None)
      Use the specified edge attribute as the edge distance in shortest
      path calculations, if None, every edge is assumed to be one.

    wf_improved : bool, optional (default=True)
      If True, scale by the fraction of nodes reachable. This gives the
      Wasserman and Faust improved formula. For single component graphs
      it is the same as the original formula.

    Returns
    -------
    nodes: dataframe

    References
    ----------
    .. [1] Linton C. Freeman: Centrality in networks: I.
       Conceptual clarification. Social Networks 1:215-239, 1979.
       http://leonidzhukov.ru/hse/2013/socialnetworks/papers/freeman79-centrality.pdf
    .. [2] pg. 201 of Wasserman, S. and Faust, K.,
       Social Network Analysis: Methods and Applications, 1994,
       Cambridge University Press.
    """
    ctx = AppAssets(algo="closeness_centrality", context="vertex_data")(G, wf_improved)
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


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
      The order of the core.  If not specified return the main core.

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


@project_to_simple
def clustering(G):
    r"""Compute the clustering coefficient for nodes.

    For unweighted graphs, the clustering of a node :math:`u`
    is the fraction of possible triangles through that node that exist,

    .. math::

      c_u = \frac{2 T(u)}{deg(u)(deg(u)-1)},

    where :math:`T(u)` is the number of triangles through node :math:`u` and
    :math:`deg(u)` is the degree of :math:`u`.

    For weighted graphs, there are several ways to define clustering [1]_.
    the one used here is defined
    as the geometric average of the subgraph edge weights [2]_,

    .. math::

       c_u = \frac{1}{deg(u)(deg(u)-1))}
             \sum_{vw} (\hat{w}_{uv} \hat{w}_{uw} \hat{w}_{vw})^{1/3}.

    The edge weights :math:`\hat{w}_{uv}` are normalized by the maximum weight
    in the network :math:`\hat{w}_{uv} = w_{uv}/\max(w)`.

    The value of :math:`c_u` is assigned to 0 if :math:`deg(u) < 2`.

    For directed graphs, the clustering is similarly defined as the fraction
    of all possible directed triangles or geometric average of the subgraph
    edge weights for unweighted and weighted directed graph respectively [3]_.

    .. math::

       c_u = \frac{1}{deg^{tot}(u)(deg^{tot}(u)-1) - 2deg^{\leftrightarrow}(u)}
             T(u),

    where :math:`T(u)` is the number of directed triangles through node
    :math:`u`, :math:`deg^{tot}(u)` is the sum of in degree and out degree of
    :math:`u` and :math:`deg^{\leftrightarrow}(u)` is the reciprocal degree of
    :math:`u`.

    Parameters
    ----------
    G : graph

    Returns
    -------
    out : dataframe
       Clustering coefficient at nodes

    Examples
    --------
    >>> G = nx.path_graph(5)
    >>>nx.clustering(G)

    References
    ----------
    .. [1] Generalizations of the clustering coefficient to weighted
       complex networks by J. Saramäki, M. Kivelä, J.-P. Onnela,
       K. Kaski, and J. Kertész, Physical Review E, 75 027105 (2007).
       http://jponnela.com/web_documents/a9.pdf
    .. [2] Intensity and coherence of motifs in weighted complex
       networks by J. P. Onnela, J. Saramäki, J. Kertész, and K. Kaski,
       Physical Review E, 71(6), 065103 (2005).
    .. [3] Clustering in complex directed networks by G. Fagiolo,
       Physical Review E, 76(2), 026107 (2007).
    """
    # FIXME(weibin): clustering now only correct in directed graph.
    # FIXME: nodes and weight not support.
    ctx = graphscope.clustering(G)
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@project_to_simple
def triangles(G, nodes=None):
    """Compute the number of triangles.

    Finds the number of triangles that include a node as one vertex.

    Parameters
    ----------
    G : graph
       A networkx graph

    Returns
    -------
    out : dataframe
       Number of triangles keyed by node label.

    Notes
    -----
    When computing triangles for the entire graph each triangle is counted
    three times, once at each node.  Self loops are ignored.

    """
    # FIXME: nodes not support.
    ctx = graphscope.triangles(G)
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )


@project_to_simple
@patch_docstring(nxa.transitivity)
def transitivity(G):
    # FIXME: nodes not support.
    return AppAssets(algo="transitivity", context="tensor")(G)


@project_to_simple
@patch_docstring(nxa.average_clustering)
def average_clustering(G, nodes=None, count_zeros=True):
    """Compute the average clustering coefficient for the graph G.

    The clustering coefficient for the graph is the average,

    .. math::

       C = \frac{1}{n}\sum_{v \in G} c_v,

    where :math:`n` is the number of nodes in `G`.

    Parameters
    ----------
    G : graph

    Returns
    -------
    avg : float
       Average clustering

    Examples
    --------
    >>> G = nx.complete_graph(5)
    >>> print(nx.average_clustering(G))
    1.0

    Notes
    -----
    This is a space saving routine; it might be faster
    to use the clustering function to get a list and then take the average.

    Self loops are ignored.

    References
    ----------
    .. [1] Generalizations of the clustering coefficient to weighted
       complex networks by J. Saramäki, M. Kivelä, J.-P. Onnela,
       K. Kaski, and J. Kertész, Physical Review E, 75 027105 (2007).
       http://jponnela.com/web_documents/a9.pdf
    .. [2] Marcus Kaiser,  Mean clustering coefficients: the role of isolated
       nodes and leafs on clustering measures for small-world networks.
       https://arxiv.org/abs/0802.2512
    """
    # FIXME: nodes, weight, count_zeros not support.
    ctx = AppAssets(algo="avg_clustering", context="tensor")(G)
    return ctx.to_numpy("r")[0]


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
    ctx = AppAssets(algo="wcc_projected", context="vertex_data")(G)
    return (
        ctx.to_dataframe({"id": "v.id", "component": "r"})
        .set_index("id")["component"]
        .to_dict()
    )


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


@project_to_simple
def node_boundary(G, nbunch1, nbunch2=None):
    """Returns the node boundary of `nbunch1`.

    The *node boundary* of a set *S* with respect to a set *T* is the
    set of nodes *v* in *T* such that for some *u* in *S*, there is an
    edge joining *u* to *v*. If *T* is not specified, it is assumed to
    be the set of all nodes not in *S*.

    Parameters
    ----------
    G : networkx graph

    nbunch1 : iterable
        Iterable of nodes in the graph representing the set of nodes
        whose node boundary will be returned. (This is the set *S* from
        the definition above.)

    nbunch2 : iterable
        Iterable of nodes representing the target (or "exterior") set of
        nodes. (This is the set *T* from the definition above.) If not
        specified, this is assumed to be the set of all nodes in `G`
        not in `nbunch1`.

    Returns
    -------
    list
        The node boundary of `nbunch1` with respect to `nbunch2`.

    Notes
    -----
    Any element of `nbunch` that is not in the graph `G` will be
    ignored.

    `nbunch1` and `nbunch2` are usually meant to be disjoint, but in
    the interest of speed and generality, that is not required here.

    """
    n1json = json.dumps(list(nbunch1))
    if nbunch2:
        n2json = json.dumps(list(nbunch2))
    else:
        n2json = ""
    ctx = AppAssets(algo="node_boundary", context="tensor")(G, n1json, n2json)
    return ctx.to_numpy("r", axis=0).tolist()


@project_to_simple
def edge_boundary(G, nbunch1, nbunch2=None):
    """Returns the edge boundary of `nbunch1`.

    The *edge boundary* of a set *S* with respect to a set *T* is the
    set of edges (*u*, *v*) such that *u* is in *S* and *v* is in *T*.
    If *T* is not specified, it is assumed to be the set of all nodes
    not in *S*.

    Parameters
    ----------
    G : networkx graph

    nbunch1 : iterable
        Iterable of nodes in the graph representing the set of nodes
        whose edge boundary will be returned. (This is the set *S* from
        the definition above.)

    nbunch2 : iterable
        Iterable of nodes representing the target (or "exterior") set of
        nodes. (This is the set *T* from the definition above.) If not
        specified, this is assumed to be the set of all nodes in `G`
        not in `nbunch1`.

    Returns
    -------
    list
        An list of the edges in the boundary of `nbunch1` with
        respect to `nbunch2`.

    Notes
    -----
    Any element of `nbunch` that is not in the graph `G` will be
    ignored.

    `nbunch1` and `nbunch2` are usually meant to be disjoint, but in
    the interest of speed and generality, that is not required here.

    """
    n1json = json.dumps(list(nbunch1))
    if nbunch2:
        n2json = json.dumps(list(nbunch2))
    else:
        n2json = ""
    ctx = AppAssets(algo="edge_boundary", context="tensor")(G, n1json, n2json)
    return ctx.to_numpy("r", axis=0).tolist()


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
    return graphscope.attribute_assortativity_coefficient(G)


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
    return graphscope.numeric_assortativity_coefficient(G)


@project_to_simple
def is_simple_path(G, nodes):
    """Returns True if and only if `nodes` form a simple path in `G`.

    A *simple path* in a graph is a nonempty sequence of nodes in which
    no node appears more than once in the sequence, and each adjacent
    pair of nodes in the sequence is adjacent in the graph.

    Parameters
    ----------
    nodes : list
        A list of one or more nodes in the graph `G`.

    Returns
    -------
    bool
        Whether the given list of nodes represents a simple path in `G`.

    Notes
    -----
    An empty list of nodes is not a path but a list of one node is a
    path. Here's an explanation why.

    This function operates on *node paths*. One could also consider
    *edge paths*. There is a bijection between node paths and edge
    paths.

    The *length of a path* is the number of edges in the path, so a list
    of nodes of length *n* corresponds to a path of length *n* - 1.
    Thus the smallest edge path would be a list of zero edges, the empty
    path. This corresponds to a list of one node.

    To convert between a node path and an edge path, you can use code
    like the following::

        >>> from networkx.utils import pairwise
        >>> nodes = [0, 1, 2, 3]
        >>> edges = list(pairwise(nodes))
        >>> edges
        [(0, 1), (1, 2), (2, 3)]
        >>> nodes = [edges[0][0]] + [v for u, v in edges]
        >>> nodes
        [0, 1, 2, 3]

    Examples
    --------
    >>> G = nx.cycle_graph(4)
    >>> nx.is_simple_path(G, [2, 3, 0])
    True
    >>> nx.is_simple_path(G, [0, 2])
    False

    """
    return graphscope.is_simple_path(G, nodes)


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
    # delte path tail padding
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


@project_to_simple
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

    k : int, optional (default=None)
      If k is not None use k node samples to estimate betweenness.
      The value of k <= n where n is the number of nodes in the graph.
      Higher values give better approximation.

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

    seed : integer, random_state, or None (default)
        Indicator of random number generation state.
        See :ref:`Randomness<randomness>`.
        Note that this is only used if k is not None.

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
    algorithm = "betweenness_centrality"
    if weight is not None:
        algorithm = "betweenness_centrality_generic"
    ctx = AppAssets(algo=algorithm, context="vertex_data")(
        G, normalized=normalized, endpoints=endpoints
    )
    return (
        ctx.to_dataframe({"id": "v.id", "value": "r"})
        .set_index("id")["value"]
        .to_dict()
    )
