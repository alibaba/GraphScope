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

import inspect

import networkx.algorithms as nxa
from networkx.utils.decorators import not_implemented_for

import graphscope
from graphscope import nx
from graphscope.framework.app import AppAssets
from graphscope.framework.errors import InvalidArgumentError
from graphscope.nx.utils.compat import patch_docstring
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2


# decorator function
def project_to_simple(func):
    def wrapper(*args, **kwargs):
        graph = args[0]
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Missing graph_type attribute in graph object.")
        elif graph.graph_type == graph_def_pb2.DYNAMIC_PROPERTY:
            if (
                "weight" in inspect.getfullargspec(func)[0]
            ):  # func has 'weight' argument
                weight = kwargs.get("weight", None)
                graph = graph._project_to_simple(e_prop=weight)
            else:
                graph = graph._project_to_simple()
        return func(graph, *args[1:], **kwargs)

    return wrapper


@patch_docstring(nxa.pagerank)
def pagerank(G, alpha=0.85, max_iter=100, tol=1.0e-6):
    raise NotImplementedError


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
    return ctx.to_dataframe({"node": "v.id", "auth": "r.auth", "hub": "r.hub"})


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
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


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
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


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
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


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
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


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
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


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
    return AppAssets(algo="sssp_has_path")(G, source, target)


@project_to_simple
@patch_docstring(nxa.shortest_path)
def shortest_path(G, source=None, target=None, weight=None):
    return AppAssets(algo="sssp_path")(G, source)


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
    ctx = AppAssets(algo="sssp_projected")(G, source)
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


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
    ctx = AppAssets(algo="sssp_average_length")(G)
    return ctx.to_numpy("r", axis=0)[0]


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
    ctx = AppAssets(algo="bfs_generic")(G, source, depth_limit, format="edges")
    return ctx.to_numpy("r", axis=0).tolist()


@project_to_simple
@patch_docstring(nxa.bfs_predecessors)
def bfs_predecessors(G, source, depth_limit=None):
    return AppAssets(algo="bfs_generic")(G, source, depth_limit, format="predecessors")


@project_to_simple
@patch_docstring(nxa.bfs_successors)
def bfs_successors(G, source, depth_limit=None):
    return AppAssets(algo="bfs_generic")(G, source, depth_limit, format="successors")


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
    return AppAssets(algo="all_pairs_shortest_path_length")(G)


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
    ctx = AppAssets(algo="closeness_centrality")(G, wf_improved)
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


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
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


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
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@project_to_simple
@patch_docstring(nxa.transitivity)
def transitivity(G):
    # FIXME: nodes not support.
    return AppAssets(algo="transitivity")(G)


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
    ctx = AppAssets(algo="avg_clustering")(G)
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
    return AppAssets(algo="wcc_projected")(G)
