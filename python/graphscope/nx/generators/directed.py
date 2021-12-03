# -*- coding: utf-8 -*-
#
# This file directed.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/generators/directed.py
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
"""
Generators for some directed graphs, including growing network (GN) graphs and
scale-free graphs.

"""

from collections import Counter

import networkx as nxa
from networkx.utils import discrete_sequence
from networkx.utils import py_random_state
from networkx.utils import weighted_choice

from graphscope import nx
from graphscope.nx.generators.classic import empty_graph
from graphscope.nx.utils.compat import patch_docstring

__all__ = [
    "gn_graph",
    "gnc_graph",
    "gnr_graph",
    "random_k_out_graph",
    "scale_free_graph",
]


@patch_docstring(nxa.gn_graph)
@py_random_state(3)
def gn_graph(n, kernel=None, create_using=None, seed=None):
    G = empty_graph(1, create_using, default=nx.DiGraph)
    if not G.is_directed():
        raise nx.NetworkXError("create_using must indicate a Directed Graph")

    if kernel is None:

        def kernel(x):
            return x

    if n == 1:
        return G

    G.add_edge(1, 0)  # get started
    ds = [1, 1]  # degree sequence

    for source in range(2, n):
        # compute distribution from kernel and degree
        dist = [kernel(d) for d in ds]
        # choose target from discrete distribution
        target = discrete_sequence(1, distribution=dist, seed=seed)[0]
        G.add_edge(source, target)
        ds.append(1)  # the source has only one link (degree one)
        ds[target] += 1  # add one to the target link degree
    return G


@patch_docstring(nxa.gnr_graph)
@py_random_state(3)
def gnr_graph(n, p, create_using=None, seed=None):
    G = empty_graph(1, create_using, default=nx.DiGraph)
    if not G.is_directed():
        raise nx.NetworkXError("create_using must indicate a Directed Graph")

    if n == 1:
        return G

    for source in range(1, n):
        target = seed.randrange(0, source)
        if seed.random() < p and target != 0:
            target = next(G.successors(target))
        G.add_edge(source, target)
    return G


@patch_docstring(nxa.gnc_graph)
@py_random_state(2)
def gnc_graph(n, create_using=None, seed=None):
    G = empty_graph(1, create_using, default=nx.DiGraph)
    if not G.is_directed():
        raise nx.NetworkXError("create_using must indicate a Directed Graph")

    if n == 1:
        return G

    for source in range(1, n):
        target = seed.randrange(0, source)
        for succ in G.successors(target):
            G.add_edge(source, succ)
        G.add_edge(source, target)
    return G


@patch_docstring(nxa.scale_free_graph)
@py_random_state(7)
def scale_free_graph(
    n,
    alpha=0.41,
    beta=0.54,
    gamma=0.05,
    delta_in=0.2,
    delta_out=0,
    create_using=None,
    seed=None,
):
    def _choose_node(G, distribution, delta, psum):
        cumsum = 0.0
        # normalization
        r = seed.random()
        for n, d in distribution:
            cumsum += (d + delta) / psum
            if r < cumsum:
                break
        return n

    if create_using is None or not hasattr(create_using, "_adj"):
        # start with 3-cycle
        G = nx.empty_graph(3, create_using, default=nx.MultiDiGraph)
        G.add_edges_from([(0, 1), (1, 2), (2, 0)])
    else:
        G = create_using
    if not (G.is_directed() and G.is_multigraph()):
        raise nx.NetworkXError("MultiDiGraph required in create_using")

    if alpha <= 0:
        raise ValueError("alpha must be > 0.")
    if beta <= 0:
        raise ValueError("beta must be > 0.")
    if gamma <= 0:
        raise ValueError("gamma must be > 0.")

    if abs(alpha + beta + gamma - 1.0) >= 1e-9:
        raise ValueError("alpha+beta+gamma must equal 1.")

    number_of_edges = G.number_of_edges()
    while len(G) < n:
        psum_in = number_of_edges + delta_in * len(G)
        psum_out = number_of_edges + delta_out * len(G)
        r = seed.random()
        # random choice in alpha,beta,gamma ranges
        if r < alpha:
            # alpha
            # add new node v
            v = len(G)
            # choose w according to in-degree and delta_in
            w = _choose_node(G, G.in_degree(), delta_in, psum_in)
        elif r < alpha + beta:
            # beta
            # choose v according to out-degree and delta_out
            v = _choose_node(G, G.out_degree(), delta_out, psum_out)
            # choose w according to in-degree and delta_in
            w = _choose_node(G, G.in_degree(), delta_in, psum_in)
        else:
            # gamma
            # choose v according to out-degree and delta_out
            v = _choose_node(G, G.out_degree(), delta_out, psum_out)
            # add new node w
            w = len(G)
        G.add_edge(v, w)
        number_of_edges += 1
    return G


@py_random_state(4)
def random_uniform_k_out_graph(n, k, self_loops=True, with_replacement=True, seed=None):
    """Returns a random `k`-out graph with uniform attachment.

    A random `k`-out graph with uniform attachment is a multidigraph
    generated by the following algorithm. For each node *u*, choose
    `k` nodes *v* uniformly at random (with replacement). Add a
    directed edge joining *u* to *v*.

    Parameters
    ----------
    n : int
        The number of nodes in the returned graph.

    k : int
        The out-degree of each node in the returned graph.

    self_loops : bool
        If True, self-loops are allowed when generating the graph.

    with_replacement : bool
        If True, neighbors are chosen with replacement and the
        returned graph will be a directed multigraph. Otherwise,
        neighbors are chosen without replacement and the returned graph
        will be a directed graph.

    seed : integer, random_state, or None (default)
        Indicator of random number generation state.
        See :ref:`Randomness<randomness>`.

    Returns
    -------
    NetworkX graph
        A `k`-out-regular directed graph generated according to the
        above algorithm. It will be a multigraph if and only if
        `with_replacement` is True.

    Raises
    ------
    ValueError
        If `with_replacement` is False and `k` is greater than
        `n`.

    See also
    --------
    random_k_out_graph

    Notes
    -----
    The return digraph or multidigraph may not be strongly connected, or
    even weakly connected.

    If `with_replacement` is True, this function is similar to
    :func:`random_k_out_graph`, if that function had parameter `alpha`
    set to positive infinity.

    """
    if with_replacement:
        create_using = nx.MultiDiGraph()

        def sample(v, nodes):
            if not self_loops:
                nodes = nodes - {v}
            return (seed.choice(list(nodes)) for i in range(k))

    else:
        create_using = nx.DiGraph()

        def sample(v, nodes):
            if not self_loops:
                nodes = nodes - {v}
            return seed.sample(nodes, k)

    G = nx.empty_graph(n, create_using)
    nodes = set(G)
    for u in G:
        G.add_edges_from((u, v) for v in sample(u, nodes))
    return G


@patch_docstring(nxa.random_k_out_graph)
@py_random_state(4)
def random_k_out_graph(n, k, alpha, self_loops=True, seed=None):
    if alpha < 0:
        raise ValueError("alpha must be positive")
    G = nx.empty_graph(n, create_using=nx.MultiDiGraph)
    weights = Counter({v: alpha for v in G})
    for i in range(k * n):
        u = seed.choice([v for v, d in G.out_degree() if d < k])
        # If self-loops are not allowed, make the source node `u` have
        # weight zero.
        if not self_loops:
            adjustment = Counter({u: weights[u]})
        else:
            adjustment = Counter()
        v = weighted_choice(weights - adjustment, seed=seed)
        G.add_edge(u, v)
        weights[v] += 1
    return G
