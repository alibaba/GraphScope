#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file classic.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/generators/classic.py
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

import itertools
from itertools import accumulate

import networkx as nxa
from networkx.utils import nodes_or_number
from networkx.utils import pairwise

from graphscope import nx
from graphscope.nx import NetworkXError
from graphscope.nx.classes.graph import Graph
from graphscope.nx.utils.compat import patch_docstring

__all__ = [
    "balanced_tree",
    "barbell_graph",
    "binomial_tree",
    "complete_graph",
    "complete_multipartite_graph",
    "circular_ladder_graph",
    "circulant_graph",
    "cycle_graph",
    "dorogovtsev_goltsev_mendes_graph",
    "empty_graph",
    "full_rary_tree",
    "ladder_graph",
    "lollipop_graph",
    "null_graph",
    "path_graph",
    "star_graph",
    "trivial_graph",
    "turan_graph",
    "wheel_graph",
]

# -------------------------------------------------------------------
#   Some Classic Graphs
# -------------------------------------------------------------------


def _tree_edges(n, r):
    if n == 0:
        return
    # helper function for trees
    # yields edges in rooted tree at 0 with n nodes and branching ratio r
    nodes = iter(range(n))
    parents = [next(nodes)]  # stack of max length r
    while parents:
        source = parents.pop(0)
        for i in range(r):
            try:
                target = next(nodes)
                parents.append(target)
                yield source, target
            except StopIteration:
                break


@patch_docstring(nxa.full_rary_tree)
def full_rary_tree(r, n, create_using=None):
    G = empty_graph(n, create_using)
    G.add_edges_from(_tree_edges(n, r))
    return G


@patch_docstring(nxa.balanced_tree)
def balanced_tree(r, h, create_using=None):
    # The number of nodes in the balanced tree is `1 + r + ... + r^h`,
    # which is computed by using the closed-form formula for a geometric
    # sum with ratio `r`. In the special case that `r` is 1, the number
    # of nodes is simply `h + 1` (since the tree is actually a path
    # graph).
    if r == 1:
        n = h + 1
    else:
        # This must be an integer if both `r` and `h` are integers. If
        # they are not, we force integer division anyway.
        n = (1 - r ** (h + 1)) // (1 - r)
    return full_rary_tree(r, n, create_using=create_using)


@patch_docstring(nxa.barbell_graph)
def barbell_graph(m1, m2, create_using=None):
    if m1 < 2:
        raise NetworkXError("Invalid graph description, m1 should be >=2")
    if m2 < 0:
        raise NetworkXError("Invalid graph description, m2 should be >=0")

    # left barbell
    G = complete_graph(m1, create_using)
    if G.is_directed():
        raise NetworkXError("Directed Graph not supported")

    # connecting path
    G.add_nodes_from(range(m1, m1 + m2 - 1))
    if m2 > 1:
        G.add_edges_from(pairwise(range(m1, m1 + m2)))
    # right barbell
    G.add_edges_from(
        (u, v) for u in range(m1 + m2, 2 * m1 + m2) for v in range(u + 1, 2 * m1 + m2)
    )
    # connect it up
    G.add_edge(m1 - 1, m1)
    if m2 > 0:
        G.add_edge(m1 + m2 - 1, m1 + m2)
    return G


@patch_docstring(nxa.binomial_tree)
def binomial_tree(n, create_using=None):
    G = nx.empty_graph(1, create_using)
    N = 1
    for i in range(n):
        # Use G.edges() to ensure 2-tuples. G.edges is 3-tuple for MultiGraph
        edges = [(u + N, v + N) for (u, v) in G.edges()]
        G.add_edges_from(edges)
        G.add_edge(0, N)
        N *= 2
    return G


@nodes_or_number(0)
@patch_docstring(nxa.complete_graph)
def complete_graph(n, create_using=None):
    n_name, nodes = n
    G = empty_graph(n_name, create_using)
    if len(nodes) > 1:
        if G.is_directed():
            edges = itertools.permutations(nodes, 2)
        else:
            edges = itertools.combinations(nodes, 2)
        G.add_edges_from(edges)
    return G


@patch_docstring(nxa.circular_ladder_graph)
def circular_ladder_graph(n, create_using=None):
    G = ladder_graph(n, create_using)
    G.add_edge(0, n - 1)
    G.add_edge(n, 2 * n - 1)
    return G


@patch_docstring(nxa.circulant_graph)
def circulant_graph(n, offsets, create_using=None):
    G = empty_graph(n, create_using)
    for i in range(n):
        for j in offsets:
            G.add_edge(i, (i - j) % n)
            G.add_edge(i, (i + j) % n)
    return G


@nodes_or_number(0)
@patch_docstring(nxa.cycle_graph)
def cycle_graph(n, create_using=None):
    n_orig, nodes = n
    G = empty_graph(nodes, create_using)
    G.add_edges_from(pairwise(nodes))
    G.add_edge(nodes[-1], nodes[0])
    return G


@patch_docstring(nxa.dorogovtsev_goltsev_mendes_graph)
def dorogovtsev_goltsev_mendes_graph(n, create_using=None):
    G = empty_graph(0, create_using)
    if G.is_directed():
        raise NetworkXError("Directed Graph not supported")
    if G.is_multigraph():
        raise NetworkXError("Multigraph not supported")

    G.add_edge(0, 1)
    if n == 0:
        return G
    new_node = 2  # next node to be added
    for i in range(1, n + 1):  # iterate over number of generations.
        last_generation_edges = list(G.edges())
        number_of_edges_in_last_generation = len(last_generation_edges)
        for j in range(0, number_of_edges_in_last_generation):
            G.add_edge(new_node, last_generation_edges[j][0])
            G.add_edge(new_node, last_generation_edges[j][1])
            new_node += 1
    return G


@nodes_or_number(0)
def empty_graph(n=0, create_using=None, default=nx.Graph, **kw):
    """Returns the empty graph with n nodes and zero edges.

    Parameters
    ----------
    n : int or iterable container of nodes (default = 0)
        If n is an integer, nodes are from `range(n)`.
        If n is a container of nodes, those nodes appear in the graph.
    create_using : Graph Instance, Constructor or None
        Indicator of type of graph to return.
        If a Graph-type instance, then clear and use it.
        If None, use the `default` constructor.
        If a constructor, call it to create an empty graph.
    default : Graph constructor (optional, default = nx.Graph)
        The constructor to use if create_using is None.
        If None, then nx.Graph is used.
        This is used when passing an unknown `create_using` value
        through your home-grown function to `empty_graph` and
        you want a default constructor other than nx.Graph.

    Examples
    --------
    >>> G = nx.empty_graph(10)
    >>> G.number_of_nodes()
    10
    >>> G.number_of_edges()
    0
    >>> G = nx.empty_graph("ABC")
    >>> G.number_of_nodes()
    3
    >>> sorted(G)
    ['A', 'B', 'C']

    """
    if create_using is None:
        G = default(**kw)
    elif hasattr(create_using, "_adj"):
        # create_using is a NetworkX style Graph
        create_using.clear()
        G = create_using
    else:
        # try create_using as constructor
        G = create_using(**kw)

    n_name, nodes = n
    G.add_nodes_from(nodes)
    return G


@patch_docstring(nxa.ladder_graph)
def ladder_graph(n, create_using=None):
    G = empty_graph(2 * n, create_using)
    if G.is_directed():
        raise NetworkXError("Directed Graph not supported")
    G.add_edges_from(pairwise(range(n)))
    G.add_edges_from(pairwise(range(n, 2 * n)))
    G.add_edges_from((v, v + n) for v in range(n))
    return G


@nodes_or_number([0, 1])
@patch_docstring(nxa.lollipop_graph)
def lollipop_graph(m, n, create_using=None):
    m, m_nodes = m
    n, n_nodes = n
    M = len(m_nodes)
    N = len(n_nodes)
    if isinstance(m, int):
        n_nodes = [len(m_nodes) + i for i in n_nodes]
    if M < 2:
        raise NetworkXError("Invalid graph description, m should be >=2")
    if N < 0:
        raise NetworkXError("Invalid graph description, n should be >=0")

    # the ball
    G = complete_graph(m_nodes, create_using)
    if G.is_directed():
        raise NetworkXError("Directed Graph not supported")
    # the stick
    G.add_nodes_from(n_nodes)
    if N > 1:
        G.add_edges_from(pairwise(n_nodes))
    # connect ball to stick
    if M > 0 and N > 0:
        G.add_edge(m_nodes[-1], n_nodes[0])
    return G


@patch_docstring(nxa.null_graph)
def null_graph(create_using=None):
    G = empty_graph(0, create_using)
    return G


@nodes_or_number(0)
@patch_docstring(nxa.path_graph)
def path_graph(n, create_using=None):
    n_name, nodes = n
    G = empty_graph(nodes, create_using)
    G.add_edges_from(pairwise(nodes))
    return G


@nodes_or_number(0)
@patch_docstring(nxa.star_graph)
def star_graph(n, create_using=None):
    n_name, nodes = n
    if isinstance(n_name, int):
        nodes = nodes + [n_name]  # there should be n+1 nodes
    first = nodes[0]
    G = empty_graph(nodes, create_using)
    if G.is_directed():
        raise NetworkXError("Directed Graph not supported")
    G.add_edges_from((first, v) for v in nodes[1:])
    return G


def trivial_graph(create_using=None):
    """Return the Trivial graph with one node (with label 0) and no edges."""
    G = empty_graph(1, create_using)
    return G


@patch_docstring(nxa.turan_graph)
def turan_graph(n, r):
    if not 1 <= r <= n:
        raise NetworkXError("Must satisfy 1 <= r <= n")

    partitions = [n // r] * (r - (n % r)) + [n // r + 1] * (n % r)
    G = complete_multipartite_graph(*partitions)
    return G


@nodes_or_number(0)
@patch_docstring(nxa.wheel_graph)
def wheel_graph(n, create_using=None):
    n_name, nodes = n
    if n_name == 0:
        G = empty_graph(0, create_using)
        return G
    G = star_graph(nodes, create_using)
    if len(G) > 2:
        G.add_edges_from(pairwise(nodes[1:]))
        G.add_edge(nodes[-1], nodes[1])
    return G


@patch_docstring(nxa.complete_multipartite_graph)
def complete_multipartite_graph(*subset_sizes):
    # The complete multipartite graph is an undirected simple graph.
    G = Graph()

    if len(subset_sizes) == 0:
        return G

    # set up subsets of nodes
    try:
        extents = pairwise(accumulate((0,) + subset_sizes))
        subsets = [range(start, end) for start, end in extents]
    except TypeError:
        subsets = subset_sizes

    # add nodes with subset attribute
    # while checking that ints are not mixed with iterables
    try:
        for i, subset in enumerate(subsets):
            G.add_nodes_from(subset, subset=i)
    except TypeError:
        raise NetworkXError("Arguments must be all ints or all iterables")

    # Across subsets, all vertices should be adjacent.
    # We can use itertools.combinations() because undirected.
    for subset1, subset2 in itertools.combinations(subsets, 2):
        G.add_edges_from(itertools.product(subset1, subset2))
    return G
