#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/generators/cograph.py
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

from networkx.generators import random_cograph as _random_cograph
from networkx.utils import py_random_state

import graphscope.nx as nx
from graphscope.nx.utils.compat import patch_docstring

__all__ = ["random_cograph"]


@py_random_state(1)
@patch_docstring(_random_cograph)
def random_cograph(n, seed=None):
    R = nx.empty_graph(1)

    for i in range(n):
        RR = nx.relabel_nodes(R.copy(), lambda x: x + len(R))

        if seed.randint(0, 1) == 0:
            R = full_join(R, RR)
        else:
            R = disjoint_union(R, RR)

    return R


def union(G, H, rename=(None, None), name=None):
    if not G.is_multigraph() == H.is_multigraph():
        raise nx.NetworkXError("G and H must both be graphs or multigraphs.")
    # Union is the same type as G
    R = G.__class__()
    # add graph attributes, H attributes take precedent over G attributes
    R.graph.update(G.graph)
    R.graph.update(H.graph)

    # rename graph to obtain disjoint node labels
    def add_prefix(graph, prefix):
        if prefix is None:
            return graph

        def label(x):
            if isinstance(x, str):
                name = prefix + x
            else:
                name = prefix + repr(x)
            return name

        return nx.relabel_nodes(graph, label)

    G = add_prefix(G, rename[0])
    H = add_prefix(H, rename[1])
    if set(G) & set(H):
        raise nx.NetworkXError(
            "The node sets of G and H are not disjoint.",
            "Use appropriate rename=(Gprefix,Hprefix)" "or use disjoint_union(G,H).",
        )
    if G.is_multigraph():
        G_edges = G.edges(keys=True, data=True)
    else:
        G_edges = G.edges(data=True)
    if H.is_multigraph():
        H_edges = H.edges(keys=True, data=True)
    else:
        H_edges = H.edges(data=True)

    # add nodes
    R.add_nodes_from(G)
    R.add_nodes_from(H)
    # add edges
    R.add_edges_from(G_edges)
    R.add_edges_from(H_edges)
    # add node attributes
    for n in G:
        R.nodes[n].update(G.nodes[n])
    for n in H:
        R.nodes[n].update(H.nodes[n])

    return R


def disjoint_union(G, H):
    R1 = nx.convert_node_labels_to_integers(G)
    R2 = nx.convert_node_labels_to_integers(H, first_label=len(R1))
    R = union(R1, R2)
    R.graph.update(G.graph)
    R.graph.update(H.graph)
    return R


def full_join(G, H, rename=(None, None)):
    R = union(G, H, rename)

    def add_prefix(graph, prefix):
        if prefix is None:
            return graph

        def label(x):
            if isinstance(x, str):
                name = prefix + x
            else:
                name = prefix + repr(x)
            return name

        return nx.relabel_nodes(graph, label)

    G = add_prefix(G, rename[0])
    H = add_prefix(H, rename[1])

    for i in G:
        for j in H:
            R.add_edge(i, j)
    if R.is_directed():
        for i in H:
            for j in G:
                R.add_edge(i, j)

    return R
