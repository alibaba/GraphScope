#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file graphviews.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/classes/graphviews.py
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

import networkx as nx
from networkx.classes.coreviews import FilterAdjacency
from networkx.classes.coreviews import FilterAtlas
from networkx.classes.coreviews import FilterMultiAdjacency
from networkx.classes.coreviews import UnionAdjacency
from networkx.classes.coreviews import UnionMultiAdjacency
from networkx.classes.filters import no_filter
from networkx.exception import NetworkXError
from networkx.utils import not_implemented_for

__all__ = ["generic_graph_view", "subgraph_view", "reverse_view"]


def generic_graph_view(G, create_using=None):
    if create_using is None:
        newG = G.__class__(create_empty_in_engine=False)
    else:
        newG = create_using(create_empty_in_engine=False)
    if G.is_multigraph() != newG.is_multigraph():
        raise NetworkXError("Multigraph for G must agree with create_using")
    newG = nx.freeze(newG)

    # create view by assigning attributes from G
    newG._graph = G
    newG.graph = G.graph

    newG._node = G._node
    if newG.is_directed():
        if G.is_directed():
            newG._succ = G._succ
            newG._pred = G._pred
            newG._adj = G._succ
        else:
            newG._succ = G._adj
            newG._pred = G._adj
            newG._adj = G._adj
    elif G.is_directed():
        if G.is_multigraph():
            newG._adj = UnionMultiAdjacency(G._succ, G._pred)
        else:
            newG._adj = UnionAdjacency(G._succ, G._pred)
    else:
        newG._adj = G._adj
    return newG


def subgraph_view(G, filter_node=no_filter, filter_edge=no_filter):
    newG = nx.freeze(G.__class__())
    newG._NODE_OK = filter_node
    newG._EDGE_OK = filter_edge

    # create view by assigning attributes from G
    newG._graph = G
    newG.graph = G.graph

    newG._node = FilterAtlas(G._node, filter_node)
    if G.is_multigraph():
        Adj = FilterMultiAdjacency

        def reverse_edge(u, v, k):
            return filter_edge(v, u, k)

    else:
        Adj = FilterAdjacency

        def reverse_edge(u, v):
            return filter_edge(v, u)

    if G.is_directed():
        newG._succ = Adj(G._succ, filter_node, filter_edge)
        newG._pred = Adj(G._pred, filter_node, reverse_edge)
        newG._adj = newG._succ
    else:
        newG._adj = Adj(G._adj, filter_node, filter_edge)
    return newG


@not_implemented_for("undirected")
def reverse_view(G):
    newG = generic_graph_view(G)
    newG._succ, newG._pred = G._pred, G._succ
    newG._adj = newG._succ
    return newG
