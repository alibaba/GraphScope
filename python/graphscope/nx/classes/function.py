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

import networkx.classes.function as func

from graphscope.nx.utils.compat import patch_docstring

__all__ = [
    "nodes",
    "edges",
    "degree",
    "degree_histogram",
    "neighbors",
    "number_of_nodes",
    "number_of_edges",
    "density",
    "is_directed",
    "info",
    "freeze",
    "is_frozen",
    "subgraph",
    "induced_subgraph",
    "edge_subgraph",
    "to_directed",
    "to_undirected",
    "add_star",
    "add_path",
    "add_cycle",
    "create_empty_copy",
    "all_neighbors",
    "non_neighbors",
    "non_edges",
    "common_neighbors",
    "set_node_attributes",
    "get_node_attributes",
    "set_edge_attributes",
    "get_edge_attributes",
    "is_weighted",
    "is_negatively_weighted",
    "is_empty",
    "selfloop_edges",
    "nodes_with_selfloops",
    "number_of_selfloops",
]


# forward the NetworkX functions
from networkx.classes.function import add_cycle
from networkx.classes.function import add_path
from networkx.classes.function import add_star
from networkx.classes.function import all_neighbors
from networkx.classes.function import common_neighbors
from networkx.classes.function import create_empty_copy
from networkx.classes.function import degree
from networkx.classes.function import degree_histogram
from networkx.classes.function import density
from networkx.classes.function import edges
from networkx.classes.function import freeze
from networkx.classes.function import get_edge_attributes
from networkx.classes.function import get_node_attributes
from networkx.classes.function import info
from networkx.classes.function import is_directed
from networkx.classes.function import is_empty
from networkx.classes.function import is_frozen
from networkx.classes.function import is_negatively_weighted
from networkx.classes.function import is_weighted
from networkx.classes.function import neighbors
from networkx.classes.function import nodes
from networkx.classes.function import nodes_with_selfloops
from networkx.classes.function import non_edges
from networkx.classes.function import non_neighbors
from networkx.classes.function import number_of_edges
from networkx.classes.function import number_of_nodes
from networkx.classes.function import selfloop_edges
from networkx.classes.function import subgraph
from networkx.classes.function import to_directed
from networkx.classes.function import to_undirected


def induced_subgraph(G, nbunch):
    """Returns a independent deep copy subgraph induced on nbunch.

    The induced subgraph of a graph on a set of nodes N is the
    graph with nodes N and edges from G which have both ends in N.

    Parameters
    ----------
    G : NetworkX Graph
    nbunch : node, container of nodes or None (for all nodes)

    Returns
    -------
    subgraph : SubGraph
        A independent deep copy of the subgraph in `G` induced by the nodes.

    Examples
    --------
    >>> G = nx.path_graph(4)  # or DiGraph, MultiGraph, MultiDiGraph, etc
    >>> H = G.subgraph([0, 1, 2])
    >>> list(H.edges)
    [(0, 1), (1, 2)]
    """
    induced_nodes = G.nbunch_iter(nbunch)
    return G.subgraph(G, induced_nodes)


def edge_subgraph(G, edges):
    """Returns a independent deep copy subgraph induced by the specified edges.

    The induced subgraph contains each edge in `edges` and each
    node incident to any of those edges.

    Parameters
    ----------
    G : NetworkX Graph
    edges : iterable
        An iterable of edges. Edges not present in `G` are ignored.

    Returns
    -------
    subgraph : SubGraph
        A edge-induced subgraph of subgraph of `G`.

    Examples
    --------
    >>> G = nx.path_graph(5)
    >>> H = G.edge_subgraph([(0, 1), (3, 4)])
    >>> list(H.nodes)
    [0, 1, 3, 4]
    >>> list(H.edges)
    [(0, 1), (3, 4)]
    """
    return G.edge_subgraph(edges)


@patch_docstring(func.number_of_selfloops)
def number_of_selfloops(G):
    if G.is_multigraph():
        # we forward the MultiGraph nd MultiDiGraph
        return sum(1 for _ in selfloop_edges(G))
    return G.number_of_selfloops()


@patch_docstring(func.set_node_attributes)
def set_node_attributes(G, values, name=None):
    # Set node attributes based on type of `values`
    if name is not None:  # `values` must not be a dict of dict
        try:  # `values` is a dict
            for n, v in values.items():
                if n in G:
                    dd = G.get_node_data(n)
                    dd[name] = values[n]
                    G.set_node_data(n, dd)
        except AttributeError:  # `values` is a constant
            for n in G:
                dd = G.get_node_data(n)
                dd[name] = values
                G.set_node_data(n, dd)
    else:  # `values` must be dict of dict
        for n, d in values.items():
            if n in G:
                dd = G.get_node_data(n)
                dd.update(d)
                G.set_node_data(n, dd)


@patch_docstring(func.set_edge_attributes)
def set_edge_attributes(G, values, name=None):  # noqa: C901
    if name is not None:
        # `values` does not contain attribute names
        try:
            # if `values` is a dict using `.items()` => {edge: value}
            if G.is_multigraph():
                for (u, v, key), value in values.items():
                    try:
                        G[u][v][key][name] = value
                    except KeyError:
                        pass
            else:
                for (u, v), value in values.items():
                    try:
                        dd = G.get_edge_data(u, v)
                        dd[name] = value
                        G.set_edge_data(u, v, dd)
                    except KeyError:
                        pass
        except AttributeError:
            # treat `values` as a constant
            for u, v, data in G.edges(data=True):
                data[name] = values
    else:
        # `values` consists of doct-of-dict {edge: {attr: value}} shape
        if G.is_multigraph():
            for (u, v, key), d in values.items():
                try:
                    G[u][v][key].update(d)
                except KeyError:
                    pass
        else:
            for (u, v), d in values.items():
                try:
                    dd = G.get_edge_data(u, v)
                    dd.update(d)
                    G.set_edge_data(u, v, dd)
                except KeyError:
                    pass
