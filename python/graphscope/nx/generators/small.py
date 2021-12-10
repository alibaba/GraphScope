#!/usr/bin/env python3
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

import networkx as nxa

from graphscope import nx
from graphscope.framework.errors import UnimplementedError
from graphscope.nx import NetworkXError
from graphscope.nx.generators.classic import complete_graph
from graphscope.nx.generators.classic import cycle_graph
from graphscope.nx.generators.classic import empty_graph
from graphscope.nx.generators.classic import path_graph
from graphscope.nx.utils.compat import patch_docstring

__all__ = [
    "make_small_graph",
    "LCF_graph",
    "bull_graph",
    "chvatal_graph",
    "cubical_graph",
    "desargues_graph",
    "diamond_graph",
    "dodecahedral_graph",
    "frucht_graph",
    "heawood_graph",
    "house_graph",
    "house_x_graph",
    "icosahedral_graph",
    "krackhardt_kite_graph",
    "moebius_kantor_graph",
    "octahedral_graph",
    "pappus_graph",
    "petersen_graph",
    "sedgewick_maze_graph",
    "tetrahedral_graph",
    "truncated_cube_graph",
    "truncated_tetrahedron_graph",
    "tutte_graph",
]


def make_small_undirected_graph(graph_description, create_using=None):
    """
    Return a small undirected graph described by graph_description.

    See make_small_graph.
    """
    G = empty_graph(0, create_using)
    if G.is_directed():
        raise NetworkXError("Directed Graph not supported")
    return make_small_graph(graph_description, G)


@patch_docstring(nxa.make_small_graph)
def make_small_graph(graph_description, create_using=None):
    if graph_description[0] not in ("adjacencylist", "edgelist"):
        raise NetworkXError("ltype must be either adjacencylist or edgelist")

    ltype = graph_description[0]
    name = graph_description[1]
    n = graph_description[2]

    G = empty_graph(n, create_using)
    nodes = G.nodes()

    if ltype == "adjacencylist":
        adjlist = graph_description[3]
        if len(adjlist) != n:
            raise NetworkXError("invalid graph_description")
        G.add_edges_from([(u - 1, v) for v in nodes for u in adjlist[v]])
    elif ltype == "edgelist":
        edgelist = graph_description[3]
        for e in edgelist:
            v1 = e[0] - 1
            v2 = e[1] - 1
            if v1 < 0 or v1 > n - 1 or v2 < 0 or v2 > n - 1:
                raise NetworkXError("invalid graph_description")
            G.add_edge(v1, v2)
    G.name = name
    return G


@patch_docstring(nxa.LCF_graph)
def LCF_graph(n, shift_list, repeats, create_using=None):
    if n <= 0:
        return empty_graph(0, create_using)

    # start with the n-cycle
    G = cycle_graph(n, create_using)
    if G.is_directed():
        raise NetworkXError("Directed Graph not supported")
    G.name = "LCF_graph"
    nodes = sorted(list(G))

    n_extra_edges = repeats * len(shift_list)
    # edges are added n_extra_edges times
    # (not all of these need be new)
    if n_extra_edges < 1:
        return G

    for i in range(n_extra_edges):
        shift = shift_list[i % len(shift_list)]  # cycle through shift_list
        v1 = nodes[i % n]  # cycle repeatedly through nodes
        v2 = nodes[(i + shift) % n]
        G.add_edge(v1, v2)
    return G


# -------------------------------------------------------------------------------
#   Various small and named graphs
# -------------------------------------------------------------------------------


@patch_docstring(nxa.bull_graph)
def bull_graph(create_using=None):
    description = [
        "adjacencylist",
        "Bull Graph",
        5,
        [[2, 3], [1, 3, 4], [1, 2, 5], [2], [3]],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.chvatal_graph)
def chvatal_graph(create_using=None):
    description = [
        "adjacencylist",
        "Chvatal Graph",
        12,
        [
            [2, 5, 7, 10],
            [3, 6, 8],
            [4, 7, 9],
            [5, 8, 10],
            [6, 9],
            [11, 12],
            [11, 12],
            [9, 12],
            [11],
            [11, 12],
            [],
            [],
        ],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.cubical_graph)
def cubical_graph(create_using=None):
    description = [
        "adjacencylist",
        "Platonic Cubical Graph",
        8,
        [
            [2, 4, 5],
            [1, 3, 8],
            [2, 4, 7],
            [1, 3, 6],
            [1, 6, 8],
            [4, 5, 7],
            [3, 6, 8],
            [2, 5, 7],
        ],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.desargues_graph)
def desargues_graph(create_using=None):
    G = LCF_graph(20, [5, -5, 9, -9], 5, create_using)
    G.name = "Desargues Graph"
    return G


@patch_docstring(nxa.diamond_graph)
def diamond_graph(create_using=None):
    description = [
        "adjacencylist",
        "Diamond Graph",
        4,
        [[2, 3], [1, 3, 4], [1, 2, 4], [2, 3]],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.dodecahedral_graph)
def dodecahedral_graph(create_using=None):
    G = LCF_graph(20, [10, 7, 4, -4, -7, 10, -4, 7, -7, 4], 2, create_using)
    G.name = "Dodecahedral Graph"
    return G


@patch_docstring(nxa.frucht_graph)
def frucht_graph(create_using=None):
    G = cycle_graph(7, create_using)
    G.add_edges_from(
        [
            [0, 7],
            [1, 7],
            [2, 8],
            [3, 9],
            [4, 9],
            [5, 10],
            [6, 10],
            [7, 11],
            [8, 11],
            [8, 9],
            [10, 11],
        ]
    )

    G.name = "Frucht Graph"
    return G


@patch_docstring(nxa.heawood_graph)
def heawood_graph(create_using=None):
    G = LCF_graph(14, [5, -5], 7, create_using)
    G.name = "Heawood Graph"
    return G


def hoffman_singleton_graph():
    """Return the Hoffman-Singleton Graph."""
    raise UnimplementedError("hoffman_singleton_graph not support in graphscope.nx")


@patch_docstring(nxa.house_graph)
def house_graph(create_using=None):
    description = [
        "adjacencylist",
        "House Graph",
        5,
        [[2, 3], [1, 4], [1, 4, 5], [2, 3, 5], [3, 4]],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.house_x_graph)
def house_x_graph(create_using=None):
    description = [
        "adjacencylist",
        "House-with-X-inside Graph",
        5,
        [[2, 3, 4], [1, 3, 4], [1, 2, 4, 5], [1, 2, 3, 5], [3, 4]],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.icosahedral_graph)
def icosahedral_graph(create_using=None):
    description = [
        "adjacencylist",
        "Platonic Icosahedral Graph",
        12,
        [
            [2, 6, 8, 9, 12],
            [3, 6, 7, 9],
            [4, 7, 9, 10],
            [5, 7, 10, 11],
            [6, 7, 11, 12],
            [7, 12],
            [],
            [9, 10, 11, 12],
            [10],
            [11],
            [12],
            [],
        ],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.krackhardt_kite_graph)
def krackhardt_kite_graph(create_using=None):
    description = [
        "adjacencylist",
        "Krackhardt Kite Social Network",
        10,
        [
            [2, 3, 4, 6],
            [1, 4, 5, 7],
            [1, 4, 6],
            [1, 2, 3, 5, 6, 7],
            [2, 4, 7],
            [1, 3, 4, 7, 8],
            [2, 4, 5, 6, 8],
            [6, 7, 9],
            [8, 10],
            [9],
        ],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.moebius_kantor_graph)
def moebius_kantor_graph(create_using=None):
    G = LCF_graph(16, [5, -5], 8, create_using)
    G.name = "Moebius-Kantor Graph"
    return G


@patch_docstring(nxa.octahedral_graph)
def octahedral_graph(create_using=None):
    description = [
        "adjacencylist",
        "Platonic Octahedral Graph",
        6,
        [[2, 3, 4, 5], [3, 4, 6], [5, 6], [5, 6], [6], []],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.pappus_graph)
def pappus_graph():
    G = LCF_graph(18, [5, 7, -7, 7, -7, -5], 3)
    G.name = "Pappus Graph"
    return G


@patch_docstring(nxa.petersen_graph)
def petersen_graph(create_using=None):
    description = [
        "adjacencylist",
        "Petersen Graph",
        10,
        [
            [2, 5, 6],
            [1, 3, 7],
            [2, 4, 8],
            [3, 5, 9],
            [4, 1, 10],
            [1, 8, 9],
            [2, 9, 10],
            [3, 6, 10],
            [4, 6, 7],
            [5, 7, 8],
        ],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.sedgewick_maze_graph)
def sedgewick_maze_graph(create_using=None):
    G = empty_graph(0, create_using)
    G.add_nodes_from(range(8))
    G.add_edges_from([[0, 2], [0, 7], [0, 5]])
    G.add_edges_from([[1, 7], [2, 6]])
    G.add_edges_from([[3, 4], [3, 5]])
    G.add_edges_from([[4, 5], [4, 7], [4, 6]])
    G.name = "Sedgewick Maze"
    return G


@patch_docstring(nxa.tetrahedral_graph)
def tetrahedral_graph(create_using=None):
    G = complete_graph(4, create_using)
    G.name = "Platonic Tetrahedral graph"
    return G


@patch_docstring(nxa.truncated_cube_graph)
def truncated_cube_graph(create_using=None):
    description = [
        "adjacencylist",
        "Truncated Cube Graph",
        24,
        [
            [2, 3, 5],
            [12, 15],
            [4, 5],
            [7, 9],
            [6],
            [17, 19],
            [8, 9],
            [11, 13],
            [10],
            [18, 21],
            [12, 13],
            [15],
            [14],
            [22, 23],
            [16],
            [20, 24],
            [18, 19],
            [21],
            [20],
            [24],
            [22],
            [23],
            [24],
            [],
        ],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G


@patch_docstring(nxa.truncated_tetrahedron_graph)
def truncated_tetrahedron_graph(create_using=None):
    G = path_graph(12, create_using)
    #    G.add_edges_from([(1,3),(1,10),(2,7),(4,12),(5,12),(6,8),(9,11)])
    G.add_edges_from([(0, 2), (0, 9), (1, 6), (3, 11), (4, 11), (5, 7), (8, 10)])
    G.name = "Truncated Tetrahedron Graph"
    return G


@patch_docstring(nxa.tutte_graph)
def tutte_graph(create_using=None):
    description = [
        "adjacencylist",
        "Tutte's Graph",
        46,
        [
            [2, 3, 4],
            [5, 27],
            [11, 12],
            [19, 20],
            [6, 34],
            [7, 30],
            [8, 28],
            [9, 15],
            [10, 39],
            [11, 38],
            [40],
            [13, 40],
            [14, 36],
            [15, 16],
            [35],
            [17, 23],
            [18, 45],
            [19, 44],
            [46],
            [21, 46],
            [22, 42],
            [23, 24],
            [41],
            [25, 28],
            [26, 33],
            [27, 32],
            [34],
            [29],
            [30, 33],
            [31],
            [32, 34],
            [33],
            [],
            [],
            [36, 39],
            [37],
            [38, 40],
            [39],
            [],
            [],
            [42, 45],
            [43],
            [44, 46],
            [45],
            [],
            [],
        ],
    ]
    G = make_small_undirected_graph(description, create_using)
    return G
