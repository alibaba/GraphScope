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

__all__ = [
    "assert_nodes_equal",
    "assert_edges_equal",
    "assert_graphs_equal",
    "almost_equal",
    "replace_with_inf",
]


def almost_equal(x, y, places=7):
    return round(abs(x - y), places) == 0


def replace_with_inf(data):
    for k, v in data.items():
        if v == 1.7976931348623157e308:
            data[k] = float("inf")
    return data


def assert_nodes_equal(nodes1, nodes2):
    # Assumes iterables of nodes, or (node,datadict) tuples
    nlist1 = list(nodes1)
    nlist2 = list(nodes2)
    try:
        d1 = dict(nlist1)
        d2 = dict(nlist2)
    except (ValueError, TypeError):
        d1 = dict.fromkeys(nlist1)
        d2 = dict.fromkeys(nlist2)
    assert d1 == d2


def assert_edges_equal(edges1, edges2):
    # Assumes iterables with u,v nodes as
    # edge tuples (u,v), or
    # edge tuples with data dicts (u,v,d), or
    # edge tuples with keys and data dicts (u,v,k, d)
    from collections import defaultdict

    d1 = defaultdict(dict)
    d2 = defaultdict(dict)
    c1 = 0
    for c1, e in enumerate(edges1):
        u, v = e[0], e[1]
        data = [e[2:]]
        if v in d1[u]:
            data = d1[u][v] + data
        d1[u][v] = data
        d1[v][u] = data
    c2 = 0
    for c2, e in enumerate(edges2):
        u, v = e[0], e[1]
        data = [e[2:]]
        if v in d2[u]:
            data = d2[u][v] + data
        d2[u][v] = data
        d2[v][u] = data
    assert c1 == c2
    # can check one direction because lengths are the same.
    print("d1: ", d1)
    print("d2: ", d2)
    for n, nbrdict in d1.items():
        for nbr, datalist in nbrdict.items():
            assert n in d2
            assert nbr in d2[n]
            d2datalist = d2[n][nbr]
            for data in datalist:
                assert datalist.count(data) == d2datalist.count(data)


def assert_graphs_equal(graph1, graph2):
    assert graph1.adj == graph2.adj
    assert graph1.nodes == graph2.nodes
    assert graph1.graph == graph2.graph
