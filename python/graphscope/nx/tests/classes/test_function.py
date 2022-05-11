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

import networkx.classes.tests.test_function as func_tests
import pytest
from networkx.utils import edges_equal
from networkx.utils import nodes_equal

from graphscope import nx
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    func_tests, decorators=pytest.mark.usefixtures("graphscope_session")
)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(func_tests.TestFunction)
class TestFunction:
    # subgraph in graphscope.nx is deep copy
    def test_subgraph(self):
        assert (
            self.G.subgraph([0, 1, 2, 4]).adj == nx.subgraph(self.G, [0, 1, 2, 4]).adj
        )
        assert (
            self.DG.subgraph([0, 1, 2, 4]).adj == nx.subgraph(self.DG, [0, 1, 2, 4]).adj
        )
        assert (
            self.G.subgraph([0, 1, 2, 4]).adj
            == nx.induced_subgraph(self.G, [0, 1, 2, 4]).adj
        )
        assert (
            self.DG.subgraph([0, 1, 2, 4]).adj
            == nx.induced_subgraph(self.DG, [0, 1, 2, 4]).adj
        )
        H = nx.induced_subgraph(self.G.subgraph([0, 1, 2, 4]), [0, 1, 4])
        assert H.adj == self.G.subgraph([0, 1, 4]).adj

    @pytest.mark.skip(reason="info api would be deprecated in networkx 3.0")
    def test_info(self):
        pass


@pytest.mark.parametrize(
    "graph_type", (nx.Graph, nx.DiGraph, nx.MultiGraph, nx.MultiDiGraph)
)
@pytest.mark.usefixtures("graphscope_session")
def test_set_node_attributes(graph_type):
    # Test single value
    G = nx.path_graph(3, create_using=graph_type)
    vals = 100
    attr = "hello"
    nx.set_node_attributes(G, vals, attr)
    assert G.nodes[0][attr] == vals
    assert G.nodes[1][attr] == vals
    assert G.nodes[2][attr] == vals

    # Test dictionary
    G = nx.path_graph(3, create_using=graph_type)
    vals = dict(zip(sorted(G.nodes()), range(len(G))))
    attr = "hi"
    nx.set_node_attributes(G, vals, attr)
    assert G.nodes[0][attr] == 0
    assert G.nodes[1][attr] == 1
    assert G.nodes[2][attr] == 2

    # Test dictionary of dictionaries
    G = nx.path_graph(3, create_using=graph_type)
    d = {"hi": 0, "hello": 200}
    vals = dict.fromkeys(G.nodes(), d)
    vals.pop(0)
    nx.set_node_attributes(G, vals)
    assert G.nodes[0] == {}
    assert G.nodes[1]["hi"] == 0
    assert G.nodes[2]["hello"] == 200


@pytest.mark.parametrize("graph_type", (nx.Graph, nx.DiGraph))
@pytest.mark.usefixtures("graphscope_session")
def test_set_edge_attributes(graph_type):
    # Test single value
    G = nx.path_graph(3, create_using=graph_type)
    attr = "hello"
    vals = 3
    nx.set_edge_attributes(G, vals, attr)
    assert G[0][1][attr] == vals
    assert G[1][2][attr] == vals

    # Test multiple values
    G = nx.path_graph(3, create_using=graph_type)
    attr = "hi"
    edges = [(0, 1), (1, 2)]
    vals = dict(zip(edges, range(len(edges))))
    nx.set_edge_attributes(G, vals, attr)
    assert G[0][1][attr] == 0
    assert G[1][2][attr] == 1

    # Test dictionary of dictionaries
    G = nx.path_graph(3, create_using=graph_type)
    d = {"hi": 0, "hello": 200}
    edges = [(0, 1)]
    vals = dict.fromkeys(edges, d)
    nx.set_edge_attributes(G, vals)
    assert G[0][1]["hi"] == 0
    assert G[0][1]["hello"] == 200
    assert G[1][2] == {}


@pytest.mark.parametrize(
    "graph_type", [nx.Graph, nx.DiGraph, nx.MultiGraph, nx.MultiDiGraph]
)
@pytest.mark.usefixtures("graphscope_session")
def test_selfloops(graph_type):
    G = nx.complete_graph(3, create_using=graph_type)
    G.add_edge(0, 0)
    assert nodes_equal(nx.nodes_with_selfloops(G), [0])
    assert edges_equal(nx.selfloop_edges(G), [(0, 0)])
    assert edges_equal(nx.selfloop_edges(G, data=True), [(0, 0, {})])
    assert nx.number_of_selfloops(G) == 1


@pytest.mark.parametrize(
    "graph_type", [nx.Graph, nx.DiGraph, nx.MultiGraph, nx.MultiDiGraph]
)
@pytest.mark.usefixtures("graphscope_session")
def test_selfloop_edges_attr(graph_type):
    G = nx.complete_graph(3, create_using=graph_type)
    G.add_edge(0, 0)
    G.add_edge(1, 1, weight=2)
    assert edges_equal(
        nx.selfloop_edges(G, data=True), [(0, 0, {}), (1, 1, {"weight": 2})]
    )
    assert edges_equal(nx.selfloop_edges(G, data="weight"), [(0, 0, None), (1, 1, 2)])


@pytest.mark.parametrize("graph_type", [nx.Graph, nx.DiGraph])
@pytest.mark.usefixtures("graphscope_session")
def test_selfloops_removal(graph_type):
    G = nx.complete_graph(3, create_using=graph_type)
    G.add_edge(0, 0)
    G.remove_edges_from(nx.selfloop_edges(G, keys=True))
    G.add_edge(0, 0)
    G.remove_edges_from(nx.selfloop_edges(G, data=True))
    G.add_edge(0, 0)
    G.remove_edges_from(nx.selfloop_edges(G, keys=True, data=True))


@pytest.mark.skip(reason="graphscope not support restricted view")
def test_restricted_view(G):
    pass


@pytest.mark.skip(reason="graphscope not support restricted view")
def test_restricted_view_multi(G):
    pass


@pytest.mark.skip(reason="graphscope not support ispath")
def test_ispath(G):
    pass


@pytest.mark.skip(reason="graphscope not support pathweight")
def test_pathweight(G):
    pass
