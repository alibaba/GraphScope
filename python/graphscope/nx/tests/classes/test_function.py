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

# fmt: off
import pytest
from networkx.classes.tests.test_function import \
    TestCommonNeighbors as _TestCommonNeighbors
from networkx.classes.tests.test_function import TestFunction as _TestFunction
from networkx.testing import assert_edges_equal
from networkx.testing import assert_nodes_equal

from graphscope import nx

# fmt: on


@pytest.mark.usefixtures("graphscope_session")
class TestFunction(_TestFunction):
    def setup_method(self):
        self.G = nx.Graph({0: [1, 2, 3], 1: [1, 2, 0], 4: []}, name="Test")
        self.Gdegree = {0: 3, 1: 2, 2: 2, 3: 1, 4: 0}
        self.Gnodes = list(range(5))
        self.Gedges = [(0, 1), (0, 2), (0, 3), (1, 0), (1, 1), (1, 2)]
        self.DG = nx.DiGraph({0: [1, 2, 3], 1: [1, 2, 0], 4: []})
        self.DGin_degree = {0: 1, 1: 2, 2: 2, 3: 1, 4: 0}
        self.DGout_degree = {0: 3, 1: 3, 2: 0, 3: 0, 4: 0}
        self.DGnodes = list(range(5))
        self.DGedges = [(0, 1), (0, 2), (0, 3), (1, 0), (1, 1), (1, 2)]


class TestCommonNeighbors(_TestCommonNeighbors):
    @classmethod
    def setup_class(cls):
        cls.func = staticmethod(nx.common_neighbors)

        def test_func(G, u, v, expected):
            result = sorted(cls.func(G, u, v))
            assert result == expected

        cls.test = staticmethod(test_func)

    def test_K5(self):
        G = nx.complete_graph(5)
        self.test(G, 0, 1, [2, 3, 4])

    def test_P3(self):
        G = nx.path_graph(3)
        self.test(G, 0, 2, [1])

    def test_S4(self):
        G = nx.star_graph(4)
        self.test(G, 1, 2, [0])

    def test_digraph(self):
        with pytest.raises(nx.NetworkXNotImplemented):
            G = nx.DiGraph()
            G.add_edges_from([(0, 1), (1, 2)])
            self.func(G, 0, 2)

    def test_nonexistent_nodes(self):
        G = nx.complete_graph(5)
        pytest.raises(nx.NetworkXError, nx.common_neighbors, G, 5, 4)
        pytest.raises(nx.NetworkXError, nx.common_neighbors, G, 4, 5)
        pytest.raises(nx.NetworkXError, nx.common_neighbors, G, 5, 6)

    def test_custom1(self):
        """Case of no common neighbors."""
        G = nx.Graph()
        G.add_nodes_from([0, 1])
        self.test(G, 0, 1, [])

    def test_custom2(self):
        """Case of equal nodes."""
        G = nx.complete_graph(4)
        self.test(G, 0, 0, [1, 2, 3])


def test_is_empty():
    graphs = [nx.Graph(), nx.DiGraph()]
    for G in graphs:
        assert nx.is_empty(G)
        G.add_nodes_from(range(5))
        assert nx.is_empty(G)
        G.add_edges_from([(1, 2), (3, 4)])
        assert not nx.is_empty(G)


def test_selfloops():
    graphs = [nx.Graph(), nx.DiGraph()]
    for graph in graphs:
        G = nx.complete_graph(3, create_using=graph)
        G.add_edge(0, 0)
        assert_nodes_equal(nx.nodes_with_selfloops(G), [0])
        assert_edges_equal(nx.selfloop_edges(G), [(0, 0)])
        assert_edges_equal(nx.selfloop_edges(G, data=True), [(0, 0, {})])
        assert nx.number_of_selfloops(G) == 1
        # test selfloop attr
        G.add_edge(1, 1, weight=2)
        assert_edges_equal(
            nx.selfloop_edges(G, data=True), [(0, 0, {}), (1, 1, {"weight": 2})]
        )
        assert_edges_equal(
            nx.selfloop_edges(G, data="weight"), [(0, 0, None), (1, 1, 2)]
        )
        # test removing selfloops behavior vis-a-vis altering a dict while iterating
        G.add_edge(0, 0)
        G.remove_edges_from(nx.selfloop_edges(G))
        if G.is_multigraph():
            G.add_edge(0, 0)
            pytest.raises(
                RuntimeError, G.remove_edges_from, nx.selfloop_edges(G, keys=True)
            )
            G.add_edge(0, 0)
            pytest.raises(
                TypeError, G.remove_edges_from, nx.selfloop_edges(G, data=True)
            )
            G.add_edge(0, 0)
            pytest.raises(
                RuntimeError,
                G.remove_edges_from,
                nx.selfloop_edges(G, data=True, keys=True),
            )
        else:
            G.add_edge(0, 0)
            G.remove_edges_from(nx.selfloop_edges(G, keys=True))
            G.add_edge(0, 0)
            G.remove_edges_from(nx.selfloop_edges(G, data=True))
            G.add_edge(0, 0)
            G.remove_edges_from(nx.selfloop_edges(G, keys=True, data=True))
