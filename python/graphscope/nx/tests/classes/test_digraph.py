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

import pytest
from networkx.classes.tests.test_digraph import BaseAttrDiGraphTester
from networkx.utils import nodes_equal

from graphscope import nx
from graphscope.nx.tests.classes.test_graph import TestEdgeSubgraph as _TestEdgeSubgraph
from graphscope.nx.tests.classes.test_graph import TestGraph as _TestGraph


@pytest.mark.usefixtures("graphscope_session")
class TestDiGraph(BaseAttrDiGraphTester, _TestGraph):
    def setup_method(self):
        self.Graph = nx.DiGraph
        # build K3
        k3edges = [(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)]
        self.k3edges = [(0, 1), (0, 2), (1, 2)]
        self.k3nodes = [0, 1, 2]
        self.K3 = self.Graph()
        self.K3.update(k3edges, self.k3nodes)

        self.P3 = self.Graph()
        self.P3nodes = [0, 1, 2]
        self.P3edges = [(0, 1), (1, 2)]
        self.P3.update(self.P3edges, self.P3nodes)

    def test_to_undirected_reciprocal(self):
        # not support reciprocal in graphscope.nx
        pass

    def test_data_input(self):
        G = self.Graph({1: [2], 2: [1]}, name="test")
        assert G.name == "test"
        assert sorted(G.adj.items()) == [(1, {2: {}}), (2, {1: {}})]
        assert sorted(G.succ.items()) == [(1, {2: {}}), (2, {1: {}})]
        assert sorted(G.pred.items()) == [(1, {2: {}}), (2, {1: {}})]

    def test_add_edge(self):
        G = self.Graph()
        G.add_edge(0, 1)
        assert G.adj == {0: {1: {}}, 1: {}}
        assert G.succ == {0: {1: {}}, 1: {}}
        assert G.pred == {0: {}, 1: {0: {}}}
        G = self.Graph()
        G.add_edge(*(0, 1))
        assert G.adj == {0: {1: {}}, 1: {}}
        assert G.succ == {0: {1: {}}, 1: {}}
        assert G.pred == {0: {}, 1: {0: {}}}

    def test_add_edges_from(self):
        G = self.Graph()
        G.add_edges_from([(0, 1), (0, 2, {"data": 3})], data=2)
        assert G.adj == {0: {1: {"data": 2}, 2: {"data": 3}}, 1: {}, 2: {}}
        assert G.succ == {0: {1: {"data": 2}, 2: {"data": 3}}, 1: {}, 2: {}}
        assert G.pred == {0: {}, 1: {0: {"data": 2}}, 2: {0: {"data": 3}}}

        with pytest.raises(nx.NetworkXError):
            G.add_edges_from([(0,)])  # too few in tuple
        with pytest.raises(nx.NetworkXError):
            G.add_edges_from([(0, 1, 2, 3)])  # too many in tuple
        with pytest.raises(TypeError):
            G.add_edges_from([0])  # not a tuple

    def test_remove_edge(self):
        G = self.K3
        G.remove_edge(0, 1)
        assert G.succ == {0: {2: {}}, 1: {0: {}, 2: {}}, 2: {0: {}, 1: {}}}
        assert G.pred == {0: {1: {}, 2: {}}, 1: {2: {}}, 2: {0: {}, 1: {}}}

    def test_remove_edges_from(self):
        G = self.K3
        G.remove_edges_from([(0, 1)])
        assert G.succ == {0: {2: {}}, 1: {0: {}, 2: {}}, 2: {0: {}, 1: {}}}
        assert G.pred == {0: {1: {}, 2: {}}, 1: {2: {}}, 2: {0: {}, 1: {}}}
        G.remove_edges_from([(0, 0)])  # silent fail

    # replace the nx
    def test_out_edges_data(self):
        G = nx.DiGraph([(0, 1, {"data": 0}), (1, 0, {})])
        assert sorted(G.out_edges(data=True)) == [(0, 1, {"data": 0}), (1, 0, {})]
        assert sorted(G.out_edges(0, data=True)) == [(0, 1, {"data": 0})]
        assert sorted(G.out_edges(data="data")) == [(0, 1, 0), (1, 0, None)]
        assert sorted(G.out_edges(0, data="data")) == [(0, 1, 0)]

    # replace the nx
    def test_in_edges_data(self):
        G = nx.DiGraph([(0, 1, {"data": 0}), (1, 0, {})])
        assert sorted(G.in_edges(data=True)) == [(0, 1, {"data": 0}), (1, 0, {})]
        assert sorted(G.in_edges(1, data=True)) == [(0, 1, {"data": 0})]
        assert sorted(G.in_edges(data="data")) == [(0, 1, 0), (1, 0, None)]
        assert sorted(G.in_edges(1, data="data")) == [(0, 1, 0)]

    # replace the nx
    def test_reverse_copy(self):
        G = nx.DiGraph([(0, 1), (1, 2)])
        R = G.reverse()
        assert sorted(R.edges()) == [(1, 0), (2, 1)]
        R.remove_edge(1, 0)
        assert sorted(R.edges()) == [(2, 1)]
        assert sorted(G.edges()) == [(0, 1), (1, 2)]

    # replace the nx
    def test_reverse_nocopy(self):
        G = nx.DiGraph([(0, 1), (1, 2)])
        R = G.reverse(copy=False)
        assert sorted(R.edges()) == [(1, 0), (2, 1)]
        with pytest.raises(nx.NetworkXError):
            R.remove_edge(1, 0)

    # replace the nx
    def test_to_undirected_as_view(self):
        H = nx.path_graph(2, create_using=self.Graph)
        H2 = H.to_undirected(as_view=True)
        assert H is H2._graph
        assert H2.has_edge(0, 1)
        assert H2.has_edge(1, 0)
        pytest.raises(nx.NetworkXError, H2.add_node, -1)
        pytest.raises(nx.NetworkXError, H2.add_edge, 1, 2)

    # original test use function object as node, here we change to bool and int.
    def test_reverse_hashable(self):
        x = True
        y = False
        G = nx.DiGraph()
        G.add_edge(x, y)
        assert nodes_equal(G.nodes(), G.reverse().nodes())
        assert [(y, x)] == list(G.reverse().edges())


@pytest.mark.usefixtures("graphscope_session")
class TestEdgeSubgraph(_TestEdgeSubgraph):
    def setup_method(self):
        G = nx.path_graph(5, nx.DiGraph)
        # Add some node, edge, and graph attributes.
        for i in range(5):
            G.nodes[i]["name"] = f"node{i}"
        G.edges[0, 1]["name"] = "edge01"
        G.edges[3, 4]["name"] = "edge34"
        G.graph["name"] = "graph"
        # Get the subgraph induced by the first and last edges.
        self.G = G
        self.H = G.edge_subgraph([(0, 1), (3, 4)])

    def test_correct_edges(self):
        """Tests that the subgraph has the correct edges."""
        assert [(0, 1, "edge01"), (3, 4, "edge34")] == sorted(self.H.edges(data="name"))

    def test_pred_succ(self):
        G = nx.DiGraph()
        G.add_edge(0, 1)
        H = G.edge_subgraph([(0, 1)])
        assert list(H.predecessors(0)) == []
        assert list(H.successors(0)) == [1]
        assert list(H.predecessors(1)) == [0]
        assert list(H.successors(1)) == []
