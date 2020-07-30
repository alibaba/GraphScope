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
from networkx.classes.tests.test_digraph import TestEdgeSubgraph

from graphscope.experimental import nx
from graphscope.experimental.nx.tests.classes.test_graph import TestGraph


@pytest.mark.usefixtures("graphscope_session")
class TestDiGraph(BaseAttrDiGraphTester, TestGraph):
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
        with pytest.raises(nx.NetworkXError):
            G.remove_edge(-1, 0)

    def test_remove_edges_from(self):
        G = self.K3
        G.remove_edges_from([(0, 1)])
        assert G.succ == {0: {2: {}}, 1: {0: {}, 2: {}}, 2: {0: {}, 1: {}}}
        assert G.pred == {0: {1: {}, 2: {}}, 1: {2: {}}, 2: {0: {}, 1: {}}}
        G.remove_edges_from([(0, 0)])  # silent fail


class TestEdgeSubgraph(TestEdgeSubgraph):
    def setup_method(self):
        # Create a doubly-linked path graph on five nodes.
        # G = nx.DiGraph(nx.path_graph(5))
        G = nx.path_graph(5, nx.DiGraph)
        # Add some node, edge, and graph attributes.
        for i in range(5):
            G.nodes[i]["name"] = "node{}".format(i)
        G.edges[0, 1]["name"] = "edge01"
        G.edges[3, 4]["name"] = "edge34"
        G.graph["name"] = "graph"
        # Get the subgraph induced by the first and last edges.
        self.G = G
        self.H = G.edge_subgraph([(0, 1), (3, 4)])

    @pytest.mark.skip(reason="edge_subgraph now is fallback with networkx, not view")
    def test_node_attr_dict(self):
        pass

    @pytest.mark.skip(reason="edge_subgraph now is fallback with networkx, not view")
    def test_edge_attr_dict(self):
        pass

    @pytest.mark.skip(reason="edge_subgraph now is fallback with networkx, not view")
    def test_graph_attr_dict(self):
        pass

    @pytest.mark.skip(reason="edge_subgraph now is fallback with networkx, not view")
    def test_remove_node(self):
        pass
