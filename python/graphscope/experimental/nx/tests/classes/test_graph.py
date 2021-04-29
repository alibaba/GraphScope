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
from networkx.classes.tests.test_graph import TestEdgeSubgraph as _TestEdgeSubgraph
from networkx.classes.tests.test_graph import TestGraph as _TestGraph

from graphscope.experimental import nx


@pytest.mark.usefixtures("graphscope_session")
class TestGraph(_TestGraph):
    def setup_method(self):
        self.Graph = nx.Graph
        self.k3nodes = [0, 1, 2]
        self.k3edges = [(0, 1), (0, 2), (1, 2)]
        self.K3 = self.Graph()
        self.K3.update(self.k3edges, self.k3nodes)

    def graphs_equal(self, H, G):
        assert G.adj == H.adj
        assert G.nodes == H.nodes
        assert G.graph == H.graph
        assert G.name == H.name
        assert G.adj == H.adj
        if G.is_directed() and H.is_directed():
            assert G.pred == H.pred
            assert G.succ == H.succ

    def shallow_copy_graph_attr(self, H, G):
        assert G.graph["foo"] == H.graph["foo"]
        G.graph["foo"] = "new_foo"
        assert G.graph["foo"] == H.graph["foo"]

    def shallow_copy_node_attr(self, H, G):
        assert G.nodes[0]["foo"] == H.nodes[0]["foo"]
        G.nodes[0]["foo"] = "new_foo"
        assert G.nodes[0]["foo"] == H.nodes[0]["foo"]

    def shallow_copy_edge_attr(self, H, G):
        assert G[1][2]["foo"] == H[1][2]["foo"]
        G[1][2]["foo"] = "new_foo"
        assert G[1][2]["foo"] == H[1][2]["foo"]

    def deepcopy_node_attr(self, H, G):
        assert G.nodes[0]["foo"] == H.nodes[0]["foo"]
        attr = G.nodes[0]["foo"]
        G.nodes[0]["foo"] = attr.append(1)
        assert G.nodes[0]["foo"] != H.nodes[0]["foo"]

    def deepcopy_edge_attr(self, H, G):
        assert G[1][2]["foo"] == H[1][2]["foo"]
        attr = G[1][2]["foo"]
        G[1][2]["foo"] = attr.append(1)
        assert G[1][2]["foo"] != H[1][2]["foo"]

    def test_memory_leak(self):
        pass

    def test_pickle(self):
        pass

    def test_to_undirected(self):
        G = self.K3
        self.add_attributes(G)
        H = G.to_undirected()
        self.is_deepcopy(H, G)

    def test_to_directed(self):
        G = self.K3
        self.add_attributes(G)
        H = G.to_directed()
        self.is_deepcopy(H, G)

    def test_graph_chain(self):
        # subgraph now is fallback with networkx, not view
        G = self.Graph([(0, 1), (1, 2)])
        DG = G.to_directed(as_view=True)
        RDG = DG.reverse(copy=False)
        assert G is DG._graph
        assert DG is RDG._graph

    def test_copy(self):
        G = self.Graph()
        G.add_node(0)
        G.add_edge(1, 2)
        self.add_attributes(G)
        # deep copy
        H = G.copy()
        self.graphs_equal(H, G)

    def test_class_copy(self):
        G = self.Graph()
        G.add_node(0)
        G.add_edge(1, 2)
        self.add_attributes(G)
        # deep copy
        H = G.__class__(G)
        self.graphs_equal(H, G)

    def test_subgraph(self):
        # subgraph now is true subgraph, not view
        G = self.K3
        self.add_attributes(G)
        H = G.subgraph([0, 1, 2, 5])
        self.graphs_equal(H, G)

        H = G.subgraph([0])
        assert H.adj == {0: {}}
        H = G.subgraph([])
        assert H.adj == {}
        assert G.adj != {}

    def test_node_type(self):
        G = self.Graph()
        nodes = [(0, 1), 3, "n", 3.14, True, False]
        edges = [((0, 1), 3, 1), ("n", 3.14, 3.14), (True, False, True)]
        G.add_nodes_from(nodes)
        G.add_weighted_edges_from(edges)
        assert list(G.nodes) == [(0, 1), 3, "n", 3.14, True, False]
        assert G[(0, 1)][3]["weight"] == 1
        assert G["n"][3.14]["weight"] == 3.14
        assert G[True][False]["weight"] == True

    def test_selfloops(self):
        G = self.Graph()
        G.add_edge(0, 0)
        assert G.number_of_edges() == 1
        G.add_edge(0, 1)
        assert G.number_of_selfloops() == 1
        G.add_edge(2, 2)
        assert G.number_of_edges() == 3
        assert G.number_of_selfloops() == 2
        SG = G.subgraph([0, 1])
        assert SG.number_of_edges() == 2
        assert SG.number_of_selfloops() == 1
        ESG = G.edge_subgraph([(0, 0), (2, 2)])
        assert ESG.number_of_edges() == 2
        assert ESG.number_of_selfloops() == 2
        H = G.copy()
        assert H.number_of_selfloops() == 2
        Gv = G.copy(as_view=True)
        assert Gv.number_of_selfloops() == 2
        G.remove_node(0)
        assert G.number_of_selfloops() == 1
        G.remove_edge(2, 2)
        assert G.number_of_selfloops() == 0


@pytest.mark.usefixtures("graphscope_session")
class TestEdgeSubgraph(_TestEdgeSubgraph):
    def setup_method(self):
        # Create a path graph on five nodes.
        G = nx.path_graph(5)
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
        assert [(1, 0, "edge01"), (4, 3, "edge34")] == sorted(self.H.edges(data="name"))

    def test_remove_node(self):
        """Tests that removing a node in the original graph does not

        affect the nodes of the subgraph, is a true subgraph.

        """
        self.G.remove_node(0)
        assert [0, 1, 3, 4] == sorted(self.H.nodes())

    def test_node_attr_dict(self):
        for v in self.H:
            assert self.G.nodes[v] == self.H.nodes[v]
        self.G.nodes[0]["name"] = "foo"
        assert self.G.nodes[0] != self.H.nodes[0]
        self.H.nodes[1]["name"] = "bar"
        assert self.G.nodes[1] != self.H.nodes[1]

    def test_edge_attr_dict(self):
        for u, v in self.H.edges():
            assert self.G.edges[u, v] == self.H.edges[u, v]
        self.G.edges[0, 1]["name"] = "foo"
        assert self.G.edges[0, 1]["name"] != self.H.edges[0, 1]["name"]
        self.H.edges[3, 4]["name"] = "bar"
        assert self.G.edges[3, 4]["name"] != self.H.edges[3, 4]["name"]

    def test_graph_attr_dict(self):
        assert self.G.graph == self.H.graph
