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
import pickle

import pytest
from networkx.classes.tests.test_graph import TestEdgeSubgraph as _TestEdgeSubgraph
from networkx.classes.tests.test_graph import TestGraph as _TestGraph

from graphscope.experimental import nx
from graphscope.experimental.nx.tests.utils import assert_nodes_equal


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
        # subgraph now is fallback with networkx, not view
        G = self.K3
        self.add_attributes(G)
        H = G.subgraph([0, 1, 2, 5])
        self.graphs_equal(H, G)


class TestEdgeSubgraph(_TestEdgeSubgraph):
    def setup_method(self):
        # Create a path graph on five nodes.
        G = nx.path_graph(5)
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
