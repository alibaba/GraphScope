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

import os

import numpy as np
import pytest
from networkx.classes.tests.test_graph import TestEdgeSubgraph as _TestEdgeSubgraph
from networkx.classes.tests.test_graph import TestGraph as _TestGraph
from networkx.testing.utils import almost_equal
from networkx.testing.utils import assert_graphs_equal

from graphscope import nx


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
        attr.append(1)
        G[1][2]["foo"] = attr
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
        nodes = [3, "n", 3.14, True, False]
        edges = [(3, "n", 1), ("n", 3.14, 3.14), (True, False, True)]
        G.add_nodes_from(nodes)
        G.add_weighted_edges_from(edges)
        nlist = list(G.nodes)
        assert len(nlist) == 5
        for n in nlist:
            assert n in [False, 3, "n", 3.14, True]
        assert G[3]["n"]["weight"] == 1
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

    def test_update(self):
        # specify both edgees and nodes
        G = self.K3.copy()
        G.update(nodes=[3, (4, {"size": 2})], edges=[(4, 5), (6, 7, {"weight": 2})])
        nlist = [
            (0, {}),
            (1, {}),
            (2, {}),
            (3, {}),
            (4, {"size": 2}),
            (5, {}),
            (6, {}),
            (7, {}),
        ]
        assert sorted(G.nodes.data()) == nlist
        if G.is_directed():
            elist = [
                (0, 1, {}),
                (0, 2, {}),
                (1, 0, {}),
                (1, 2, {}),
                (2, 0, {}),
                (2, 1, {}),
                (4, 5, {}),
                (6, 7, {"weight": 2}),
            ]
        else:
            if os.environ.get("DEPLOYMENT", None) == "standalone":
                elist = [
                    (0, 1, {}),
                    (0, 2, {}),
                    (1, 2, {}),
                    (4, 5, {}),
                    (6, 7, {"weight": 2}),
                ]
            else:  # num_workers=2
                elist = [
                    (0, 1, {}),
                    (0, 2, {}),
                    (2, 1, {}),
                    (4, 5, {}),
                    (6, 7, {"weight": 2}),
                ]
        assert sorted(G.edges.data()) == elist
        assert G.graph == {}

        # no keywords -- order is edges, nodes
        G = self.K3.copy()
        G.update([(4, 5), (6, 7, {"weight": 2})], [3, (4, {"size": 2})])
        assert sorted(G.nodes.data()) == nlist
        assert sorted(G.edges.data()) == elist
        assert G.graph == {}

        # update using only a graph
        G = self.Graph()
        G.graph["foo"] = "bar"
        G.add_node(2, data=4)
        G.add_edge(0, 1, weight=0.5)
        GG = G.copy()
        H = self.Graph()
        GG.update(H)
        assert_graphs_equal(G, GG)
        H.update(G)
        assert_graphs_equal(H, G)

        # update nodes only
        H = self.Graph()
        H.update(nodes=[3, 4])
        assert H.nodes ^ {3, 4} == set()
        assert H.size() == 0

        # update edges only
        H = self.Graph()
        H.update(edges=[(3, 4)])
        if H.is_directed():
            assert sorted(H.edges.data()) == [(3, 4, {})]
        else:
            assert sorted(H.edges.data()) in ([(3, 4, {})], [(4, 3, {})])
        assert H.size() == 1

        # No inputs -> exception
        with pytest.raises(nx.NetworkXError):
            nx.Graph().update()

    def test_duplicated_modification(self):
        G = nx.complete_graph(5, create_using=self.Graph)
        ret = nx.builtin.closeness_centrality(G)
        assert ret == {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0}

        # test add node
        G.add_node(5)
        ret = nx.builtin.closeness_centrality(G)
        assert ret == {0: 0.8, 1: 0.8, 2: 0.8, 3: 0.8, 4: 0.8, 5: 0.0}

        # test add edge
        G.add_edge(4, 5)
        ret = nx.builtin.closeness_centrality(G)
        expect1 = {
            0: 0.8,
            1: 0.8,
            2: 0.8,
            3: 0.8,
            4: 0.8,
            5: 0.555556,
        }
        expect2 = {
            0: 0.833333,
            1: 0.833333,
            2: 0.833333,
            3: 0.833333,
            4: 1.0,
            5: 0.555556,
        }
        if G.is_directed():
            for n in ret:
                assert almost_equal(ret[n], expect1[n], places=4)
        else:
            for n in ret:
                assert almost_equal(ret[n], expect2[n], places=4)

        # test remove edge
        G.remove_edge(4, 5)
        ret = nx.builtin.closeness_centrality(G)
        assert ret == {0: 0.8, 1: 0.8, 2: 0.8, 3: 0.8, 4: 0.8, 5: 0.0}

        # test remove node
        G.remove_node(5)
        ret = nx.builtin.closeness_centrality(G)
        assert ret == {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0}

        # test update
        for e in G.edges:
            G.edges[e]["weight"] = 2
        ret = nx.builtin.closeness_centrality(G, weight="weight")
        assert ret == {0: 0.5, 1: 0.5, 2: 0.5, 3: 0.5, 4: 0.5}

        # test copy
        G2 = G.copy()
        ret = nx.builtin.closeness_centrality(G2)
        assert ret == {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0}

        # test reverse
        if G.is_directed():
            rG = G.reverse()
            ret = nx.builtin.closeness_centrality(rG)
            assert ret == {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0}

        # to_directed/to_undirected
        if G.is_directed():
            udG = G.to_undirected()
            ret = nx.builtin.closeness_centrality(udG)
            assert ret == {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0}
        else:
            dG = G.to_directed()
            ret = nx.builtin.closeness_centrality(dG)
            assert ret == {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0}

        # sub_graph
        sG = G.subgraph([0, 1, 2])
        ret = nx.builtin.closeness_centrality(sG)
        assert ret == {0: 1.0, 1: 1.0, 2: 1.0}

        esG = G.edge_subgraph([(0, 1), (1, 2), (2, 3)])
        ret = nx.builtin.closeness_centrality(esG)
        expect1 = {
            0: 0.000,
            1: 0.333333,
            2: 0.444444,
            3: 0.500,
        }
        expect2 = {
            0: 0.5,
            1: 0.75,
            2: 0.75,
            3: 0.5,
        }
        if G.is_directed():
            for n in ret:
                assert almost_equal(ret[n], expect1[n], places=4)
        else:
            for n in ret:
                assert almost_equal(ret[n], expect2[n], places=4)


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
        assert sorted(self.H.edges(data="name")) in (
            [(1, 0, "edge01"), (4, 3, "edge34")],
            [(0, 1, "edge01"), (4, 3, "edge34")],
        )

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
