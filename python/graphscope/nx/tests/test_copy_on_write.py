#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os

import pytest
from networkx.testing.utils import assert_graphs_equal

import graphscope
import graphscope.nx as nx
from graphscope.framework.loader import Loader
from graphscope.nx.tests.classes.test_digraph import TestDiGraph as _TestDiGraph
from graphscope.nx.tests.classes.test_graph import TestGraph as _TestGraph
from graphscope.proto.types_pb2 import SRC_LABEL


def k3_graph(prefix, directed):
    graph = graphscope.g(directed=directed, generate_eid=False)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "3v.csv"), delimiter="|"), "vertex"
    )
    if directed:
        graph = graph.add_edges(
            Loader(os.path.join(prefix, "k3_directed.csv"), delimiter="|"),
            "edge",
        )
    else:
        graph = graph.add_edges(
            Loader(os.path.join(prefix, "k3_undirected.csv"), delimiter="|"),
            "edge",
        )
    return graph


def p3_graph(prefix, directed):
    graph = graphscope.g(directed=directed, generate_eid=False)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "3v.csv"), delimiter="|"), "vertex"
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "p3_directed.csv"), delimiter="|"),
        "edge",
    )
    return graph


@pytest.mark.usefixtures("graphscope_session")
class TestGraphCopyOnWrite(_TestGraph):
    def setup_method(self):
        self.Graph = nx.Graph
        self.k3nodes = [0, 1, 2]
        self.k3edges = [(0, 1), (0, 2), (1, 2)]
        data_dir = os.path.expandvars("${GS_TEST_DIR}/networkx")
        self.k3 = k3_graph(data_dir, False)
        self.K3 = nx.Graph(self.k3, default_label="vertex")

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
            elist = [
                (0, 1, {}),
                (2, 0, {}),  # N.B: diff with _TestGraph, update the order of id
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
            assert sorted(H.edges.data()) == [(4, 3, {})]
        assert H.size() == 1

        # No inputs -> exception
        with pytest.raises(nx.NetworkXError):
            nx.Graph().update()


@pytest.mark.usefixtures("graphscope_session")
class TestDiGraphCopyOnWrite(_TestDiGraph):
    def setup_method(self):
        self.Graph = nx.DiGraph
        # build K3
        self.k3edges = [(0, 1), (0, 2), (1, 2)]
        self.k3nodes = [0, 1, 2]
        data_dir = os.path.expandvars("${GS_TEST_DIR}/networkx")
        self.k3 = k3_graph(data_dir, True)
        self.K3 = nx.DiGraph(self.k3, default_label="vertex")

        self.p3 = p3_graph(data_dir, True)
        self.P3 = nx.DiGraph(self.p3, default_label="vertex")


@pytest.mark.usefixtures("graphscope_session")
class TestBuiltinCopyOnWrite:
    def setup_method(self):
        self.Graph = nx.Graph
        self.k3nodes = [0, 1, 2]
        self.k3edges = [(0, 1), (0, 2), (1, 2)]
        data_dir = os.path.expandvars("${GS_TEST_DIR}/networkx")
        self.k3 = k3_graph(data_dir, False)
        self.K3 = nx.Graph(self.k3, default_label="vertex")

        self.p3 = p3_graph(data_dir, True)
        self.P3 = nx.DiGraph(self.p3, default_label="vertex")

    def test_single_source_dijkstra_path_length(self):
        ret = nx.builtin.single_source_dijkstra_path_length(
            self.K3, source=0, weight="weight"
        )
        assert dict(ret.values) == {0.0: 0.0, 1.0: 1.0, 2.0: 1.0}

    def test_wcc(self):
        ret = nx.builtin.weakly_connected_components(self.K3)

    def test_pagerank(self):
        ret = nx.builtin.pagerank(self.K3)

    def test_hits(self):
        ret = nx.builtin.hits(self.K3)

    def test_degree_centrality(self):
        ret = nx.builtin.degree_centrality(self.K3)
        print(ret)

    def test_eigenvector_centrality(self):
        ret = nx.builtin.eigenvector_centrality(self.K3)
        print(ret)

    def test_katz_centrality(self):
        ret = nx.builtin.katz_centrality(self.K3)
        print(ret)

    def test_has_path(self):
        assert nx.builtin.has_path(self.K3, source=0, target=2)

    @pytest.mark.skip(reason="grape::VertexDenseSet")
    def test_average_shortest_path_length(self):
        ret = nx.builtin.average_shortest_path_length(self.K3)
        print(ret)

    def test_bfs_edges(self):
        ret = nx.builtin.bfs_edges(self.K3, 0, 10)
        print(ret)

    def bfs_tree(self):
        ret = nx.builtin.bfs_tree(self.K3, 0, depth_limit=10)
        print(ret)

    @pytest.mark.skip(reason="VertexDenseSet")
    def test_k_core(self):
        ret = nx.builtin.k_core(self.K3, k=1)
        print(ret)

    def test_clustering(self):
        ret = nx.builtin.clustering(self.K3)
        print(ret)

    @pytest.mark.skip(reason="VertexDenseSet")
    def test_triangles(self):
        ret = nx.builtin.triangles(self.K3)
        print(ret)

    def test_average_clustering(self):
        ret = nx.builtin.average_clustering(self.K3)
        print(ret)

    def test_degree_assortativity_coefficient(self):
        ret = nx.builtin.degree_assortativity_coefficient(self.K3)
        print(ret)

    def test_node_boundary(self):
        ret = nx.builtin.node_boundary(self.K3, [0, 1])
        print(ret)

    def test_edge_boundary(self):
        ret = nx.builtin.edge_boundary(self.K3, [0, 1])
        print(ret)

    def test_attribute_assortativity_coefficient(self):
        ret = nx.builtin.attribute_assortativity_coefficient(
            self.K3, attribute="weight"
        )
        print(ret)

    def test_numeric_assortativity_coefficient(self):
        ret = nx.builtin.numeric_assortativity_coefficient(self.K3, attribute="weight")
        print(ret)
