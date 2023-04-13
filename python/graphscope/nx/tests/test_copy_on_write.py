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

import pandas as pd
import pytest

import graphscope
import graphscope.nx as nx
from graphscope.framework.loader import Loader
from graphscope.nx.tests.classes.test_digraph import TestDiGraph as _TestDiGraph
from graphscope.nx.tests.classes.test_graph import TestGraph as _TestGraph
from graphscope.nx.tests.utils import almost_equal
from graphscope.nx.utils.misc import graphs_equal
from graphscope.nx.utils.misc import replace_with_inf


def k3_graph(prefix, directed):
    graph = graphscope.g(directed=directed, generate_eid=False, retain_oid=False)
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
    graph = graphscope.g(directed=directed, generate_eid=False, retain_oid=False)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "3v.csv"), delimiter="|"), "vertex"
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "p3_directed.csv"), delimiter="|"),
        "edge",
    )
    return graph


def simple_label_graph(prefix, directed):
    graph = graphscope.g(directed=directed, generate_eid=False, retain_oid=False)
    graph = graph.add_vertices(Loader(os.path.join(prefix, "simple_v_0.csv")), "v-0")
    graph = graph.add_vertices(Loader(os.path.join(prefix, "simple_v_1.csv")), "v-1")
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "simple_e_0.csv")),
        "e-0",
        src_label="v-0",
        dst_label="v-0",
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "simple_e_1.csv")),
        "e-1",
        src_label="v-0",
        dst_label="v-1",
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "simple_e_2.csv")),
        "e-2",
        src_label="v-1",
        dst_label="v-1",
    )
    return graph


def simple_label_multigraph(prefix, directed):
    graph = graphscope.g(directed=directed, generate_eid=False, retain_oid=False)
    graph = graph.add_vertices(Loader(os.path.join(prefix, "simple_v_0.csv")), "v-0")
    graph = graph.add_vertices(Loader(os.path.join(prefix, "simple_v_1.csv")), "v-1")
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "simple_e_0.csv")),
        "e-0",
        src_label="v-0",
        dst_label="v-0",
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "simple_e_1_multiple.csv")),
        "e-1",
        src_label="v-0",
        dst_label="v-1",
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "simple_e_2.csv")),
        "e-2",
        src_label="v-1",
        dst_label="v-1",
    )
    return graph


def p2p_31_graph(prefix, directed):
    graph = graphscope.g(directed=directed, generate_eid=False, retain_oid=False)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "p2p-31.v"), delimiter=" ", header_row=False),
        "vertex",
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "p2p-31.e"), delimiter=" ", header_row=False),
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
        # specify both edges and nodes
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
            else:  # num_workers=2, N.B: diff with _TestGraph, update the order of id
                elist = [
                    (1, 0, {}),
                    (1, 2, {}),
                    (2, 0, {}),
                    (5, 4, {}),
                    (7, 6, {"weight": 2}),
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
        assert graphs_equal(G, GG)
        H.update(G)
        assert graphs_equal(H, G)

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


@pytest.mark.usefixtures("graphscope_session")
class TestDiGraphCopyOnWrite(_TestDiGraph):
    def setup_method(self):
        data_dir = os.path.expandvars("${GS_TEST_DIR}/networkx")
        self.Graph = nx.DiGraph
        # build K3
        self.k3edges = [(0, 1), (0, 2), (1, 2)]
        self.k3nodes = [0, 1, 2]
        self.k3 = k3_graph(data_dir, True)
        self.K3 = nx.DiGraph(self.k3, default_label="vertex")

        self.p3 = p3_graph(data_dir, True)
        self.P3 = nx.DiGraph(self.p3, default_label="vertex")


@pytest.mark.usefixtures("graphscope_session")
class TestBuiltinCopyOnWrite:
    def setup_class(cls):
        data_dir = os.path.expandvars("${GS_TEST_DIR}/networkx")
        p2p_dir = os.path.expandvars("${GS_TEST_DIR}")

        cls.simple = simple_label_graph(data_dir, True)
        cls.multi_simple = simple_label_multigraph(data_dir, True)
        cls.K3 = k3_graph(data_dir, False)
        cls.SG = nx.DiGraph(cls.simple, default_label="v-0")
        cls.SG.pagerank = {
            1: 0.03721197,
            2: 0.05395735,
            3: 0.04150565,
            4: 0.37508082,
            5: 0.20599833,
            6: 0.28624589,
        }
        cls.SG.auth = {
            1: 0.165000,
            2: 0.243018,
            3: 0.078017,
            4: 0.078017,
            5: 0.270943,
            6: 0.165000,
        }
        cls.SG.hub = {
            1: 0.182720,
            2: 0.0,
            3: 0.386437,
            4: 0.248121,
            5: 0.138316,
            6: 0.044404,
        }
        cls.SG.eigen = {
            1: 3.201908045277076e-06,
            2: 6.4038160905537886e-06,
            3: 3.201908045277076e-06,
            5: 0.40044823300165794,
            4: 0.6479356498234745,
            6: 0.6479356498234745,
        }
        cls.SG.katz = {
            1: 0.37871516522035104,
            2: 0.4165866814015425,
            3: 0.37871516522035104,
            5: 0.42126739520601203,
            4: 0.4255225997990211,
            6: 0.4255225997990211,
        }

        cls.p2p_31 = p2p_31_graph(p2p_dir, False)
        cls.P2P = nx.Graph(cls.p2p_31, default_label="vertex")
        cls.P2P.sssp = dict(
            pd.read_csv(
                "{}/p2p-31-sssp".format(os.path.expandvars("${GS_TEST_DIR}")),
                sep=" ",
                header=None,
            ).values
        )

    def test_with_multigraph(self):
        nx.DiGraph(self.multi_simple)

    def test_single_source_dijkstra_path_length(self):
        ret = nx.builtin.single_source_dijkstra_path_length(
            self.SG, source=1, weight="weight"
        )
        assert ret == {1: 0.0, 2: 1.0, 3: 1.0, 4: 3.0, 5: 2.0, 6: 3.0}
        p2p_ans = nx.builtin.single_source_dijkstra_path_length(
            self.P2P, source=6, weight="f2"
        )
        assert replace_with_inf(p2p_ans) == self.P2P.sssp

    def test_wcc(self):
        ret = nx.builtin.weakly_connected_components(self.SG)
        assert ret == {1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0, 5: 0.0, 6: 0.0}

    def test_pagerank(self):
        p = nx.builtin.pagerank(self.SG, alpha=0.9, tol=1.0e-08)
        for n in p:
            assert almost_equal(p[n], self.SG.pagerank[n], places=4)

    def test_hits(self):
        h, a = nx.builtin.hits(self.SG, tol=1.0e-08)
        for n in h:
            assert almost_equal(h[n], self.SG.hub[n], places=4)
            assert almost_equal(a[n], self.SG.auth[n], places=4)

    def test_degree_centrality(self):
        ret = nx.builtin.degree_centrality(self.SG)
        assert ret == {
            1: 0.6,
            2: 0.4,
            3: 0.8,
            5: 0.8,
            4: 0.8,
            6: 0.6,
        }

    def test_eigenvector_centrality(self):
        ret = nx.builtin.eigenvector_centrality(self.SG)
        for n in ret:
            assert almost_equal(ret[n], self.SG.eigen[n], places=12)

    def test_katz_centrality(self):
        ret = nx.builtin.katz_centrality(self.SG)
        for n in ret:
            assert almost_equal(ret[n], self.SG.katz[n], places=12)

    def test_has_path(self):
        assert nx.builtin.has_path(self.SG, source=1, target=6)

    def test_average_shortest_path_length(self):
        assert nx.builtin.average_shortest_path_length(self.SG) == 0.8

    def test_bfs_edges(self):
        ret = nx.builtin.bfs_edges(self.SG, 1, depth_limit=10)
        assert sorted(ret) == [[1, 2], [1, 3], [3, 5], [5, 4], [5, 6]]

    def bfs_tree(self):
        ret = nx.builtin.bfs_tree(self.SG, 1, depth_limit=10)
        assert sorted(ret) == [1, 2, 3, 4, 5, 6]

    def test_k_core(self):
        ret = nx.builtin.k_core(self.SG, k=1)
        assert ret is not None

    def test_clustering(self):
        ret = nx.builtin.clustering(self.SG)
        assert ret == {1: 0.5, 2: 1.0, 3: 0.2, 5: 0.4, 4: 0.5, 6: 1.0}

    def test_triangles(self):
        ret = nx.builtin.triangles(self.K3)
        assert ret == {2: 1, 0: 1, 1: 1}

    def test_average_clustering(self):
        ret = nx.builtin.average_clustering(self.SG)
        assert almost_equal(ret, 0.6, places=4)

    def test_degree_assortativity_coefficient(self):
        ret = nx.builtin.degree_assortativity_coefficient(self.SG)
        assert almost_equal(ret, -0.25000000000000033, places=12)

    def test_node_boundary(self):
        ret = nx.builtin.node_boundary(self.SG, [1, 2])
        assert ret == {3}

    def test_edge_boundary(self):
        ret = nx.builtin.edge_boundary(self.SG, [1, 2])
        assert list(ret) == [(1, 3)]

    def test_attribute_assortativity_coefficient(self):
        ret = nx.builtin.attribute_assortativity_coefficient(self.SG, attribute="attr")
        assert almost_equal(ret, -0.17647058823529418, places=12)

    def test_numeric_assortativity_coefficient(self):
        ret = nx.builtin.numeric_assortativity_coefficient(self.SG, attribute="attr")
        assert almost_equal(ret, 0.5383819020581653, places=12)

    def test_voterank(self):
        gt = [
            9788,
            17325,
            585,
            50445,
            28802,
            2550,
            61511,
            5928,
            29965,
            38767,
            57802,
            52032,
            44619,
            13596,
            59426,
            454,
            58170,
            3544,
            364,
            5530,
        ]
        ans = nx.builtin.voterank(self.P2P, 20)
        assert gt == ans
