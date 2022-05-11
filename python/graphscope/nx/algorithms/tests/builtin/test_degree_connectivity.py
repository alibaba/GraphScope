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
"""
    Unit tests for average degree connectivity.
"""
from itertools import permutations

import pytest

from graphscope import nx


@pytest.mark.usefixtures("graphscope_session")
class TestNeighborConnectivity:
    def test_degree_p4(self):
        G = nx.path_graph(4)
        answer = {1: 2.0, 2: 1.5}
        nd = nx.builtin.average_degree_connectivity(G)
        assert nd == answer

        D = G.to_directed()
        answer = {2: 2.0, 4: 1.5}
        nd = nx.builtin.average_degree_connectivity(D)
        assert nd == answer

        answer = {1: 2.0, 2: 1.5}
        D = G.to_directed()
        nd = nx.builtin.average_degree_connectivity(D, source="in", target="in")
        assert nd == answer

    def test_degree_p4_weighted(self):
        G = nx.Graph()
        G.add_nodes_from([0, 1, 2, 3])
        G.add_edges_from([(0, 1), (2, 3)], weight=1.0)
        G.add_edge(1, 2, weight=4.0)
        # G = nx.path_graph(4)
        # G[1][2]["weight"] = 4
        answer = {1: 2.0, 2: 1.8}
        nd = nx.builtin.average_degree_connectivity(G, weight="weight")
        assert nd == answer
        answer = {1: 2.0, 2: 1.5}
        nd = nx.builtin.average_degree_connectivity(G)
        assert nd == answer

        D = G.to_directed()
        answer = {2: 2.0, 4: 1.8}
        nd = nx.builtin.average_degree_connectivity(D, weight="weight")
        assert nd == answer

        answer = {1: 2.0, 2: 1.8}
        D = G.to_directed()
        nd = nx.builtin.average_degree_connectivity(
            D, weight="weight", source="in", target="in"
        )
        assert nd == answer

        D = G.to_directed()
        nd = nx.builtin.average_degree_connectivity(
            D, source="in", target="out", weight="weight"
        )
        assert nd == answer

    def test_weight_keyword(self):
        G = nx.Graph()
        G.add_nodes_from([0, 1, 2, 3])
        G.add_edges_from([(0, 1), (2, 3)], other=1.0)
        G.add_edge(1, 2, other=4.0)
        # G = nx.path_graph(4)
        # G[1][2]["other"] = 4
        answer = {1: 2.0, 2: 1.8}
        nd = nx.builtin.average_degree_connectivity(G, weight="other")
        assert nd == answer
        answer = {1: 2.0, 2: 1.5}
        nd = nx.builtin.average_degree_connectivity(G, weight=None)
        assert nd == answer

        D = G.to_directed()
        answer = {2: 2.0, 4: 1.8}
        nd = nx.builtin.average_degree_connectivity(D, weight="other")
        assert nd == answer

        answer = {1: 2.0, 2: 1.8}
        D = G.to_directed()
        nd = nx.builtin.average_degree_connectivity(
            D, weight="other", source="in", target="in"
        )
        assert nd == answer

        D = G.to_directed()
        nd = nx.builtin.average_degree_connectivity(
            D, weight="other", source="in", target="in"
        )
        assert nd == answer

    def test_degree_barrat(self):
        # G = nx.star_graph(5)
        G = nx.Graph()
        G.add_edges_from(
            [
                (1, 0),
                (2, 0),
                (3, 0),
                (4, 0),
                (5, 0),
                (0, 1),
                (0, 2),
                (0, 3),
                (0, 4),
                (0, 5),
            ],
            weight=1,
        )
        G.add_edges_from([(5, 6), (5, 7), (5, 8), (5, 9)], weight=1)
        G.add_edge(0, 5, weight=5)
        # G[0][5]["weight"] = 5
        nd = nx.builtin.average_degree_connectivity(G)[5]
        assert nd == 1.8
        nd = nx.builtin.average_degree_connectivity(G, weight="weight")[5]
        assert nd == pytest.approx(3.222222, abs=1e-5)
        # nd = nx.k_nearest_neighbors(G, weight="weight")[5]
        # assert nd == pytest.approx(3.222222, abs=1e-5)

    def test_zero_deg(self):
        G = nx.DiGraph()
        G.add_edge(1, 2)
        G.add_edge(1, 3)
        G.add_edge(1, 4)
        c = nx.builtin.average_degree_connectivity(G)
        assert c == {1: 0, 3: 1}
        c = nx.builtin.average_degree_connectivity(G, source="in", target="in")
        assert c == {0: 0, 1: 0}
        c = nx.builtin.average_degree_connectivity(G, source="in", target="out")
        assert c == {0: 0, 1: 3}
        c = nx.builtin.average_degree_connectivity(G, source="in", target="in+out")
        assert c == {0: 0, 1: 3}
        c = nx.builtin.average_degree_connectivity(G, source="out", target="out")
        assert c == {0: 0, 3: 0}
        c = nx.builtin.average_degree_connectivity(G, source="out", target="in")
        assert c == {0: 0, 3: 1}
        c = nx.builtin.average_degree_connectivity(G, source="out", target="in+out")
        assert c == {0: 0, 3: 1}

    def test_in_out_weight(self):
        G = nx.DiGraph()
        G.add_edge(1, 2, weight=1)
        G.add_edge(1, 3, weight=1)
        G.add_edge(3, 1, weight=1)
        for s, t in permutations(["in", "out", "in+out"], 2):
            c = nx.builtin.average_degree_connectivity(G, source=s, target=t)
            cw = nx.builtin.average_degree_connectivity(
                G, source=s, target=t, weight="weight"
            )
            assert c == cw

    def test_invalid_source(self):
        with pytest.raises(nx.NetworkXError):
            G = nx.DiGraph()
            nx.average_degree_connectivity(G, source="bogus")

    def test_invalid_target(self):
        with pytest.raises(nx.NetworkXError):
            G = nx.DiGraph()
            nx.average_degree_connectivity(G, target="bogus")

    def test_single_node(self):
        # Is this really the intended behavior for providing a
        # single node as the argument `nodes`? Shouldn't the function
        # just return the connectivity value itself?
        G = nx.trivial_graph()
        conn = nx.builtin.average_degree_connectivity(G)
        assert conn == {0: 0}
