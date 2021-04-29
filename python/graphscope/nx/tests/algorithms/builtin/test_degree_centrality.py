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
    Unit tests for degree centrality.
"""
import pytest

from graphscope import nx
from graphscope.nx.tests.utils import almost_equal


@pytest.mark.usefixtures("graphscope_session")
class TestDegreeCentrality:
    def setup_method(self):
        # self.K = nx.krackhardt_kite_graph()
        self.P3 = nx.path_graph(3)
        self.K5 = nx.complete_graph(5)

        # F = nx.Graph()  # Florentine families
        # F.add_edge('Acciaiuoli', 'Medici')
        # F.add_edge('Castellani', 'Peruzzi')
        # F.add_edge('Castellani', 'Strozzi')
        # F.add_edge('Castellani', 'Barbadori')
        # F.add_edge('Medici', 'Barbadori')
        # F.add_edge('Medici', 'Ridolfi')
        # F.add_edge('Medici', 'Tornabuoni')
        # F.add_edge('Medici', 'Albizzi')
        # F.add_edge('Medici', 'Salviati')
        # F.add_edge('Salviati', 'Pazzi')
        # F.add_edge('Peruzzi', 'Strozzi')
        # F.add_edge('Peruzzi', 'Bischeri')
        # F.add_edge('Strozzi', 'Ridolfi')
        # F.add_edge('Strozzi', 'Bischeri')
        # F.add_edge('Ridolfi', 'Tornabuoni')
        # F.add_edge('Tornabuoni', 'Guadagni')
        # F.add_edge('Albizzi', 'Ginori')
        # F.add_edge('Albizzi', 'Guadagni')
        # F.add_edge('Bischeri', 'Guadagni')
        # F.add_edge('Guadagni', 'Lamberteschi')
        # self.F = F

        G = nx.DiGraph()
        G.add_edge(0, 5)
        G.add_edge(1, 5)
        G.add_edge(2, 5)
        G.add_edge(3, 5)
        G.add_edge(4, 5)
        G.add_edge(5, 6)
        G.add_edge(5, 7)
        G.add_edge(5, 8)
        self.G = G

    def teardown_method(self):
        del self.G
        del self.K5
        del self.P3

    def test_run_degree_centrality(self):
        nx.builtin.degree_centrality(self.G)

    def test_run_indegree_centrality(self):
        nx.builtin.in_degree_centrality(self.G)

    def test_run_outdegree_centrality(self):
        nx.builtin.out_degree_centrality(self.G)

    @pytest.mark.skip(reason="output not ready, wait to check.")
    def test_degree_centrality_1(self):
        d = nx.degree_centrality(self.K5)
        exact = dict(zip(range(5), [1] * 5))
        for n, dc in d.items():
            assert almost_equal(exact[n], dc)

    @pytest.mark.skip(reason="output not ready, wait to check.")
    def test_degree_centrality_2(self):
        d = nx.degree_centrality(self.P3)
        exact = {0: 0.5, 1: 1, 2: 0.5}
        for n, dc in d.items():
            assert almost_equal(exact[n], dc)

    @pytest.mark.skip(reason="output not ready, wait to check.")
    def test_degree_centrality_3(self):
        d = nx.degree_centrality(self.K)
        exact = {
            0: 0.444,
            1: 0.444,
            2: 0.333,
            3: 0.667,
            4: 0.333,
            5: 0.556,
            6: 0.556,
            7: 0.333,
            8: 0.222,
            9: 0.111,
        }
        for n, dc in d.items():
            assert almost_equal(exact[n], float("%5.3f" % dc))

    @pytest.mark.skip(reason="output not ready, wait to check.")
    def test_degree_centrality_4(self):
        d = nx.degree_centrality(self.F)
        names = sorted(self.F.nodes())
        dcs = [
            0.071,
            0.214,
            0.143,
            0.214,
            0.214,
            0.071,
            0.286,
            0.071,
            0.429,
            0.071,
            0.214,
            0.214,
            0.143,
            0.286,
            0.214,
        ]
        exact = dict(zip(names, dcs))
        for n, dc in d.items():
            assert almost_equal(exact[n], float("%5.3f" % dc))

    @pytest.mark.skip(reason="output not ready, wait to check.")
    def test_indegree_centrality(self):
        d = nx.in_degree_centrality(self.G)
        exact = {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.625,
            6: 0.125,
            7: 0.125,
            8: 0.125,
        }
        for n, dc in d.items():
            assert almost_equal(exact[n], dc)

    @pytest.mark.skip(reason="output not ready, wait to check.")
    def test_outdegree_centrality(self):
        d = nx.out_degree_centrality(self.G)
        exact = {
            0: 0.125,
            1: 0.125,
            2: 0.125,
            3: 0.125,
            4: 0.125,
            5: 0.375,
            6: 0.0,
            7: 0.0,
            8: 0.0,
        }
        for n, dc in d.items():
            assert almost_equal(exact[n], dc)

    @pytest.mark.skip(reason="output not ready, wait to check.")
    def test_small_graph_centrality(self):
        G = nx.empty_graph(create_using=nx.DiGraph)
        assert {} == nx.degree_centrality(G)
        assert {} == nx.out_degree_centrality(G)
        assert {} == nx.in_degree_centrality(G)

        G = nx.empty_graph(1, create_using=nx.DiGraph)
        assert {0: 1} == nx.degree_centrality(G)
        assert {0: 1} == nx.out_degree_centrality(G)
        assert {0: 1} == nx.in_degree_centrality(G)
