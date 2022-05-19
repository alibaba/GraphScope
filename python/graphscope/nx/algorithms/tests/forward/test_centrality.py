import pytest
import numpy as np
import networkx.algorithms.centrality.tests.test_betweenness_centrality
import networkx.algorithms.centrality.tests.test_betweenness_centrality_subset
import networkx.algorithms.centrality.tests.test_closeness_centrality
import networkx.algorithms.centrality.tests.test_current_flow_betweenness_centrality
import networkx.algorithms.centrality.tests.test_current_flow_betweenness_centrality_subset
import networkx.algorithms.centrality.tests.test_current_flow_closeness
import networkx.algorithms.centrality.tests.test_dispersion
import networkx.algorithms.centrality.tests.test_eigenvector_centrality
import networkx.algorithms.centrality.tests.test_group
import networkx.algorithms.centrality.tests.test_harmonic_centrality
import networkx.algorithms.centrality.tests.test_katz_centrality
import networkx.algorithms.centrality.tests.test_load_centrality
import networkx.algorithms.centrality.tests.test_percolation_centrality
import networkx.algorithms.centrality.tests.test_reaching
import networkx.algorithms.centrality.tests.test_second_order_centrality
import networkx.algorithms.centrality.tests.test_subgraph

import graphscope.nx as nx
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_betweenness_centrality,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_betweenness_centrality_subset,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_closeness_centrality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_current_flow_betweenness_centrality,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.
                        test_current_flow_betweenness_centrality_subset,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_current_flow_closeness,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_dispersion,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_eigenvector_centrality,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_group,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_harmonic_centrality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_katz_centrality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_load_centrality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_percolation_centrality,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_reaching,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_second_order_centrality,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_subgraph,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestBetweennessCentrality)
class TestBetweennessCentrality:
    @pytest.mark.skip(reason="not support sampling")
    def test_sample_from_P3(self):
        G = nx.path_graph(3)
        b_answer = {0: 0.0, 1: 1.0, 2: 0.0}
        b = nx.betweenness_centrality(G, k=3, weight=None, normalized=False, seed=1)
        for n in sorted(G):
            assert b[n] == pytest.approx(b_answer[n], abs=1e-7)
        b = nx.betweenness_centrality(G, k=2, weight=None, normalized=False, seed=1)
        # python versions give different results with same seed
        b_approx1 = {0: 0.0, 1: 1.5, 2: 0.0}
        b_approx2 = {0: 0.0, 1: 0.75, 2: 0.0}
        for n in sorted(G):
            assert b[n] in (b_approx1[n], b_approx2[n])


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestApproximateFlowBetweennessCentrality)
class TestApproximateFlowBetweennessCentrality:
    # NB: graphscope.nx does not support grid_graph, pass the test
    def test_grid(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestKatzCentralityDirectedNumpy)
class TestKatzCentralityDirectedNumpy():
    @classmethod
    def setup_class(cls):
        global np
        np = pytest.importorskip('numpy')
        scipy = pytest.importorskip('scipy')
        G = nx.DiGraph()
        edges = [(1, 2), (1, 3), (2, 4), (3, 2), (3, 5), (4, 2), (4, 5), (4, 6), (5, 6),
                 (5, 7), (5, 8), (6, 8), (7, 1), (7, 5), (7, 8), (8, 6), (8, 7)]
        G.add_edges_from(edges, weight=2.0)
        cls.G = G.reverse()
        cls.G.alpha = 0.1
        cls.G.evc = [
            0.3289589783189635,
            0.2832077296243516,
            0.3425906003685471,
            0.3970420865198392,
            0.41074871061646284,
            0.272257430756461,
            0.4201989685435462,
            0.34229059218038554,
        ]

        H = nx.DiGraph(edges)
        cls.H = G.reverse()
        cls.H.alpha = 0.1
        cls.H.evc = [
            0.3289589783189635,
            0.2832077296243516,
            0.3425906003685471,
            0.3970420865198392,
            0.41074871061646284,
            0.272257430756461,
            0.4201989685435462,
            0.34229059218038554,
        ]


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestKatzEigenvectorVKatz)
class TestKatzEigenvectorVKatz():
    @pytest.mark.skip(reason="not support adjacency_matrix now")
    def test_eigenvector_v_katz_random(self):
        G = nx.gnp_random_graph(10, 0.5, seed=1234)
        l = float(max(eigvals(nx.adjacency_matrix(G).todense())))
        e = nx.eigenvector_centrality_numpy(G)
        k = nx.katz_centrality_numpy(G, 1.0 / l)
        for n in G:
            assert almost_equal(e[n], k[n])


@pytest.mark.usefixtures("graphscope_session")
class TestVoteRankCentrality:
    @pytest.mark.skip(reason="not support list as attribute")
    def test_voterank_centrality_1(self):
        G = nx.Graph()
        G.add_edges_from([(7, 8), (7, 5), (7, 9), (5, 0), (0, 1), (0, 2), (0, 3),
                          (0, 4), (1, 6), (2, 6), (3, 6), (4, 6)])
        assert [0, 7, 6] == nx.voterank(G)

    @pytest.mark.skip(reason="not support list as attribute")
    def test_voterank_centrality_2(self):
        G = nx.florentine_families_graph()
        d = nx.voterank(G, 4)
        exact = ['Medici', 'Strozzi', 'Guadagni', 'Castellani']
        assert exact == d
