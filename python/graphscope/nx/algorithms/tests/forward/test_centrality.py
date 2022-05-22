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
import numpy as np
import pytest

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
@with_graphscope_nx_context(TestEigenvectorCentralityDirected)
class TestEigenvectorCentralityDirected:
    def test_eigenvector_centrality_weighted_numpy(self):
        G = self.G
        p = nx.eigenvector_centrality_numpy(G)
        for (a, b) in zip(list(p.values()), self.G.evc):
            assert a == pytest.approx(b, abs=1e-4)

    def test_eigenvector_centrality_unweighted_numpy(self):
        G = self.H
        p = nx.eigenvector_centrality_numpy(G)
        for (a, b) in zip(list(p.values()), self.G.evc):
            assert a == pytest.approx(b, abs=1e-4)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestKatzCentralityDirectedNumpy)
class TestKatzCentralityDirectedNumpy():
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
    def test_eigenvector_v_katz_random(self):
        G = nx.gnp_random_graph(10, 0.5, seed=1234)
        l = float(max(np.linalg.eigvals(nx.to_scipy_sparse_array(G).todense())))
        e = nx.eigenvector_centrality_numpy(G)
        k = nx.katz_centrality_numpy(G, 1.0 / l)
        for n in G:
            assert e[n] == pytest.approx(k[n], abs=1e-5)
