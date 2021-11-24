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

import networkx.algorithms.centrality.tests.test_katz_centrality
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_katz_centrality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.centrality.tests.test_katz_centrality import TestKatzCentrality
from networkx.algorithms.centrality.tests.test_katz_centrality import \
    TestKatzCentralityDirectedNumpy
from networkx.algorithms.centrality.tests.test_katz_centrality import \
    TestKatzCentralityNumpy
from networkx.algorithms.centrality.tests.test_katz_centrality import \
    TestKatzEigenvectorVKatz


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestKatzCentrality)
class TestKatzCentrality():
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestKatzCentralityNumpy)
class TestKatzCentralityNumpy():
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
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
