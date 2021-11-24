#!/usr/bin/env python
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

import random

import pytest

from graphscope import nx

numpy = pytest.importorskip("numpy")
scipy = pytest.importorskip("scipy")

from graphscope.nx.tests.utils import almost_equal

# Example from
# A. Langville and C. Meyer, "A survey of eigenvector methods of web
# information retrieval."  http://citeseer.ist.psu.edu/713792.html


@pytest.mark.usefixtures("graphscope_session")
class TestPageRank(object):
    @classmethod
    def setup_class(cls):
        G = nx.DiGraph()
        edges = [
            (1, 2),
            (1, 3),
            # 2 is a dangling node
            (3, 1),
            (3, 2),
            (3, 5),
            (4, 5),
            (4, 6),
            (5, 4),
            (5, 6),
            (6, 4),
        ]
        G.add_edges_from(edges)
        cls.G = G
        cls.G.pagerank = dict(
            zip(
                sorted(G),
                [
                    0.03721197,
                    0.05395735,
                    0.04150565,
                    0.37508082,
                    0.20599833,
                    0.28624589,
                ],
            )
        )
        cls.dangling_node_index = 1
        cls.dangling_edges = {1: 2, 2: 3, 3: 0, 4: 0, 5: 0, 6: 0}
        cls.G.dangling_pagerank = dict(
            zip(
                sorted(G),
                [0.10844518, 0.18618601, 0.0710892, 0.2683668, 0.15919783, 0.20671497],
            )
        )

    def test_pagerank(self):
        G = self.G
        p = nx.builtin.pagerank(G, alpha=0.9, tol=1.0e-08)
        for n in G:
            assert almost_equal(p[n], G.pagerank[n], places=4)

        # (TODO): open the comment if nstart implemented.
        # nstart = dict((n, random.random()) for n in G)
        # p = nx.builtin.pagerank(G, alpha=0.9, tol=1.0e-08, nstart=nstart)
        # for n in G:
        #     assert almost_equal(p[n], G.pagerank[n], places=4)

    @pytest.mark.skip(reason="Not support raise PowerIterationFailedConvergence yet.")
    def test_pagerank_max_iter(self):
        with pytest.raises(nx.PowerIterationFailedConvergence):
            nx.builtin.pagerank(self.G, max_iter=0)

    @pytest.mark.skip(reason="pagerank_numpy not implemented yet.")
    def test_numpy_pagerank(self):
        G = self.G
        p = nx.pagerank_numpy(G, alpha=0.9)
        for n in G:
            assert almost_equal(p[n], G.pagerank[n], places=4)
        personalize = dict((n, random.random()) for n in G)
        p = nx.pagerank_numpy(G, alpha=0.9, personalization=personalize)

    @pytest.mark.skip(reason="google_matrix not implemented yet.")
    def test_google_matrix(self):
        G = self.G
        M = nx.google_matrix(G, alpha=0.9, nodelist=sorted(G))
        e, ev = numpy.linalg.eig(M.T)
        p = numpy.array(ev[:, 0] / ev[:, 0].sum())[:, 0]
        for (a, b) in zip(p, self.G.pagerank.values()):
            assert almost_equal(a, b)

    @pytest.mark.skip(reason="pagerank not support personalization yet.")
    def test_personalization(self):
        G = nx.complete_graph(4)
        personalize = {0: 1, 1: 1, 2: 4, 3: 4}
        answer = {
            0: 0.23246732615667579,
            1: 0.23246732615667579,
            2: 0.267532673843324,
            3: 0.2675326738433241,
        }
        p = nx.builtin.pagerank(G, alpha=0.85, personalization=personalize)
        for n in G:
            assert almost_equal(p[n], answer[n], places=4)

    @pytest.mark.skip(reason="pagerank not support personalization yet.")
    def test_zero_personalization_vector(self):
        G = nx.complete_graph(4)
        personalize = {0: 0, 1: 0, 2: 0, 3: 0}
        pytest.raises(
            ZeroDivisionError, nx.builtin.pagerank, G, personalization=personalize
        )

    @pytest.mark.skip(reason="pagerank not support personalization yet.")
    def test_one_nonzero_personalization_value(self):
        G = nx.complete_graph(4)
        personalize = {0: 0, 1: 0, 2: 0, 3: 1}
        answer = {
            0: 0.22077931820379187,
            1: 0.22077931820379187,
            2: 0.22077931820379187,
            3: 0.3376620453886241,
        }
        p = nx.builtin.pagerank(G, alpha=0.85, personalization=personalize)
        for n in G:
            assert almost_equal(p[n], answer[n], places=4)

    @pytest.mark.skip(reason="pagerank not support personalization yet.")
    def test_incomplete_personalization(self):
        G = nx.complete_graph(4)
        personalize = {3: 1}
        answer = {
            0: 0.22077931820379187,
            1: 0.22077931820379187,
            2: 0.22077931820379187,
            3: 0.3376620453886241,
        }
        p = nx.builtin.pagerank(G, alpha=0.85, personalization=personalize)
        for n in G:
            assert almost_equal(p[n], answer[n], places=4)

    @pytest.mark.skip(reason="google_matrix not implemented yet.")
    def test_dangling_matrix(self):
        """
        Tests that the google_matrix doesn't change except for the dangling
        nodes.
        """
        G = self.G
        dangling = self.dangling_edges
        dangling_sum = float(sum(dangling.values()))
        M1 = nx.google_matrix(G, personalization=dangling)
        M2 = nx.google_matrix(G, personalization=dangling, dangling=dangling)
        for i in range(len(G)):
            for j in range(len(G)):
                if i == self.dangling_node_index and (j + 1) in dangling:
                    assert almost_equal(
                        M2[i, j], dangling[j + 1] / dangling_sum, places=4
                    )
                else:
                    assert almost_equal(M2[i, j], M1[i, j], places=4)

    @pytest.mark.skip(reason="pagerank not support dangling yet.")
    def test_dangling_pagerank(self):
        pr = nx.builtin.pagerank(self.G, dangling=self.dangling_edges)
        for n in self.G:
            assert almost_equal(pr[n], self.G.dangling_pagerank[n], places=4)

    @pytest.mark.skip(reason="pagerank_numpy not implemented yet.")
    def test_dangling_numpy_pagerank(self):
        pr = nx.pagerank_numpy(self.G, dangling=self.dangling_edges)
        for n in self.G:
            assert almost_equal(pr[n], self.G.dangling_pagerank[n], places=4)

    @pytest.mark.skip(reason="pagerank_numpy not implemented yet.")
    def test_empty(self):
        G = nx.Graph()
        assert nx.pagerank(G) == {}
        assert nx.pagerank_numpy(G) == {}
        assert nx.google_matrix(G).shape == (0, 0)


@pytest.mark.usefixtures("graphscope_session")
@pytest.mark.skip(reason="not runnable in nx.")
class TestPageRankScipy(TestPageRank):
    def test_scipy_pagerank(self):
        G = self.G
        p = nx.pagerank_scipy(G, alpha=0.9, tol=1.0e-08)
        for n in G:
            assert almost_equal(p[n], G.pagerank[n], places=4)
        personalize = dict((n, random.random()) for n in G)
        p = nx.pagerank_scipy(G, alpha=0.9, tol=1.0e-08, personalization=personalize)

        nstart = dict((n, random.random()) for n in G)
        p = nx.pagerank_scipy(G, alpha=0.9, tol=1.0e-08, nstart=nstart)
        for n in G:
            assert almost_equal(p[n], G.pagerank[n], places=4)

    def test_scipy_pagerank_max_iter(self):
        with pytest.raises(nx.PowerIterationFailedConvergence):
            nx.pagerank_scipy(self.G, max_iter=0)

    def test_dangling_scipy_pagerank(self):
        pr = nx.pagerank_scipy(self.G, dangling=self.dangling_edges)
        for n in self.G:
            assert almost_equal(pr[n], self.G.dangling_pagerank[n], places=4)

    def test_empty_scipy(self):
        G = nx.Graph()
        assert nx.pagerank_scipy(G) == {}
