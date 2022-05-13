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
import math

import pytest

np = pytest.importorskip("numpy")
scipy = pytest.importorskip("scipy")

from graphscope import nx
from graphscope.nx.tests.utils import almost_equal


@pytest.mark.usefixtures("graphscope_session")
class TestEigenvectorCentrality(object):
    def test_K5(self):
        """Eigenvector centrality: K5"""
        G = nx.complete_graph(5)
        b = nx.builtin.eigenvector_centrality(G)
        v = math.sqrt(1 / 5.0)
        b_answer = dict.fromkeys(G, v)
        for n in sorted(G):
            assert almost_equal(b[n], b_answer[n])
        nstart = dict([(n, 1) for n in G])

        b = nx.eigenvector_centrality_numpy(G)
        for n in sorted(G):
            assert almost_equal(b[n], b_answer[n], places=3)

    def test_P3(self):
        """Eigenvector centrality: P3"""
        G = nx.path_graph(3)
        b_answer = {0: 0.5, 1: 0.7071, 2: 0.5}
        b = nx.eigenvector_centrality_numpy(G)
        for n in sorted(G):
            assert almost_equal(b[n], b_answer[n], places=4)
        b = nx.builtin.eigenvector_centrality(G)
        for n in sorted(G):
            assert almost_equal(b[n], b_answer[n], places=4)

    def test_P3_unweighted(self):
        """Eigenvector centrality: P3"""
        G = nx.path_graph(3)
        b_answer = {0: 0.5, 1: 0.7071, 2: 0.5}
        b = nx.eigenvector_centrality_numpy(G, weight=None)
        for n in sorted(G):
            assert almost_equal(b[n], b_answer[n], places=4)

    def test_maxiter(self):
        with pytest.raises(nx.PowerIterationFailedConvergence):
            G = nx.path_graph(3)
            b = nx.builtin.eigenvector_centrality(G, max_iter=0)


@pytest.mark.usefixtures("graphscope_session")
class TestEigenvectorCentralityDirected(object):
    @classmethod
    def setup_class(cls):
        G = nx.DiGraph()

        edges = [
            (1, 2),
            (1, 3),
            (2, 4),
            (3, 2),
            (3, 5),
            (4, 2),
            (4, 5),
            (4, 6),
            (5, 6),
            (5, 7),
            (5, 8),
            (6, 8),
            (7, 1),
            (7, 5),
            (7, 8),
            (8, 6),
            (8, 7),
        ]

        G.add_edges_from(edges, weight=2.0)
        cls.G = G.reverse()
        cls.G.evc = [
            0.25368793,
            0.19576478,
            0.32817092,
            0.40430835,
            0.48199885,
            0.15724483,
            0.51346196,
            0.32475403,
        ]

        H = nx.DiGraph()

        edges = [
            (1, 2),
            (1, 3),
            (2, 4),
            (3, 2),
            (3, 5),
            (4, 2),
            (4, 5),
            (4, 6),
            (5, 6),
            (5, 7),
            (5, 8),
            (6, 8),
            (7, 1),
            (7, 5),
            (7, 8),
            (8, 6),
            (8, 7),
        ]

        G.add_edges_from(edges)
        cls.H = G.reverse()
        cls.H.evc = [
            0.25368793,
            0.19576478,
            0.32817092,
            0.40430835,
            0.48199885,
            0.15724483,
            0.51346196,
            0.32475403,
        ]

    def test_eigenvector_centrality_weighted(self):
        G = self.G
        p = nx.builtin.eigenvector_centrality(G)
        for (a, b) in zip(list(dict(sorted(p.items())).values()), self.G.evc):
            assert almost_equal(a, b, places=4)

    def test_eigenvector_centrality_weighted_numpy(self):
        G = self.G
        p = nx.eigenvector_centrality_numpy(G, weight="weight")
        for (a, b) in zip(list(dict(sorted(p.items())).values()), self.G.evc):
            assert almost_equal(a, b, places=4)

    def test_eigenvector_centrality_unweighted(self):
        G = self.H
        p = nx.builtin.eigenvector_centrality(G)
        for (a, b) in zip(list(dict(sorted(p.items())).values()), self.G.evc):
            assert almost_equal(a, b, places=4)

    def test_eigenvector_centrality_unweighted_numpy(self):
        G = self.H
        p = nx.eigenvector_centrality_numpy(G)
        for (a, b) in zip(list(dict(sorted(p.items())).values()), self.H.evc):
            assert almost_equal(a, b, places=4)


class TestEigenvectorCentralityExceptions(object):
    def test_multigraph(self):
        with pytest.raises(nx.NetworkXException):
            e = nx.builtin.eigenvector_centrality(nx.MultiGraph())

    def test_multigraph_numpy(self):
        with pytest.raises(nx.NetworkXException):
            e = nx.eigenvector_centrality_numpy(nx.MultiGraph())

    def test_empty(self):
        with pytest.raises(nx.NetworkXException):
            e = nx.eigenvector_centrality(nx.Graph())

    def test_empty_numpy(self):
        with pytest.raises(nx.NetworkXException):
            e = nx.eigenvector_centrality_numpy(nx.Graph())
