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
import pytest

from graphscope import nx
from graphscope.nx.tests.utils import almost_equal

# Example from
# A. Langville and C. Meyer, "A survey of eigenvector methods of web
# information retrieval."  http://citeseer.ist.psu.edu/713792.html


@pytest.mark.usefixtures("graphscope_session")
class TestHITS:
    def setup_class(cls):
        G = nx.DiGraph()

        edges = [(1, 3), (1, 5), (2, 1), (3, 5), (5, 4), (5, 3), (6, 5)]

        G.add_edges_from(edges, weight=1)
        cls.G = G
        cls.G.a = dict(
            zip(sorted(G), [0.000000, 0.000000, 0.366025, 0.133975, 0.500000, 0.000000])
        )
        cls.G.h = dict(
            zip(sorted(G), [0.366025, 0.000000, 0.211325, 0.000000, 0.211325, 0.211325])
        )

    def test_hits(self):
        G = self.G
        h, a = nx.hits(G, tol=1.0e-08)
        for n in G:
            assert almost_equal(h[n], G.h[n], places=4)
        for n in G:
            assert almost_equal(a[n], G.a[n], places=4)

    @pytest.mark.skip(reason="nstart not support.")
    def test_hits_nstart(self):
        G = self.G
        nstart = dict([(i, 1.0 / 2) for i in G])
        h, a = nx.hits(G, nstart=nstart)

    @pytest.mark.skip(reason="hits_numpy not implemented.")
    def test_hits_numpy(self):
        numpy = pytest.importorskip("numpy")
        G = self.G
        h, a = nx.hits_numpy(G)
        for n in G:
            assert almost_equal(h[n], G.h[n], places=4)
        for n in G:
            assert almost_equal(a[n], G.a[n], places=4)

    def test_hits_scipy(self):
        sp = pytest.importorskip("scipy")
        G = self.G
        h, a = nx.hits_scipy(G, tol=1.0e-08)
        for n in G:
            assert almost_equal(h[n], G.h[n], places=4)
        for n in G:
            assert almost_equal(a[n], G.a[n], places=4)

    def test_empty(self):
        G = nx.Graph()
        assert nx.builtin.hits(G) == ({}, {})

    @pytest.mark.skip(reason="hits_scipy not implemented.")
    def test_empty_scipy(self):
        scipy = pytest.importorskip("scipy")
        G = nx.Graph()
        assert nx.hits_scipy(G) == ({}, {})

    def test_hits_not_convergent(self):
        with pytest.raises(nx.PowerIterationFailedConvergence):
            G = self.G
            nx.hits(G, max_iter=0)
