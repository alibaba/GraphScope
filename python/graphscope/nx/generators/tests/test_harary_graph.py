#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

import os

import pytest
from networkx.algorithms.isomorphism.isomorph import is_isomorphic
from networkx.generators.tests.test_harary_graph import TestHararyGraph

import graphscope.nx as nx
from graphscope.nx.generators.harary_graph import hkn_harary_graph
from graphscope.nx.generators.harary_graph import hnm_harary_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestHararyGraph)
class TestHararyGraph:
    def test_hkn_harary_graph(self):
        # When k == 1, the hkn_harary_graph(k,n) is
        # the path_graph(n)
        for (k, n) in [(1, 6), (1, 7)]:
            G1 = hkn_harary_graph(k, n)
            G2 = nx.path_graph(n)
            assert is_isomorphic(G1, G2)

        # When k is even, the hkn_harary_graph(k,n) is
        # the circulant_graph(n, list(range(1,k/2+1)))
        for (k, n) in [(2, 6), (2, 7), (4, 6), (4, 7)]:
            G1 = hkn_harary_graph(k, n)
            G2 = nx.circulant_graph(n, list(range(1, k // 2 + 1)))
            assert is_isomorphic(G1, G2)

        # When k is odd and n is even, the hkn_harary_graph(k,n) is
        # the circulant_graph(n, list(range(1,(k+1)/2)) plus [n/2])
        for (k, n) in [(3, 6), (5, 8), (7, 10)]:
            G1 = hkn_harary_graph(k, n)
            L = list(range(1, (k + 1) // 2))
            L.append(n // 2)
            G2 = nx.circulant_graph(n, L)
            assert is_isomorphic(G1, G2)

        # When k is odd and n is odd, the hkn_harary_graph(k,n) is
        # the circulant_graph(n, list(range(1,(k+1)/2))) with
        # n//2+1 edges added between node i and node i+n//2+1
        for (k, n) in [(3, 5), (5, 9), (7, 11)]:
            G1 = hkn_harary_graph(k, n)
            G2 = nx.circulant_graph(n, list(range(1, (k + 1) // 2)))
            eSet1 = set(G1.edges)
            eSet2 = set(G2.edges)
            eSet3 = set()
            half = n // 2
            for i in range(0, half + 1):
                # add half+1 edges between i and i+half
                eSet3.add((i, (i + half) % n))
            if os.environ.get("DEPLOYMENT", None) != "standalone" and k == 7:
                eSet1.remove((8, 3))
                eSet1.remove((6, 1))
                eSet1.remove((10, 5))
                eSet1.add((3, 8))
                eSet1.add((1, 6))
                eSet1.add((5, 10))
            assert eSet1 == eSet2 | eSet3

        # Raise NetworkXError if k<1
        k = 0
        n = 0
        pytest.raises(nx.NetworkXError, hkn_harary_graph, k, n)

        # Raise NetworkXError if n<k+1
        k = 6
        n = 6
        pytest.raises(nx.NetworkXError, hkn_harary_graph, k, n)
