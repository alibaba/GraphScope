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

"""Unit tests for the :mod:`networkx.algorithms.boundary` module."""

import os

import pytest

from graphscope import nx
from graphscope.nx.utils.misc import edges_equal


@pytest.mark.usefixtures("graphscope_session")
class TestNodeBoundary:
    """Unit tests for the :func:`~networkx.builtin.node_boundary` function."""

    def test_path_graph(self):
        P10 = nx.path_graph(10)
        assert nx.builtin.node_boundary(P10, [0, 1, 2]) == {3}
        assert nx.builtin.node_boundary(P10, [3, 4, 5]) == {2, 6}
        assert nx.builtin.node_boundary(P10, [2, 3, 4, 5, 6]) == {1, 7}
        assert nx.builtin.node_boundary(P10, [7, 8, 9]) == {6}

    def test_complete_graph(self):
        K10 = nx.complete_graph(10)
        assert sorted(nx.builtin.node_boundary(K10, [0, 1, 2])) == [3, 4, 5, 6, 7, 8, 9]
        assert sorted(nx.builtin.node_boundary(K10, [3, 4, 5])) == [0, 1, 2, 6, 7, 8, 9]
        assert sorted(nx.builtin.node_boundary(K10, [2, 3, 4, 5, 6])) == [0, 1, 7, 8, 9]
        if os.environ.get("DEPLOYMENT", None) == "standalone":
            assert nx.builtin.node_boundary(K10, [0, 1, 2], [2, 3, 4]) == {3, 4}
        else:  # num_workers=2
            assert nx.builtin.node_boundary(K10, [0, 1, 2], [2, 3, 4]) == {4, 3}

    def test_directed(self):
        """Tests the node boundary of a directed graph."""
        G = nx.DiGraph([(0, 1), (1, 2), (2, 3), (3, 4), (4, 0)])
        S = [0, 1]
        boundary = nx.builtin.node_boundary(G, S)
        expected = {2}
        assert boundary == expected


@pytest.mark.usefixtures("graphscope_session")
class TestEdgeBoundary:
    """Unit tests for the :func:`~networkx.builtin.edge_boundary` function."""

    def test_path_graph(self):
        P10 = nx.path_graph(10)
        assert list(nx.builtin.edge_boundary(P10, [0, 1, 2])) == [(2, 3)]
        assert sorted(nx.builtin.edge_boundary(P10, [3, 4, 5])) == [(3, 2), (5, 6)]
        assert sorted(nx.builtin.edge_boundary(P10, [2, 3, 4, 5, 6])) == [
            (2, 1),
            (6, 7),
        ]
        assert list(nx.builtin.edge_boundary(P10, [7, 8, 9])) == [(7, 6)]
        assert sorted(nx.builtin.edge_boundary(P10, [0, 1, 2], [2, 3, 4])) == [
            (1, 2),
            (2, 3),
        ]

    def test_complete_graph(self):
        K10 = nx.complete_graph(10)

        def ilen(iterable):
            return sum(1 for i in iterable)

        assert ilen(nx.builtin.edge_boundary(K10, [0, 1, 2])) == 21
        assert ilen(nx.builtin.edge_boundary(K10, [3, 4, 5, 6])) == 24
        assert ilen(nx.builtin.edge_boundary(K10, [2, 3, 4, 5, 6])) == 25
        assert ilen(nx.builtin.edge_boundary(K10, [7, 8, 9])) == 21
        assert edges_equal(
            nx.builtin.edge_boundary(K10, [3, 4, 5], [8, 9]),
            [(3, 8), (3, 9), (4, 8), (4, 9), (5, 8), (5, 9)],
        )
        assert edges_equal(
            nx.builtin.edge_boundary(K10, [0, 1, 2], [2, 3, 4]),
            [(0, 2), (0, 3), (0, 4), (1, 2), (1, 3), (1, 4), (2, 3), (2, 4)],
        )

    def test_directed(self):
        """Tests the edge boundary of a directed graph."""
        G = nx.DiGraph([(0, 1), (1, 2), (2, 3), (3, 4), (4, 0)])
        S = [0, 1]
        boundary = list(nx.builtin.edge_boundary(G, S))
        expected = [(1, 2)]
        assert boundary == expected
