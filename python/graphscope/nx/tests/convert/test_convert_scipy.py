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

import pytest
from networkx.tests.test_convert_scipy import TestConvertNumpy

import graphscope.nx as nx
from graphscope.nx.generators.classic import barbell_graph
from graphscope.nx.generators.classic import cycle_graph
from graphscope.nx.generators.classic import path_graph
from graphscope.nx.tests.utils import assert_graphs_equal
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestConvertNumpy)
class TestConvertNumpy:
    @pytest.mark.skip(reason="graphscope.nx not support numpy dtype yet")
    def test_identity_graph_matrix(self):
        "Conversion from graph to sparse matrix to graph."
        A = nx.to_scipy_sparse_matrix(self.G1)
        self.identity_conversion(self.G1, A, nx.Graph())

    @pytest.mark.skip(reason="graphscope.nx not support numpy dtype yet")
    def test_identity_digraph_matrix(self):
        "Conversion from digraph to sparse matrix to digraph."
        A = nx.to_scipy_sparse_matrix(self.G2)
        self.identity_conversion(self.G2, A, nx.DiGraph())

    @pytest.mark.skip(reason="graphscope.nx not support numpy dtype yet")
    def test_identity_weighted_graph_matrix(self):
        """Conversion from weighted graph to sparse matrix to weighted graph."""
        A = nx.to_scipy_sparse_matrix(self.G3)
        self.identity_conversion(self.G3, A, nx.Graph())

    @pytest.mark.skip(reason="graphscope.nx not support numpy dtype yet")
    def test_identity_weighted_digraph_matrix(self):
        """Conversion from weighted digraph to sparse matrix to weighted digraph."""
        A = nx.to_scipy_sparse_matrix(self.G4)
        self.identity_conversion(self.G4, A, nx.DiGraph())

    @pytest.mark.skip(reason="graphscope.nx not support numpy dtype yet")
    def test_nodelist(self):
        """Conversion from graph to sparse matrix to graph with nodelist."""
        P4 = path_graph(4)
        P3 = path_graph(3)
        nodelist = list(P3.nodes())
        A = nx.to_scipy_sparse_matrix(P4, nodelist=nodelist)
        GA = nx.Graph(A)
        self.assert_isomorphic(GA, P3)

        # Make nodelist ambiguous by containing duplicates.
        nodelist += [nodelist[0]]
        pytest.raises(nx.NetworkXError, nx.to_numpy_matrix, P3, nodelist=nodelist)

    @pytest.mark.skip(reason="graphscope.nx not support numpy dtype yet")
    def test_from_scipy_sparse_matrix_parallel_edges(self):
        """Tests that the :func:`networkx.from_scipy_sparse_matrix` function
        interprets integer weights as the number of parallel edges when
        creating a multigraph.

        """
        A = sparse.csr_matrix([[1, 1], [1, 2]])
        # First, with a simple graph, each integer entry in the adjacency
        # matrix is interpreted as the weight of a single edge in the graph.
        expected = nx.DiGraph()
        edges = [(0, 0), (0, 1), (1, 0)]
        expected.add_weighted_edges_from([(u, v, 1) for (u, v) in edges])
        expected.add_edge(1, 1, weight=2)
        actual = nx.from_scipy_sparse_matrix(
            A, parallel_edges=True, create_using=nx.DiGraph
        )
        assert_graphs_equal(actual, expected)
        actual = nx.from_scipy_sparse_matrix(
            A, parallel_edges=False, create_using=nx.DiGraph
        )
        assert_graphs_equal(actual, expected)
        # Now each integer entry in the adjacency matrix is interpreted as the
        # number of parallel edges in the graph if the appropriate keyword
        # argument is specified.
        edges = [(0, 0), (0, 1), (1, 0), (1, 1), (1, 1)]
        expected = nx.MultiDiGraph()
        expected.add_weighted_edges_from([(u, v, 1) for (u, v) in edges])
        actual = nx.from_scipy_sparse_matrix(
            A, parallel_edges=True, create_using=nx.MultiDiGraph
        )
        assert_graphs_equal(actual, expected)
        expected = nx.MultiDiGraph()
        expected.add_edges_from(set(edges), weight=1)
        # The sole self-loop (edge 0) on vertex 1 should have weight 2.
        expected[1][1][0]["weight"] = 2
        actual = nx.from_scipy_sparse_matrix(
            A, parallel_edges=False, create_using=nx.MultiDiGraph
        )
        assert_graphs_equal(actual, expected)
