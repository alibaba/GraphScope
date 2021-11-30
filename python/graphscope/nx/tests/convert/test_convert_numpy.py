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

np = pytest.importorskip("numpy")
np_assert_equal = np.testing.assert_equal

from networkx.tests.test_convert_numpy import TestConvertNumpy

import graphscope.nx as nx
from graphscope.nx.generators.classic import barbell_graph
from graphscope.nx.generators.classic import cycle_graph
from graphscope.nx.generators.classic import path_graph
from graphscope.nx.tests.utils import assert_graphs_equal
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestConvertNumpy)
class TestConvertNumpy:
    def test_from_numpy_matrix_type(self):
        pass

    def test_from_numpy_matrix_dtype(self):
        pass

    def test_from_numpy_array_type(self):
        pass

    def test_from_numpy_array_dtype(self):
        pass

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_identity_graph_matrix(self):
        """Conversion from digraph to matrix to digraph."""
        A = nx.to_numpy_matrix(self.G2)
        self.identity_conversion(self.G2, A, nx.DiGraph())

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_identity_graph_array(self):
        "Conversion from graph to array to graph."
        A = nx.to_numpy_matrix(self.G1)
        A = np.asarray(A)
        self.identity_conversion(self.G1, A, nx.Graph())

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_identity_digraph_matrix(self):
        """Conversion from digraph to matrix to digraph."""
        A = nx.to_numpy_matrix(self.G2)
        self.identity_conversion(self.G2, A, nx.DiGraph())

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_identity_digraph_array(self):
        """Conversion from digraph to array to digraph."""
        A = nx.to_numpy_matrix(self.G2)
        A = np.asarray(A)
        self.identity_conversion(self.G2, A, nx.DiGraph())

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_identity_weighted_graph_matrix(self):
        """Conversion from weighted graph to matrix to weighted graph."""
        A = nx.to_numpy_matrix(self.G3)
        self.identity_conversion(self.G3, A, nx.Graph())

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_identity_weighted_graph_array(self):
        """Conversion from weighted graph to array to weighted graph."""
        A = nx.to_numpy_matrix(self.G3)
        A = np.asarray(A)
        self.identity_conversion(self.G3, A, nx.Graph())

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_identity_weighted_digraph_matrix(self):
        """Conversion from weighted digraph to matrix to weighted digraph."""
        A = nx.to_numpy_matrix(self.G4)
        self.identity_conversion(self.G4, A, nx.DiGraph())

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_identity_weighted_digraph_array(self):
        """Conversion from weighted digraph to array to weighted digraph."""
        A = nx.to_numpy_matrix(self.G4)
        A = np.asarray(A)
        self.identity_conversion(self.G4, A, nx.DiGraph())

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone", reason="edge order."
    )
    def test_nodelist(self):
        """Conversion from graph to matrix to graph with nodelist."""
        P4 = path_graph(4)
        P3 = path_graph(3)
        nodelist = list(P3)
        A = nx.to_numpy_matrix(P4, nodelist=nodelist)
        GA = nx.Graph(A)
        self.assert_equal(GA, P3)

        # Make nodelist ambiguous by containing duplicates.
        nodelist += [nodelist[0]]
        pytest.raises(nx.NetworkXError, nx.to_numpy_matrix, P3, nodelist=nodelist)
