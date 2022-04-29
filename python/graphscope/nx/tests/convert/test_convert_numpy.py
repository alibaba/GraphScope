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

# fmt: off
from networkx.tests.test_convert_numpy import \
    TestConvertNumpyArray as _TestConvertNumpyArray
from networkx.tests.test_convert_numpy import \
    TestConvertNumpyMatrix as _TestConvertNumpyMatrix
from networkx.utils import edges_equal

import graphscope.nx as nx
from graphscope.nx.generators.classic import barbell_graph
from graphscope.nx.generators.classic import cycle_graph
from graphscope.nx.generators.classic import path_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context

# fmt: on


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(_TestConvertNumpyMatrix)
class TestConvertNumpyMatrix:
    def assert_equal(self, G1, G2):
        assert sorted(G1.nodes()) == sorted(G2.nodes())
        assert edges_equal(sorted(G1.edges()), sorted(G2.edges()))

    def test_from_numpy_matrix_type(self):
        pass

    def test_from_numpy_matrix_dtype(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(_TestConvertNumpyArray)
class TestConvertNumpyArray:
    def assert_equal(self, G1, G2):
        assert sorted(G1.nodes()) == sorted(G2.nodes())
        assert edges_equal(sorted(G1.edges()), sorted(G2.edges()))

    def test_from_numpy_array_type(self):
        pass

    def test_from_numpy_array_dtype(self):
        pass
