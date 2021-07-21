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
"""
    Unit tests for degree assortativity coefficient.
"""
import pytest

from graphscope import nx
from graphscope.nx.tests.utils import almost_equal


@pytest.mark.usefixtures("graphscope_session")
class TestDegreeAssortativity:
    def setup_method(self):
        self.P4 = nx.path_graph(4)
        self.D = nx.DiGraph()
        self.D.add_edges_from([(0, 2), (0, 3), (1, 3), (2, 3)])

    def teardown_method(self):
        del self.P4
        del self.D

    def test_degree_assortativity_undirected1(self):
        r = nx.builtin.degree_assortativity_coefficient(self.P4)
        assert almost_equal(r, -1.0 / 2, places=4)

    def test_degree_assortativity_undirected2(self):
        r = nx.builtin.degree_assortativity_coefficient(self.P4, x="in", y="in")
        assert almost_equal(r, -1.0 / 2, places=4)

    def test_degree_assortativity_undirected3(self):
        r = nx.builtin.degree_assortativity_coefficient(self.P4, x="in", y="out")
        assert almost_equal(r, -1.0 / 2, places=4)

    def test_degree_assortativity_undirected4(self):
        r = nx.builtin.degree_assortativity_coefficient(self.P4, x="out", y="out")
        assert almost_equal(r, -1.0 / 2, places=4)

    def test_degree_assortativity_directed1(self):
        r = nx.builtin.degree_assortativity_coefficient(self.D, directed=True)
        assert almost_equal(r, -0.57735, places=4)

    def test_degree_assortativity_directed2(self):
        r = nx.builtin.degree_assortativity_coefficient(
            self.D, x="in", y="in", directed=True
        )
        assert almost_equal(r, 0.33333, places=4)

    def test_degree_assortativity_directed3(self):
        r = nx.builtin.degree_assortativity_coefficient(
            self.D, x="in", y="out", directed=True
        )
        assert almost_equal(r, -0.33333, places=4)

    def test_degree_assortativity_directed4(self):
        r = nx.builtin.degree_assortativity_coefficient(
            self.D, x="out", y="out", directed=True
        )
        assert almost_equal(r, 0.57735, places=4)
