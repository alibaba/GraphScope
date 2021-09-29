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
    Unit tests for attribute assortativity coefficient.
"""
import pytest

from graphscope import nx
from graphscope.nx.tests.utils import almost_equal


@pytest.mark.usefixtures("graphscope_session")
class TestAttributeAssortativity:
    def setup_method(self):
        G = nx.Graph()
        G.add_nodes_from([0, 1], fish="one")
        G.add_nodes_from([2, 3], fish="two")
        G.add_nodes_from([4], fish="red")
        G.add_nodes_from([5], fish="blue")
        G.add_edges_from([(0, 1), (2, 3), (0, 4), (2, 5)])
        self.G = G

        D = nx.DiGraph()
        D.add_nodes_from([0, 1], fish="one")
        D.add_nodes_from([2, 3], fish="two")
        D.add_nodes_from([4], fish="red")
        D.add_nodes_from([5], fish="blue")
        D.add_edges_from([(0, 1), (2, 3), (0, 4), (2, 5)])
        self.D = D

        N = nx.Graph()
        N.add_nodes_from([0, 1], margin=-2)
        N.add_nodes_from([2, 3], margin=-2)
        N.add_nodes_from([4], margin=-3)
        N.add_nodes_from([5], margin=-4)
        N.add_edges_from([(0, 1), (2, 3), (0, 4), (2, 5)])
        self.N = N

        F = nx.Graph()
        F.add_node(1, margin=0.5)
        F.add_nodes_from([0, 2, 3], margin=1.5)
        F.add_edges_from([(0, 3), (1, 3), (2, 3)], weight=0.5)
        F.add_edge(0, 2, weight=1)
        self.F = F

        M = nx.Graph()
        M.add_nodes_from([1, 2], margin=-1)
        M.add_nodes_from([3], margin=1)
        M.add_nodes_from([4], margin=2)
        M.add_edges_from([(3, 4), (1, 2), (1, 3)])
        self.M = M

    def teardown_method(self):
        del self.G
        del self.D
        del self.N
        del self.F
        del self.M

    def test_attribute_assortativity_undirected(self):
        r = nx.builtin.attribute_assortativity_coefficient(self.G, attribute="fish")
        assert r == 6.0 / 22.0

    def test_attribute_assortativity_directed(self):
        r = nx.builtin.attribute_assortativity_coefficient(self.D, attribute="fish")
        assert r == 1.0 / 3.0

    def test_attribute_assortativity_negative(self):
        r = nx.builtin.attribute_assortativity_coefficient(self.N, attribute="margin")
        assert almost_equal(r, -0.230767, places=4)

    def test_attribute_assortativity_float(self):
        r = nx.builtin.attribute_assortativity_coefficient(self.F, attribute="margin")
        assert almost_equal(r, -0.14286, places=4)

    def test_attribute_assortativity_mixed(self):
        r = nx.builtin.attribute_assortativity_coefficient(self.M, attribute="margin")
        assert almost_equal(r, -0.09091, places=4)


@pytest.mark.usefixtures("graphscope_session")
class TestNumericAssortativity:
    def setup_method(self):
        N = nx.Graph()
        N.add_nodes_from([0, 1], margin=-2)
        N.add_nodes_from([2, 3], margin=-2)
        N.add_nodes_from([4], margin=-3)
        N.add_nodes_from([5], margin=-4)
        N.add_edges_from([(0, 1), (2, 3), (0, 4), (2, 5)])
        self.N = N

        F = nx.Graph()
        F.add_node(1, margin=0.5)
        F.add_nodes_from([0, 2, 3], margin=1.5)
        F.add_edges_from([(0, 3), (1, 3), (2, 3)], weight=0.5)
        F.add_edge(0, 2, weight=1)
        self.F = F

        M = nx.Graph()
        M.add_nodes_from([1, 2], margin=-1)
        M.add_nodes_from([3], margin=1)
        M.add_nodes_from([4], margin=2)
        M.add_edges_from([(3, 4), (1, 2), (1, 3)])
        self.M = M

        P = nx.DiGraph()
        P.add_nodes_from([1, 2], margin=-1)
        P.add_nodes_from([3], margin=1)
        P.add_nodes_from([4], margin=2)
        P.add_edges_from([(3, 4), (1, 2), (1, 3)])
        self.P = P

    def teardown_method(self):
        del self.N
        del self.F
        del self.M

    def test_numeric_assortativity_negative(self):
        r = nx.builtin.numeric_assortativity_coefficient(self.N, attribute="margin")
        assert almost_equal(r, -0.2903, places=4)

    def test_numeric_assortativity_directed(self):
        r = nx.builtin.numeric_assortativity_coefficient(self.P, attribute="margin")
        assert almost_equal(r, 0.7559, places=4)

    def test_numeric_assortativity_float(self):
        r = nx.builtin.numeric_assortativity_coefficient(self.F, attribute="margin")
        assert almost_equal(r, -0.1429, places=4)

    def test_numeric_assortativity_mixed(self):
        r = nx.builtin.numeric_assortativity_coefficient(self.M, attribute="margin")
        assert almost_equal(r, 0.4340, places=4)
