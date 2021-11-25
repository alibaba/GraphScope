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

import graphscope.nx as nx


@pytest.mark.usefixtures("graphscope_session")
class BaseTestAttributeMixing:
    def setup_method(cls):
        G = nx.Graph()
        G.add_nodes_from([0, 1], fish='one')
        G.add_nodes_from([2, 3], fish='two')
        G.add_nodes_from([4], fish='red')
        G.add_nodes_from([5], fish='blue')
        G.add_edges_from([(0, 1), (2, 3), (0, 4), (2, 5)])
        cls.G = G

        D = nx.DiGraph()
        D.add_nodes_from([0, 1], fish='one')
        D.add_nodes_from([2, 3], fish='two')
        D.add_nodes_from([4], fish='red')
        D.add_nodes_from([5], fish='blue')
        D.add_edges_from([(0, 1), (2, 3), (0, 4), (2, 5)])
        cls.D = D

        S = nx.Graph()
        S.add_nodes_from([0, 1], fish='one')
        S.add_nodes_from([2, 3], fish='two')
        S.add_nodes_from([4], fish='red')
        S.add_nodes_from([5], fish='blue')
        S.add_edge(0, 0)
        S.add_edge(2, 2)
        cls.S = S


@pytest.mark.usefixtures("graphscope_session")
class BaseTestDegreeMixing:
    def setup_method(cls):
        cls.P4 = nx.path_graph(4)
        cls.D = nx.DiGraph()
        cls.D.add_edges_from([(0, 2), (0, 3), (1, 3), (2, 3)])
        cls.S = nx.Graph()
        cls.S.add_edges_from([(0, 0), (1, 1)])
