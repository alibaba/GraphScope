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

import graphscope.nx as nx


class TestRandomClusteredGraph:
    def test_valid(self):
        node = [1, 1, 1, 2, 1, 2, 0, 0]
        tri = [0, 0, 0, 0, 0, 1, 1, 1]
        joint_degree_sequence = zip(node, tri)
        G = nx.random_clustered_graph(joint_degree_sequence)
        assert G.number_of_nodes() == 8
        assert G.number_of_edges() == 7

    def test_valid2(self):
        G = nx.random_clustered_graph([(1, 2), (2, 1), (1, 1), (1, 1), (1, 1), (2, 0)])
        assert G.number_of_nodes() == 6
        assert G.number_of_edges() == 10

    def test_invalid1(self):
        pytest.raises(
            (TypeError, nx.NetworkXError),
            nx.random_clustered_graph,
            [[1, 1], [2, 1], [0, 1]],
        )

    def test_invalid2(self):
        pytest.raises(
            (TypeError, nx.NetworkXError),
            nx.random_clustered_graph,
            [[1, 1], [1, 2], [0, 1]],
        )
