#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/readwrite/adjlist.py
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
from networkx.drawing.tests.test_layout import TestLayout
from networkx.generators.lattice import grid_2d_graph

import graphscope.nx as nx
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestLayout)
class TestLayout:
    @classmethod
    def setup_class(cls):
        cnlti = nx.convert_node_labels_to_integers
        Gi = cnlti(grid_2d_graph(5, 5), first_label=1, ordering="sorted")
        cls.Gi = nx.Graph(Gi)
        cls.Gs = nx.Graph()
        nx.add_path(cls.Gs, "abcdef")
        bigG = cnlti(grid_2d_graph(25, 25), first_label=1, ordering="sorted")
        cls.bigG = nx.Graph(bigG)

    @pytest.mark.skip(reason="Gi and bigG is not a tuple position graph.")
    def test_fixed_node_fruchterman_reingold(self):
        # Dense version (numpy based)
        pos = nx.circular_layout(self.Gi)
        npos = nx.spring_layout(self.Gi, pos=pos, fixed=[(0, 0)])
        assert tuple(pos[(0, 0)]) == tuple(npos[(0, 0)])
        # Sparse version (scipy based)
        pos = nx.circular_layout(self.bigG)
        npos = nx.spring_layout(self.bigG, pos=pos, fixed=[(0, 0)])
        for axis in range(2):
            assert almost_equal(pos[(0, 0)][axis], npos[(0, 0)][axis])  # noqa F821

    def test_center_wrong_dimensions(self):
        G = nx.path_graph(1)
        # in graphscope.nx the ids of the two methods are not identical.
        # assert id(nx.spring_layout) == id(nx.fruchterman_reingold_layout)
        pytest.raises(ValueError, nx.random_layout, G, center=(1, 1, 1))
        pytest.raises(ValueError, nx.circular_layout, G, center=(1, 1, 1))
        pytest.raises(ValueError, nx.planar_layout, G, center=(1, 1, 1))
        pytest.raises(ValueError, nx.spring_layout, G, center=(1, 1, 1))
        pytest.raises(ValueError, nx.spring_layout, G, dim=3, center=(1, 1))
        pytest.raises(ValueError, nx.spectral_layout, G, center=(1, 1, 1))
        pytest.raises(ValueError, nx.spectral_layout, G, dim=3, center=(1, 1))
        pytest.raises(ValueError, nx.shell_layout, G, center=(1, 1, 1))
        pytest.raises(ValueError, nx.spiral_layout, G, center=(1, 1, 1))
        pytest.raises(ValueError, nx.kamada_kawai_layout, G, center=(1, 1, 1))
