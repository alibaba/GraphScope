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
from networkx.tests.test_convert import TestConvert as _TestConvert
from networkx.utils import edges_equal
from networkx.utils import nodes_equal

import graphscope.nx as nx
from graphscope.nx.convert import from_dict_of_dicts
from graphscope.nx.convert import from_dict_of_lists
from graphscope.nx.convert import to_dict_of_dicts
from graphscope.nx.convert import to_dict_of_lists
from graphscope.nx.convert import to_networkx_graph
from graphscope.nx.generators.classic import barbell_graph
from graphscope.nx.generators.classic import cycle_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(_TestConvert)
class TestConvert:
    def test_attribute_dict_integrity(self):
        # we must not replace dict-like graph data structures with dicts
        G = nx.Graph()
        G.add_nodes_from("abc")
        H = to_networkx_graph(G, create_using=nx.Graph)
        assert sorted(list(H.nodes)) == sorted(list(G.nodes))
        H = nx.Graph(G)
        assert sorted(list(H.nodes)) == sorted(list(G.nodes))

    def test_graph(self):
        g = nx.cycle_graph(10)
        G = nx.Graph()
        G.add_nodes_from(g)
        G.add_weighted_edges_from((u, v, u) for u, v in g.edges())

        # Dict of dicts
        dod = to_dict_of_dicts(G)
        GG = from_dict_of_dicts(dod, create_using=nx.Graph)
        assert nodes_equal(sorted(G.nodes()), sorted(GG.nodes()))
        assert edges_equal(sorted(G.edges()), sorted(GG.edges()))
        GW = to_networkx_graph(dod, create_using=nx.Graph)
        assert nodes_equal(sorted(G.nodes()), sorted(GW.nodes()))
        assert edges_equal(sorted(G.edges()), sorted(GW.edges()))
        GI = nx.Graph(dod)
        assert nodes_equal(sorted(G.nodes()), sorted(GI.nodes()))
        assert edges_equal(sorted(G.edges()), sorted(GI.edges()))

        # Dict of lists
        dol = to_dict_of_lists(G)
        GG = from_dict_of_lists(dol, create_using=nx.Graph)
        # dict of lists throws away edge data so set it to none
        enone = [(u, v, {}) for (u, v, d) in G.edges(data=True)]
        assert nodes_equal(sorted(G.nodes()), sorted(GG.nodes()))
        assert edges_equal(enone, sorted(GG.edges(data=True)))
        GW = to_networkx_graph(dol, create_using=nx.Graph)
        assert nodes_equal(sorted(G.nodes()), sorted(GW.nodes()))
        assert edges_equal(enone, sorted(GW.edges(data=True)))
        GI = nx.Graph(dol)
        assert nodes_equal(sorted(G.nodes()), sorted(GI.nodes()))
        assert edges_equal(enone, sorted(GI.edges(data=True)))

    def test_custom_node_attr_dict_safekeeping(self):
        pass
