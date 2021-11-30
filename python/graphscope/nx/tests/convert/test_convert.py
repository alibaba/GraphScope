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
from networkx.tests.test_convert import TestConvert

import graphscope.nx as nx
from graphscope.nx.convert import from_dict_of_dicts
from graphscope.nx.convert import from_dict_of_lists
from graphscope.nx.convert import to_dict_of_dicts
from graphscope.nx.convert import to_dict_of_lists
from graphscope.nx.convert import to_networkx_graph
from graphscope.nx.generators.classic import barbell_graph
from graphscope.nx.generators.classic import cycle_graph
from graphscope.nx.tests.utils import assert_edges_equal
from graphscope.nx.tests.utils import assert_graphs_equal
from graphscope.nx.tests.utils import assert_nodes_equal
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestConvert)
class TestConvert:
    def test_attribute_dict_integrity(self):
        # we must not replace dict-like graph data structures with dicts
        G = nx.Graph()
        G.add_nodes_from("abc")
        H = to_networkx_graph(G, create_using=nx.Graph)
        assert list(H.nodes) == list(G.nodes)
        H = nx.Graph(G)
        assert list(H.nodes) == list(G.nodes)

    def test_custom_node_attr_dict_safekeeping(self):
        pass
