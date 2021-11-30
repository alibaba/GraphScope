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

import io
import os
import tempfile

import pytest
from networkx.readwrite.tests.test_adjlist import TestAdjlist
from networkx.readwrite.tests.test_adjlist import TestMultilineAdjlist

import graphscope.nx as nx
from graphscope.nx.tests.utils import assert_edges_equal
from graphscope.nx.tests.utils import assert_graphs_equal
from graphscope.nx.tests.utils import assert_nodes_equal
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAdjlist)
class TestAdjlist:
    def test_parse_adjlist(self):
        lines = ["1 2 5", "2 3 4", "3 5", "4", "5"]
        nx.parse_adjlist(lines, nodetype=int)  # smoke test
        with pytest.raises(TypeError):
            nx.parse_adjlist(lines, nodetype="int")
        lines = ["1 2 5", "2 b", "c"]
        with pytest.raises(ValueError):
            nx.parse_adjlist(lines, nodetype=int)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMultilineAdjlist)
class TestMultilineAdjlist:
    pass
