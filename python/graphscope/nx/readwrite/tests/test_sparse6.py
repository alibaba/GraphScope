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

import networkx.readwrite.tests.test_sparse6
import pytest
from networkx.readwrite.tests.test_sparse6 import TestWriteSparse6

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.readwrite.tests.test_sparse6,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestWriteSparse6)
class TestWriteSparse6:
    @pytest.mark.slow
    def test_very_large_empty_graph(self):
        G = nx.empty_graph(258049)
        result = BytesIO()
        nx.write_sparse6(G, result)
        assert result.getvalue() == b">>sparse6<<:~~???~?@\n"
