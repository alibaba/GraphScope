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
import networkx.algorithms.tests.test_planarity
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_planarity,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_planarity import TestLRPlanarity


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestLRPlanarity)
class TestLRPlanarity:
    @pytest.mark.skip(reason="not support multigraph")
    def test_planar_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_non_planar_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support OrderedGraph")
    def test_graph1(self):
        pass

    @pytest.mark.skip(reason="not support OrderedGraph")
    def test_graph2(self):
        pass

    @pytest.mark.skip(reason="not support OrderedGraph")
    def test_graph3(self):
        pass
