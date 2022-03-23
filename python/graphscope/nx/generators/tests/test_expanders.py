#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# fmt: off
import pytest
from networkx import adjacency_matrix
from networkx.generators.tests.test_expanders import test_chordal_cycle_graph
from networkx.generators.tests.test_expanders import test_margulis_gabber_galil_graph
from networkx.generators.tests.test_expanders import \
    test_margulis_gabber_galil_graph_badinput

#fmt: off

try:
    from networkx.generators.tests.test_expanders import test_paley_graph
except ImportError:
    # NetworkX<=2.4 not contains paley_graph
    test_paley_graph = lambda: None


import graphscope.nx as nx
from graphscope.nx import number_of_nodes
from graphscope.nx.generators.expanders import chordal_cycle_graph
from graphscope.nx.generators.expanders import margulis_gabber_galil_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context

try:
    from graphscope.nx.generators.expanders import paley_graph
except ImportError:
    # NetworkX <= 2.4
    pass


@pytest.mark.usefixtures("graphscope_session")
def test_margulis_gabber_galil_graph():
    for n in 2, 3, 5, 6, 10:
        g = margulis_gabber_galil_graph(n)
        assert number_of_nodes(g) == n * n
        for node in g:
            assert g.degree(node) == 8
            assert len(node) == 2
            for i in node:
                assert int(i) == i
                assert 0 <= i < n

    np = pytest.importorskip("numpy")
    # scipy submodule has to import explicitly
    linalg = pytest.importorskip("scipy.linalg")
    # Eigenvalues are already sorted using the scipy eigvalsh,
    # but the implementation in numpy does not guarantee order.
    w = sorted(linalg.eigvalsh(adjacency_matrix(g).A))
    assert w[-2] < 5 * np.sqrt(2)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_chordal_cycle_graph)
def test_chordal_cycle_graph():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_margulis_gabber_galil_graph_badinput)
def test_margulis_gabber_galil_graph_badinput():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_paley_graph)
def test_paley_graph():
    pass
