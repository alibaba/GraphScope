#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.
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
import networkx.algorithms.components.tests.test_attracting
import networkx.algorithms.components.tests.test_biconnected
import networkx.algorithms.components.tests.test_connected
import networkx.algorithms.components.tests.test_semiconnected
import networkx.algorithms.components.tests.test_strongly_connected
import networkx.algorithms.components.tests.test_weakly_connected
import pytest
from networkx.generators.lattice import grid_2d_graph

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.components.tests.test_attracting,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.components.tests.test_biconnected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.components.tests.test_connected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.components.tests.test_semiconnected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.components.tests.test_strongly_connected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.components.tests.test_weakly_connected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestConnected)
class TestConnected:
    @classmethod
    def setup_class(cls):
        # NB: graphscope.nx does not support tuple graph, we construct from
        # networkx and then convert to nx.Graph
        H1 = cnlti(grid_2d_graph(2, 2), first_label=0, ordering="sorted")
        G1 = nx.Graph(H1)
        G2 = cnlti(nx.lollipop_graph(3, 3), first_label=4, ordering="sorted")
        G3 = cnlti(nx.house_graph(), first_label=10, ordering="sorted")
        cls.G = nx.union(G1, G2)
        cls.G = nx.union(cls.G, G3)
        cls.DG = nx.DiGraph([(1, 2), (1, 3), (2, 3)])
        grid = cnlti(grid_2d_graph(4, 4), first_label=1)
        cls.grid = nx.Graph(grid)

        cls.gc = []
        G = nx.DiGraph()
        G.add_edges_from(
            [
                (1, 2),
                (2, 3),
                (2, 8),
                (3, 4),
                (3, 7),
                (4, 5),
                (5, 3),
                (5, 6),
                (7, 4),
                (7, 6),
                (8, 1),
                (8, 7),
            ]
        )
        C = [[3, 4, 5, 7], [1, 2, 8], [6]]
        cls.gc.append((G, C))

        G = nx.DiGraph()
        G.add_edges_from([(1, 2), (1, 3), (1, 4), (4, 2), (3, 4), (2, 3)])
        C = [[2, 3, 4], [1]]
        cls.gc.append((G, C))

        G = nx.DiGraph()
        G.add_edges_from([(1, 2), (2, 3), (3, 2), (2, 1)])
        C = [[1, 2, 3]]
        cls.gc.append((G, C))

        # Eppstein's tests
        G = nx.DiGraph({0: [1], 1: [2, 3], 2: [4, 5], 3: [4, 5], 4: [6], 5: [], 6: []})
        C = [[0], [1], [2], [3], [4], [5], [6]]
        cls.gc.append((G, C))

        G = nx.DiGraph({0: [1], 1: [2, 3, 4], 2: [0, 3], 3: [4], 4: [3]})
        C = [[0, 1, 2], [3, 4]]
        cls.gc.append((G, C))

        G = nx.DiGraph()
        C = []
        cls.gc.append((G, C))

@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestIsSemiconnected)
class TestIsSemiconnected():
    def test_undirected(self):
        pytest.raises(nx.NetworkXNotImplemented, nx.is_semiconnected, nx.Graph())

    def test_empty(self):
        pytest.raises(nx.NetworkXPointlessConcept, nx.is_semiconnected, nx.DiGraph())
