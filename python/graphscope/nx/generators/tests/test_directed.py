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

import pytest
from networkx.generators.tests.test_directed import TestGeneratorsDirected
from networkx.generators.tests.test_directed import TestRandomKOutGraph
from networkx.generators.tests.test_directed import TestUniformRandomKOutGraph

import graphscope.nx as nx
from graphscope.nx.classes import Graph
from graphscope.nx.classes import MultiDiGraph
from graphscope.nx.generators.directed import gn_graph
from graphscope.nx.generators.directed import gnc_graph
from graphscope.nx.generators.directed import gnr_graph
from graphscope.nx.generators.directed import random_k_out_graph
from graphscope.nx.generators.directed import random_uniform_k_out_graph
from graphscope.nx.generators.directed import scale_free_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorsDirected)
class TestGeneratorsDirected:
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestRandomKOutGraph)
class TestRandomKOutGraph:
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestUniformRandomKOutGraph)
class TestUniformRandomKOutGraph:
    pass
