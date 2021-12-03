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

import networkx.generators.tests.test_geometric
import pytest
from networkx.generators.tests.test_geometric import TestNavigableSmallWorldGraph

from graphscope.framework.errors import UnimplementedError
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.generators.tests.test_geometric,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestNavigableSmallWorldGraph)
class TestNavigableSmallWorldGraph:
    def test_navigable_small_world(self):
        with pytest.raises(UnimplementedError):
            G = nx.navigable_small_world_graph(5, p=1, q=0, seed=42)
