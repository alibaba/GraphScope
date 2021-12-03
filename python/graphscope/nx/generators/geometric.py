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
"""
Generators for some directed graphs, including growing network (GN) graphs and
scale-free graphs.

"""

import networkx.generators.geometric

from graphscope.framework.errors import UnimplementedError
from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.generators.geometric)


def navigable_small_world_graph(n, p=1, q=1, r=2, dim=2, seed=None):
    # graphscope not support graph with tuple node
    raise UnimplementedError("navigable_small_world_graph not support in graphscope.nx")
