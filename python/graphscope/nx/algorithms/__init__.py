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

import inspect
import sys

import networkx

from graphscope.nx.algorithms import builtin
from graphscope.nx.algorithms import tests

# NB: currently we don't what to make "builtin" in precedence to networkx's
# pure python implementation.
#
# After our built-in is ready, you just need to uncomment the following line.
#
# from python.graphscope.nx.algorithms.builtin import *
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import internal_name

existing_names = set(
    k for k, _ in inspect.getmembers(sys.modules[__name__]) if not internal_name(k)
)
mod = sys.modules[__name__]

__all__ = list(existing_names)

for name, func in inspect.getmembers(
    import_as_graphscope_nx(networkx.algorithms, expand=True)
):
    if callable(func) or inspect.isclass(func):
        if not internal_name(name) and name not in existing_names:
            __all__.append(name)
            setattr(mod, name, func)

# NB: here are builtin algorithms which exactly equivalent to networkx's pure python
# implementation.
from graphscope.nx.algorithms.builtin import average_clustering
from graphscope.nx.algorithms.builtin import average_shortest_path_length
from graphscope.nx.algorithms.builtin import betweenness_centrality
from graphscope.nx.algorithms.builtin import closeness_centrality
from graphscope.nx.algorithms.builtin import clustering
from graphscope.nx.algorithms.builtin import degree_centrality
from graphscope.nx.algorithms.builtin import edge_boundary
from graphscope.nx.algorithms.builtin import eigenvector_centrality
from graphscope.nx.algorithms.builtin import eigenvector_centrality_numpy
from graphscope.nx.algorithms.builtin import has_path
from graphscope.nx.algorithms.builtin import hits
from graphscope.nx.algorithms.builtin import hits_scipy
from graphscope.nx.algorithms.builtin import in_degree_centrality
from graphscope.nx.algorithms.builtin import is_simple_path
from graphscope.nx.algorithms.builtin import katz_centrality
from graphscope.nx.algorithms.builtin import node_boundary
from graphscope.nx.algorithms.builtin import out_degree_centrality
from graphscope.nx.algorithms.builtin import triangles
