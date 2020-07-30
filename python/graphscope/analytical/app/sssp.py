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

from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for

__all__ = [
    "sssp",
    "property_sssp",
]


@not_compatible_for("arrow_property", "dynamic_property")
def sssp(graph, src=0):
    """Compute single source shortest path on the `graph`.

    Args:
        graph (:class:`Graph`): A projected simple graph.
        src (int, optional): The source vertex. Defaults to 0.

    Returns:
        :class:`VertexDataContext`: A context with each vertex assigned with the shortest distance from the src.

    Examples:

    .. code:: python

        import graphscope as gs
        s = gs.session()
        g = s.load_from('The parameters for loading a graph...')
        pg = g.project_to_simple(v_label='vlabel', e_label='elabel', v_prop=None, e_prop='distance')
        r = gs.sssp(pg, src=0)
        s.close()

    """
    return AppAssets(algo="sssp")(graph, src)


@not_compatible_for("dynamic_property", "arrow_projected", "dynamic_projected")
def property_sssp(graph, src=0):
    """Compute single source shortest path on graph G.

    Args:
        graph (Graph): a property graph.
        src (int, optional): the source. Defaults to 0.

    Returns:
        A context with each vertex assigned with the shortest distance from the src.
    """
    return AppAssets(algo="property_sssp")(graph, src)
