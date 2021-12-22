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
from graphscope.framework.app import project_to_simple

__all__ = ["bfs", "property_bfs"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bfs(graph, src=0):
    """Breadth first search from the src on projected simple graph.

    Args:
        graph (:class:`Graph`): A simple graph.
        src (optional): Source vertex of breadth first search. The type should be consistent
            with the id type of the `graph`, that is, it's `int` or `str` depending
            on the `oid_type` is `int64_t` or `string` of the `graph`. Defaults to 0.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex with a distance from the source, will be evaluated in eager mode.

    Examples:

    .. code:: python

        import graphscope as gs
        g = gs.g()
        # Load some data, then project to a simple graph (if needed).
        pg = g.project(vertices={"vlabel": []}, edges={"elabel": []})
        r = gs.bfs(pg, 6)  # use 6 as source vertex
        s.close()

    """
    return AppAssets(algo="bfs", context="vertex_data")(graph, src)


@not_compatible_for("dynamic_property", "arrow_projected", "dynamic_projected")
def property_bfs(graph, src=0):
    """Breath first search from the src on property graph.

    Args:
        graph (:class:`Graph`): A property graph.
        src (optional): Source vertex of breadth first search. The type should be consistent
            with the id type of the `graph`, that is, it's `int` or `str` depending
            on the `oid_type` is `int64_t` or `string` of the `graph`. Defaults to 0.

    Returns:
        :class:`graphscope.framework.context.LabeledVertexDataContextDAGNode`:
            A context with each vertex with a distance from the source, evaluated in eager mode.

    Examples:

    .. code:: python

        import graphscope as gs
        sess = gs.session()
        g = sess.g()
        r = gs.property_bfs(g, 6)  # use 6 as source vertex
        s.close()

    """
    return AppAssets(algo="property_bfs", context="labeled_vertex_data")(graph, src)
