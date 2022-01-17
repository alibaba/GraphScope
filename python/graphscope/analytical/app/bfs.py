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

__all__ = ["bfs"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bfs(graph, src=0):
    """Breadth first search from the src on projected simple graph.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        src (optional): Source vertex of breadth first search. The type should be consistent
            with the id type of the `graph`, that is, it's `int` or `str` depending
            on the `oid_type` is `int64_t` or `string` of the `graph`. Defaults to 0.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex with a distance from the source, will be evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.bfs(pg, src=6)
        >>> sess.close()
    """
    return AppAssets(algo="bfs", context="vertex_data")(graph, src)
