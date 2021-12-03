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

__all__ = ["pagerank_nx"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def pagerank_nx(graph, alpha=0.85, max_iter=100, tol=1e-06):
    """Evalute PageRank on a graph in NetworkX version.

    Args:
        graph (Graph): A simple graph.
        alpha (float, optional): Dumping factor. Defaults to 0.85.
        max_iter (int, optional): Maximum number of iteration. Defaults to 100.
        tol (float, optional): Error tolerance used to check convergence in power method solver.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with the pagerank value, evaluated in eager mode.

    Examples:

    .. code:: python

        import graphscope as gs
        g = gs.g()
        # Load some data, then project to a simple graph (if needed).
        pg = g.project(vertices={"vlabel": []}, edges={"elabel": []})
        r = gs.pagerank(pg, alpha=0.85, max_iter=10)
        s.close()

    """
    alpha = float(alpha)
    max_iter = int(max_iter)
    return AppAssets(algo="pagerank_nx", context="vertex_data")(
        graph, alpha, max_iter, tol
    )
