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

__all__ = ["eigenvector_centrality"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def eigenvector_centrality(graph, tolerance=1e-06, max_round=100):
    """Compute the eigenvector centrality for the `graph`.
    See more about eigenvector centrality here:
    https://networkx.org/documentation/networkx-1.10/reference/generated/networkx.algorithms.centrality.eigenvector_centrality.html

    Args:
        graph (:class:`Graph`): A simple graph.
        tolerance (float, optional): Defaults to 1e-06.
        max_round (int, optional): Defaults to 100.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with a gv-centrality, evaluated in eager mode.

    Examples:

    .. code:: python

        import graphscope as gs
        g = gs.g()
        # Load some data, then project to a simple graph (if needed).
        pg = g.project(vertices={"vlabel": []}, edges={"elabel": []})
        r = gs.eigenvector_centrality(pg)
        s.close()

    """
    tolerance = float(tolerance)
    max_round = int(max_round)
    return AppAssets(algo="eigenvector_centrality", context="vertex_data")(
        graph, tolerance, max_round
    )
