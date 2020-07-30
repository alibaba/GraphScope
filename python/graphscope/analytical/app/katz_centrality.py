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

__all__ = ["katz_centrality"]


@not_compatible_for("arrow_property", "dynamic_property")
def katz_centrality(
    graph, alpha=0.1, beta=1.0, tolerance=1e-06, max_round=100, normalized=True
):
    """Compute the Katz centrality.

    See more details for Katz centrality here:
    https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.katz_centrality_numpy.html

    Args:
        graph (:class:`Graph`): A projected simple graph.
        alpha (float, optional): Auttenuation factor. Defaults to 0.1.
        beta (float, optional): Weight attributed to the immediate neighborhood. Defaults to 1.0.
        tolerance (float, optional): Error tolerance. Defaults to 1e-06.
        max_round (int, optional): Maximun number of rounds. Defaults to 100.
        normalized (bool, optional): Whether to normalize result values. Defaults to True.

    Returns:
        :class:`VertexDatacontext`: A context with each vertex assigned with the computed katz_centrality.

    Examples:

    .. code:: python

        import graphscope as gs
        s = gs.session()
        g = s.load_from('The parameters for loading a graph...')
        pg = g.project_to_simple(v_label='vlabel', e_label='elabel')
        r = gs.katz_centrality(pg)
        s.close()

    """
    alpha = float(alpha)
    beta = float(beta)
    tolerance = float(tolerance)
    max_round = int(max_round)
    normalized = bool(normalized)
    return AppAssets(algo="katz_centrality")(
        graph, alpha, beta, tolerance, max_round, normalized
    )
