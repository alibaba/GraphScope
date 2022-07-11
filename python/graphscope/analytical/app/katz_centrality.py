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

__all__ = ["katz_centrality"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def katz_centrality(
    graph,
    alpha=0.1,
    beta=1.0,
    tolerance=1e-06,
    max_round=100,
    normalized=True,
    degree_threshold=1e9,
):
    """Compute the Katz centrality.

    See more details for Katz centrality here:
    https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.katz_centrality_numpy.html

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        alpha (float, optional): Auttenuation factor. Defaults to 0.1.
        beta (float, optional): Weight attributed to the immediate neighborhood. Defaults to 1.0.
        tolerance (float, optional): Error tolerance. Defaults to 1e-06.
        max_round (int, optional): Maximun number of rounds. Defaults to 100.
        normalized (bool, optional): Whether to normalize result values. Defaults to True.
        degree_threshold (int, optional): Filter super vertex which degree is greater than threshold. Default to 1e9.
    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with the computed katz_centrality, evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.katz_centrality(pg)
        >>> sess.close()
    """
    alpha = float(alpha)
    beta = float(beta)
    tolerance = float(tolerance)
    max_round = int(max_round)
    normalized = bool(normalized)
    degree_threshold = int(degree_threshold)
    return AppAssets(algo="katz_centrality", context="vertex_data")(
        graph, alpha, beta, tolerance, max_round, normalized, degree_threshold
    )
