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

__all__ = ["hits"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def hits(graph, tolerance=0.01, max_round=100, normalized=True):
    """Compute HITS on `graph`.

    Hyperlink-Induced Topic Search (HITS; also known as hubs and authorities)
    is a link analysis algorithm that rates Web pages. See more here:
    https://en.wikipedia.org/wiki/HITS_algorithm

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        tolerance (float, optional): Defaults to 0.01.
        max_round (int, optional): Defaults to 100.
        normalized (bool, optional): Whether to normalize the result to 0-1. Defaults to True.

    Returns:
        :class:`graphscope.framework.context.VertexPropertyContextDAGNode`:
            A context with each vertex assigned with the HITS value, evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.hits(pg, tolerance=0.01, max_round=10, normalized=True)
        >>> sess.close()
    """
    tolerance = float(tolerance)
    max_round = int(max_round)
    normalized = bool(normalized)
    return AppAssets(algo="hits", context="vertex_property")(
        graph, tolerance, max_round, normalized
    )
