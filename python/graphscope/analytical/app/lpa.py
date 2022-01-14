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

__all__ = ["lpa", "cdlp", "lpa_u2i"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def lpa(graph, max_round=10):
    """Evaluate Community Detection with Label Propagation.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        max_round (int, optional): Maximum rounds. Defaults to 10.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with a community ID, will be evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.lpa(pg, max_round=10)
        >>> sess.close()
    """
    max_round = int(max_round)
    return AppAssets(algo="cdlp", context="vertex_data")(graph, max_round)


@not_compatible_for(
    "dynamic_property", "arrow_projected", "dynamic_projected", "arrow_flattened"
)
def lpa_u2i(graph, max_round=10):
    """Evaluate (multi-) label propagation on a property graph.

    Args:
        graph (:class:`graphscope.Graph`): A property graph.
        max_round (int, optional): Maximum number of rounds. Defaults to 10.

    Returns:
        :class:`graphscope.framework.context.LabeledVertexPropertyContextDAGNode`:
            A context with each vertex, following an array of propagated labels, evaluated in eager mode.

    Examples:

    .. code:: python

        import graphscope as gs
        g = gs.g()
        # Load some data
        r = gs.lpa(g)
        s.close()

    """
    max_round = int(max_round)
    return AppAssets(algo="lpau2i", context="labeled_vertex_property")(graph, max_round)


cdlp = lpa
