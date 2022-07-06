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

__all__ = ["avg_clustering", "clustering"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def clustering(graph, degree_threshold=1000000000):
    """Local clustering coefficient of a node in a Graph is the fraction
    of pairs of the node’s neighbors that are adjacent to each other.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        degree_threshold (int, optional): Filter super vertex which degree is greater than threshold. Default to 1e9.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned the computed clustering value, will be evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.clustering(pg)
        >>> sess.close()
    """
    degree_threshold = int(degree_threshold)
    if graph.is_directed():
        return AppAssets(algo="clustering", context="vertex_data")(
            graph, degree_threshold
        )
    else:
        return AppAssets(algo="lcc", context="vertex_data")(graph, degree_threshold)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property", "undirected")
def avg_clustering(graph, degree_threshold=1000000000):
    """Compute the average clustering coefficient for the directed graph.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        degree_threshold (int, optional): Filter super vertex which degree is greater than threshold. Default to 1e9.

    Returns:
        r: float
            The average clustering coefficient.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.avg_clustering(pg)
        >>> print(c.to_numpy("r", axis=0)[0])
        >>> sess.close()
    """
    degree_threshold = int(degree_threshold)
    return AppAssets(algo="avg_clustering", context="tensor")(graph, degree_threshold)
