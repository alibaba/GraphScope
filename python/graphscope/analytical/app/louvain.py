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
from graphscope.framework.errors import InvalidArgumentError

__all__ = [
    "louvain",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def louvain(graph, min_progress=1000, progress_tries=1):
    """Compute best partition on the `graph` by louvain.

    Args:
        graph (:class:`graphscope.Graph`): A simple undirected graph.
        min_progress: The minimum delta X required to be considered progress, where X is the number of nodes
                      that have changed their community on a particular pass.
                      Delta X is then the difference in number of nodes that changed communities
                      on the current pass compared to the previous pass.
        progress_tries: number of times the min_progress setting is not met
                        before exiting form the current level and compressing the graph.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with id of community it belongs to, evaluated in eager mode.

    References:
        [1] Blondel, V.D. et al. Fast unfolding of communities in large networks. J. Stat. Mech 10008, 1-12(2008).

        [2] https://github.com/Sotera/distributed-graph-analytics

        [3] https://sotera.github.io/distributed-graph-analytics/louvain/

    Notes:
        louvain now only support undirected graph. If input graph is directed graph, louvain would raise
        an InvalidArgumentError.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess, directed=False)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.louvain(pg, min_progress=1000, progress_tries=1)
        >>> sess.close()
    """
    if graph.is_directed():
        raise InvalidArgumentError("Louvain not support directed graph.")
    return AppAssets(algo="louvain", context="vertex_data")(
        graph, min_progress, progress_tries
    )
