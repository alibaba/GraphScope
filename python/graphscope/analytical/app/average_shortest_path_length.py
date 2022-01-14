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
#


from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.app import project_to_simple

__all__ = ["average_shortest_path_length"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def average_shortest_path_length(graph, weight=None):
    r"""Compute the average shortest path length.

    The average shortest path length is

    .. math::

       a =\sum_{s,t \in V} \frac{d(s, t)}{n(n-1)}

    where `V` is the set of nodes in `G`, `d(s, t)` is the shortest path from `s` to `t`,
    and `n` is the number of nodes in `G`.

    Parameters
    ----------
    graph : (:class:`graphscope.Graph`): A simple graph.
    weight: (str, optional): The edge data key corresponding to the edge weight.
        Note that property under multiple labels should have the consistent index.
        Defaults to None.

    Returns
    -------
    r : float
        The average shortest path length.

    Examples
    --------
    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_modern_graph
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_modern_graph(sess)
        >>> g.schema
        >>> c = graphscope.average_shortest_path_length(g, weight="weight")
        >>> sess.close()
    """
    ctx = AppAssets(algo="sssp_average_length", context="tensor")(graph)
    return ctx.to_numpy("r", axis=0)[0]
