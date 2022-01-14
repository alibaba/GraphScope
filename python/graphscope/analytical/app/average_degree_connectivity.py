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
# Author: Ning Xin
#


from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.app import project_to_simple

__all__ = ["average_degree_connectivity"]


@project_to_simple
@not_compatible_for("arrow_property")
def average_degree_connectivity(graph, source="in+out", target="in+out", weight=None):
    """Compute the average degree connectivity of graph.

    The average degree connectivity is the average nearest neighbor degree of
    nodes with degree k. For weighted graphs, an analogous measure can
    be computed using the weighted average neighbors degree defined in
    [1]_, for a node `i`, as

    .. math::

        k_{nn,i}^{w} = \frac{1}{s_i} sum_{j in N(i)} w_{ij} k_j

    where `s_i` is the weighted degree of node `i`,
    `w_{ij}` is the weight of the edge that links `i` and `j`,
    and `N(i)` are the neighbors of node `i`.

    Parameters
    ----------
    graph : (:class:`graphscope.Graph`): A simple graph.

    source :  "in"|"out"|"in+out" (default:"in+out")
       Directed graphs only. Use "in"- or "out"-degree for source node.

    target : "in"|"out"|"in+out" (default:"in+out"
       Directed graphs only. Use "in"- or "out"-degree for target node.

    weight : string or None, optional (default=None)
       The edge attribute that holds the numerical value used as a weight.
       If None, then each edge has weight 1.

    Returns
    -------
    d : dict
       A dictionary keyed by degree k with the value of average connectivity.

    Raises
    ------
    ValueError
        If either `source` or `target` are not one of 'in',
        'out', or 'in+out'.

    Examples
    --------
    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_modern_graph
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_modern_graph(sess)
        >>> g.schema
        >>> c = graphscope.average_degree_connectivity(g, weight="weight")
        >>> sess.close()

    References
    ----------
    .. [1] A. Barrat, M. Barthélemy, R. Pastor-Satorras, and A. Vespignani,
       "The architecture of complex weighted networks".
       PNAS 101 (11): 3747–3752 (2004).
    """
    if graph.is_directed():
        if source not in ("in", "out", "in+out"):
            raise ValueError('source must be one of "in", "out", or "in+out"')
        if target not in ("in", "out", "in+out"):
            raise ValueError('target must be one of "in", "out", or "in+out"')
    ctx = AppAssets(algo="average_degree_connectivity", context="tensor")(
        graph, source, target
    )
    res_list = ctx.to_numpy("r", axis=0).tolist()
    res_list = [i for item in res_list for i in item]
    degree = [int(i) for i in res_list[0::2]]
    degree_connectivity = res_list[1::2]
    res = dict(zip(degree, degree_connectivity))
    return res
