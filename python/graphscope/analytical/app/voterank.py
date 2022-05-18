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

__all__ = ["voterank"]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def voterank(graph, num_of_nodes=0):
    """Evalute VoteRank on a graph.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        num_of_nodes (unsigned long int, optional): Number of ranked nodes to extract. Default all nodes.

    Returns:
        :voterank : list
         Ordered list of computed seeds. Only nodes with positive number of votes are returned.


    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.voterank(pg, num_of_nodes=10)
        >>> sess.close()
    """
    num_of_nodes = int(num_of_nodes)
    c = AppAssets(algo="voterank", context="vertex_data")(graph, num_of_nodes)
    r = c.to_dataframe({"id": "v.id", "result": "r"})
    r = r[r["result"] != 0].sort_values(by=["result"])
    return r["id"].tolist()
