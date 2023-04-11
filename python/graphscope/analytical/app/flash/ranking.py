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

__all__ = [
    "pagerank",
    "articlerank",
    "personalized_pagerank",
    "hyperlink_induced_topic_search",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def pagerank(graph, delta=0.85, max_round=10):
    """Evaluate PageRank on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        delta (float, optional): Dumping factor. Defaults to 0.85.
        max_round (int, optional): Maximum number of rounds. Defaults to 10.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the pagerank value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.pagerank(pg, delta=0.85, max_round=10)
    """
    delta = float(delta)
    max_round = int(max_round)
    return AppAssets(algo="flash_pagerank", context="vertex_data")(
        graph, max_round, delta
    )


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def articlerank(graph, delta=0.85, max_round=10):
    """Evaluate ArticleRank on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        delta (float, optional): Dumping factor. Defaults to 0.85.
        max_round (int, optional): Maximum number of rounds. Defaults to 10.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the articlerank value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.articlerank(pg, delta=0.85, max_round=10)
    """
    delta = float(delta)
    max_round = int(max_round)
    return AppAssets(algo="flash_articlerank", context="vertex_data")(
        graph, max_round, delta
    )


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def personalized_pagerank(graph, source=1, max_round=10):
    """Evaluate Personalized PageRank on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.
        max_round (int, optional): Maximum number of rounds. Defaults to 10.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the personalized pagerank value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.personalized_pagerank(pg, source=1, max_round=10)
    """
    max_round = int(max_round)
    return AppAssets(algo="flash_ppr", context="vertex_data")(graph, source, max_round)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def hyperlink_induced_topic_search(graph, max_round=10):
    """Evaluate Hyperlink-induced Topic Search on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        max_round (int, optional): Maximum number of rounds. Defaults to 10.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the authority score value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.hyperlink_induced_topic_search(pg, max_round=10)
    """
    max_round = int(max_round)
    return AppAssets(algo="flash_hits", context="vertex_data")(graph, max_round)
