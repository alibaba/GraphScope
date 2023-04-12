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
    "maximal_matching",
    "maximal_matching_2",
    "maximal_matching_3",
    "maximal_independent_set",
    "maximal_independent_set_2",
    "minimal_dominating_set",
    "minimal_dominating_set_2",
    "minimal_edge_cover",
    "minimal_vertex_cover",
    "minimal_vertex_cover_2",
    "minimal_vertex_cover_3",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def maximal_matching(graph):
    """Evaluate Maximal Matching on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of matching pairs.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.maximal_matching(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_mm", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def maximal_matching_2(graph):
    """Evaluate Maximal Matching (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of matching pairs.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.maximal_matching_2(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_mm_opt", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def maximal_matching_3(graph):
    """Evaluate Maximal Matching (version 3) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of matching pairs.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.maximal_matching_3(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_mm_opt_2", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def maximal_independent_set(graph):
    """Evaluate Maximal Independent Set on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the size of max independent set.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.maximal_independent_set(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_mis", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def maximal_independent_set_2(graph):
    """Evaluate Maximal Independent Set (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the size of max independent set.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.maximal_independent_set_2(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_mis_2", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def minimal_dominating_set(graph):
    """Evaluate Minimal Dominating Set on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the size of min dominating set.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.minimal_dominating_set(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_min_dominating_set", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def minimal_dominating_set_2(graph):
    """Evaluate Minimal Dominating Set (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the size of min dominating set.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.minimal_dominating_set_2(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_min_dominating_set_2", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def minimal_edge_cover(graph):
    """Evaluate Minimal Edge Cover on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of edges.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.minimal_edge_cover(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_min_edge_cover", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def minimal_vertex_cover(graph):
    """Evaluate Minimal Vertex Cover on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of vertices.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.minimal_vertex_cover(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_min_cover", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def minimal_vertex_cover_2(graph):
    """Evaluate Minimal Vertex Cover (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of vertices.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.minimal_vertex_cover_2(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_min_cover_greedy", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def minimal_vertex_cover_3(graph):
    """Evaluate Minimal Vertex Cover (version 3) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of vertices.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.minimal_vertex_cover_3(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_min_cover_greedy_2", context="tensor")(graph)
