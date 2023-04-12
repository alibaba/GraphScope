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
    "bfs",
    "bfs_push",
    "bfs_pull",
    "bfs_undirected",
    "random_multi_bfs",
    "sssp",
    "sssp_undirected",
    "sssp_dlt_step",
    "sssp_dlt_step_undirected",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bfs(graph, source=1):
    """Evaluate BFS on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.bfs(pg, source=1)
    """
    return AppAssets(algo="flash_bfs", context="vertex_data")(graph, source)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bfs_push(graph, source=1):
    """Evaluate BFS (push version) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.bfs_push(pg, source=1)
    """
    return AppAssets(algo="flash_bfs_push", context="vertex_data")(graph, source)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bfs_pull(graph, source=1):
    """Evaluate BFS (pull version) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.bfs_pull(pg, source=1)
    """
    return AppAssets(algo="flash_bfs_pull", context="vertex_data")(graph, source)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bfs_undirected(graph, source=1):
    """Evaluate BFS (undirected version) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.bfs_undirected(pg, source=1)
    """
    return AppAssets(algo="flash_bfs_undirected", context="vertex_data")(graph, source)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def random_multi_bfs(graph):
    """Evaluate Random Multi-source BFS on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with its max distance form sources.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.random_multi_bfs(pg)
    """
    return AppAssets(algo="flash_random_multi_bfs", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def sssp(graph, source=1):
    """Evaluate SSSP on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": ["dist"]})
        >>> c = graphscope.flash.sssp(pg, source=1)
    """
    return AppAssets(algo="flash_sssp", context="vertex_data")(graph, source)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def sssp_undirected(graph, source=1):
    """Evaluate SSSP (undirected version) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": ["dist"]})
        >>> c = graphscope.flash.sssp_undirected(pg, source=1)
    """
    return AppAssets(algo="flash_sssp_undirected", context="vertex_data")(graph, source)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def sssp_dlt_step(graph, source=1):
    """Evaluate SSSP (delta-stepping algorithm) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": ["dist"]})
        >>> c = graphscope.flash.sssp_dlt_step(pg, source=1)
    """
    return AppAssets(algo="flash_sssp_dlt_step", context="vertex_data")(graph, source)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def sssp_dlt_step_undirected(graph, source=1):
    """Evaluate SSSP (delta-stepping algorithm, undirected version) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": ["dist"]})
        >>> c = graphscope.flash.sssp_dlt_step_undirected(pg, source=1)
    """
    return AppAssets(algo="flash_sssp_dlt_step_undirected", context="vertex_data")(
        graph, source
    )
