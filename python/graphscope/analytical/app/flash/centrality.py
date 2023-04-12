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
    "betweenness_centrality",
    "closeness_centrality",
    "eigenvector_centrality",
    "harmonic_centrality",
    "katz_centrality",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def betweenness_centrality(graph, source=1):
    """Evaluate Betweenness Centrality on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the betweenness centrality value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.betweenness_centrality(pg, source=1)
    """
    return AppAssets(algo="flash_bc", context="vertex_data")(graph, source)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def closeness_centrality(graph):
    """Evaluate Closeness Centrality on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the closeness centrality value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.closeness_centrality(pg)
    """
    return AppAssets(algo="flash_closeness", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def eigenvector_centrality(graph):
    """Evaluate Eigenvector Centrality on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the eigenvector centrality value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.eigenvector_centrality(pg)
    """
    return AppAssets(algo="flash_eigenvec", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def harmonic_centrality(graph):
    """Evaluate Harmonic Centrality on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the harmonic centrality value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.harmonic_centrality(pg)
    """
    return AppAssets(algo="flash_harmonic", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def katz_centrality(graph):
    """Evaluate Katz Centrality on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the katz centrality value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.katz_centrality(pg)
    """
    return AppAssets(algo="flash_katz", context="vertex_data")(graph)
