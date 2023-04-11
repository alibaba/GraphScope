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
    "kcore_decomposition",
    "kcore_decomposition_2",
    "kcore_searching",
    "degeneracy_ordering",
    "onion_layer_ordering",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def kcore_decomposition(graph):
    """Evaluate K-core Decomposition on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the core value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.kcore_decomposition(pg)
    """
    return AppAssets(algo="flash_kcore", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def kcore_decomposition_2(graph):
    """Evaluate K-core Decomposition (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the core value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.kcore_decomposition_2(pg)
    """
    return AppAssets(algo="flash_kcore_2", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def kcore_searching(graph, k=5):
    """Evaluate K-core Searchig on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        k (int, optional): k of k-core seraching. Defaults to 5.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the size of k-core.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.kcore_searching(pg, k=5)
        >>> c.to_numpy("r")[0]
    """
    k = int(k)
    return AppAssets(algo="flash_kcore_search", context="tensor")(graph, k)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def degeneracy_ordering(graph):
    """Evaluate Degeneracy Ordering on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the degeneracy ordering rank.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.degeneracy_ordering(pg)
    """
    return AppAssets(algo="flash_degeneracy", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def onion_layer_ordering(graph):
    """Evaluate Onion-layer Ordering on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the onion-layer ordering rank.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.onion_layer_ordering(pg)
    """
    return AppAssets(algo="flash_onion", context="vertex_data")(graph)
