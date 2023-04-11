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
    "minimum_spanning_forest",
    "minimum_spanning_forest_2",
    "diameter_approximation",
    "diameter_approximation_2",
    "k_center",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def minimum_spanning_forest(graph):
    """Evaluate Minimum Spanning Forest on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the value of minimum spanning forest.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": ["dist"]})
        >>> c = graphscope.flash.minimum_spanning_forest(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_msf", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def minimum_spanning_forest_2(graph):
    """Evaluate Minimum Spanning Forest (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the value of minimum spanning forest.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": ["dist"]})
        >>> c = graphscope.flash.minimum_spanning_forest_2(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_msf_block", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def diameter_approximation(graph):
    """Evaluate Diameter Approximation on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the value of diameter approximation.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.diameter_approximation(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_diameter_approx", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def diameter_approximation_2(graph):
    """Evaluate Diameter Approximation (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the value of diameter approximation.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.diameter_approximation_2(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_diameter_approx_2", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def k_center(graph, k=5):
    """Evaluate K-center on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        k (int, optional): k of k-center searching. Defaults to 5.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the k-center distance value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.k_center(pg, k=5)
        >>> c.to_numpy("r")[0]
    """
    k = int(k)
    return AppAssets(algo="flash_k_center", context="vertex_data")(graph, k)
