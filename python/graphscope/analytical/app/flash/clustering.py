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
    "clustering_coefficient",
    "fluid_community",
    "fluid_community_2",
    "graph_coloring",
    "label_propagation",
    "label_propagation_2",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def clustering_coefficient(graph):
    """Evaluate Clustering Coefficient on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the clustering coefficient value.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.clustering_coefficient(pg)
    """
    return AppAssets(algo="flash_clustering_coeff", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def fluid_community(graph):
    """Evaluate Fluid Community on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its community.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.fluid_community(pg)
    """
    return AppAssets(algo="flash_fluid_community", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def fluid_community_2(graph):
    """Evaluate Fluid Community (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its community.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.fluid_community_2(pg)
    """
    return AppAssets(algo="flash_fluid_by_color", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def graph_coloring(graph):
    """Evaluate Graph Coloring on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the color.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.graph_coloring(pg)
    """
    return AppAssets(algo="flash_color", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def label_propagation(graph):
    """Evaluate Label Propagation on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with its label.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.label_propagation(pg)
    """
    return AppAssets(algo="flash_lpa", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def label_propagation_2(graph):
    """Evaluate Label Propagation (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with its label.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.label_propagation_2(pg)
    """
    return AppAssets(algo="flash_lpa_by_color", context="vertex_data")(graph)
