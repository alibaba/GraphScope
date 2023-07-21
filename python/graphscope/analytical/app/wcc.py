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

import logging

from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.app import project_to_simple

__all__ = ["wcc", "wcc_opt", "wcc_auto", "wcc_projected"]

logger = logging.getLogger("graphscope")


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property", "directed")
def wcc(graph):
    """Evaluate weakly connected components on the `graph`.
    This is an optimized version of WCC.
    Note this cannot be compiled against a property graph that has multiple labels.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with the component ID, evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.wcc(pg)
        >>> sess.close()
    """
    cmake_extra_options = None
    if graph.oid_type == "std::string":
        logger.warning(
            "WCC algorithm will output int value as component ID on graphs that has 'string' type as ID"
        )
        cmake_extra_options = "-DWCC_USE_GID=ON"
    return AppAssets(
        algo="wcc", context="vertex_data", cmake_extra_options=cmake_extra_options
    )(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property", "directed")
def wcc_opt(graph):
    """Evaluate weakly connected components on the `graph`.
    This is an optimized version of WCC.
    Note this cannot be compiled against a property graph that has multiple labels.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with the component ID, evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.wcc_opt(pg)
        >>> sess.close()
    """
    cmake_extra_options = None
    if graph.oid_type == "std::string":
        raise ValueError(
            "The `wcc_opt()` algorithm cannot work on graphs that has 'string' type as ID, "
            "use `wcc()` instead"
        )
    return AppAssets(
        algo="wcc_opt", context="vertex_data", cmake_extra_options=cmake_extra_options
    )(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property", "directed")
def wcc_auto(graph):
    """Evaluate weakly connected components on the `graph`.
    This is an auto parallel version of WCC.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:
            A context with each vertex assigned with the component ID, evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>> g = load_p2p_network(sess)
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>> c = graphscope.wcc_auto(pg)
        >>> sess.close()
    """
    return AppAssets(algo="wcc_auto", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property", "directed")
def wcc_projected(graph):
    """Evaluate weakly connected components on the `graph`.
    This is a naive version of WCC that could work on dynamic, projected, flatten graph
    """
    return AppAssets(algo="wcc_projected", context="vertex_data")(graph)
