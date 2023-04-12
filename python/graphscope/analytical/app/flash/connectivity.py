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
    "cc",
    "cc_push",
    "cc_pull",
    "cc_opt",
    "cc_log",
    "cc_block",
    "cc_union",
    "bcc",
    "bcc_2",
    "scc",
    "scc_2",
    "bridge",
    "bridge_2",
    "cut_point",
    "cut_point_2",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cc(graph):
    """Evaluate Connected Components on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cc(pg)
    """
    return AppAssets(algo="flash_cc", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cc_push(graph):
    """Evaluate Connected Components (push version) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cc_push(pg)
    """
    return AppAssets(algo="flash_cc_push", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cc_pull(graph):
    """Evaluate Connected Components (pull version) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cc_pull(pg)
    """
    return AppAssets(algo="flash_cc_pull", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cc_opt(graph):
    """Evaluate Connected Components (optimized version) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cc_opt(pg)
    """
    return AppAssets(algo="flash_cc_opt", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cc_log(graph):
    """Evaluate Connected Components (the cc-log algorithm) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cc_log(pg)
    """
    return AppAssets(algo="flash_cc_log", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cc_block(graph):
    """Evaluate Connected Components (the block-centric algorithm) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cc_block(pg)
    """
    return AppAssets(algo="flash_cc_block", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cc_union(graph):
    """Evaluate Connected Components (the union-based algorithm) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cc_union(pg)
    """
    return AppAssets(algo="flash_cc_union", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bcc(graph):
    """Evaluate Biconnected Components on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.bcc(pg)
    """
    return AppAssets(algo="flash_bcc", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bcc_2(graph):
    """Evaluate Biconnected Components (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.bcc_2(pg)
    """
    return AppAssets(algo="flash_bcc_2", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def scc(graph):
    """Evaluate Strongly Connected Components on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.scc(pg)
    """
    return AppAssets(algo="flash_scc", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def scc_2(graph):
    """Evaluate Strongly Connected Components (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the label of its component.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.scc_2(pg)
    """
    return AppAssets(algo="flash_scc_2", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bridge(graph):
    """Evaluate Bridge Detection on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
           A context with the number of bridges.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.bridge(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_bridge", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bridge_2(graph):
    """Evaluate Bridge Detection (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
           A context with the number of bridges.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.bridge_2(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_bridge_2", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cut_point(graph):
    """Evaluate Cut Point Detection on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
           A context with the number of cut-points.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cut_point(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_cut_point", context="vertex_data")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cut_point_2(graph):
    """Evaluate Cut Point Detection (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
           A context with the number of cut-points.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cut_point_2(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_cut_point_2", context="vertex_data")(graph)
