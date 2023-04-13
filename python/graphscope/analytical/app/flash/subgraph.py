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
    "three_path_counting",
    "triangle_counting",
    "tailed_triangle_counting",
    "acyclic_triangle_counting",
    "cyclic_triangle_counting",
    "cycle_plus_triangle_counting",
    "in_plus_triangle_counting",
    "out_plus_triangle_counting",
    "rectangle_counting",
    "diamond_counting",
    "k_clique_counting",
    "k_clique_counting_2",
    "densest_subgraph_2_approximation",
]


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def three_path_counting(graph):
    """Evaluate 3-path Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of 3-paths.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.three_path_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_3_path", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def triangle_counting(graph):
    """Evaluate Triangle Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of triangles.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.triangle_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_triangle", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def tailed_triangle_counting(graph):
    """Evaluate Tailed-triangle Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of tailed-triangles.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.tailed_triangle_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_tailed_triangle", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def acyclic_triangle_counting(graph):
    """Evaluate Acyclic Triangle Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of acyclic triangles.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.acyclic_triangle_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_acyclic_triangle", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cyclic_triangle_counting(graph):
    """Evaluate Cyclic Triangle Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of cyclic triangles.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cyclic_triangle_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_cyclic_triangle", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def cycle_plus_triangle_counting(graph):
    """Evaluate Cycle+ Triangle Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of cycle+ triangles.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.cycle_plus_triangle_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_cycle_plus_triangle", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def in_plus_triangle_counting(graph):
    """Evaluate In+ Triangle Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of in+ triangles.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.in_plus_triangle_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_in_triangle", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def out_plus_triangle_counting(graph):
    """Evaluate Out+ Triangle Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of out+ triangles.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.out_plus_triangle_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_out_triangle", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def rectangle_counting(graph):
    """Evaluate Rectangle Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of rectangles.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.rectangle_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_rectangle", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def diamond_counting(graph):
    """Evaluate Diamond Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of diamonds.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.diamond_counting(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_diamond", context="tensor")(graph)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def k_clique_counting(graph, k=5):
    """Evaluate K-clique Counting on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        k (int, optional): k of k-clique seraching. Defaults to 5.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of k-cliques.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.k_clique_counting(pg, k=5)
        >>> c.to_numpy("r")[0]
    """
    k = int(k)
    return AppAssets(algo="flash_k_clique", context="tensor")(graph, k)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def k_clique_counting_2(graph, k=5):
    """Evaluate K-clique Counting (version 2) on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        k (int, optional): k of k-clique seraching. Defaults to 5.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the number of k-cliques.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.k_clique_counting_2(pg, k=5)
        >>> c.to_numpy("r")[0]
    """
    k = int(k)
    return AppAssets(algo="flash_k_clique_2", context="tensor")(graph, k)


@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def densest_subgraph_2_approximation(graph, d=10):
    """Evaluate 2-approximation for the densest subgraph problem on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with the density.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> g = load_p2p_network()
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": []}, edges={"connect": []})
        >>> c = graphscope.flash.densest_subgraph_2_approximation(pg)
        >>> c.to_numpy("r")[0]
    """
    return AppAssets(algo="flash_densest_sub_2_approx", context="tensor")(graph)
