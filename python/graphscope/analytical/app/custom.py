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

__all__ = ["custom_analytical_algorithm"]

logger = logging.getLogger("graphscope")


@project_to_simple
def custom_analytical_algorithm(
    graph, algorithm, *args, context="vertex_data", cmake_extra_options=None, **kwargs
):
    """A special application DAG node to running arbitrary supported algorithms.

    Note that this is only for debugging/profiling usage for developers and
    the application should be defined in .gs_conf.yaml in coordinator.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        algorithm (:code:`str`): A predefined algorithm name,
            e.g., `sssp`, `wcc`, `lcc`, etc.
        context (:code:`str`): The context type of the algorithm,
            e.g., `vertex_data`, etc. Defaults to `vertex_data`.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

    Returns:
        :class:`graphscope.framework.context.ContextDAGNode`:
            A context, evaluated in eager mode.

    Examples:

    .. code:: python

        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(cluster_type="hosts", mode="eager")
        >>>
        >>> g = load_p2p_network(sess)
        >>>
        >>> # project to a simple graph (if needed)
        >>> pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
        >>>
        >>> c = graphscope.custom_analytical_algorithm(pg, 'wcc')
        >>>
        >>> c = graphscope.custom_analytical_algorithm(pg, 'sssp', 6)
        >>>
        >>> sess.close()
    """
    return AppAssets(
        algo=algorithm, context=context, cmake_extra_options=cmake_extra_options
    )(graph, *args, **kwargs)
