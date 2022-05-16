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


import functools

import networkx.utils.misc

from graphscope.framework import dag_utils
from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.utils.misc)


def init_empty_graph_in_engine(graph, directed, distributed=True):
    """initialize an empty graph in grape_engine with the graph metadata.

    Parameters:
    -----------
    graph: the graph instance in python.
    graph_type: the graph type of graph (IMMUTABLE, ARROW, DYNAMIC).

    """
    op = dag_utils.create_graph(
        graph.session.session_id,
        graph_type=graph._graph_type,
        directed=directed,
        distributed=distributed,
        efile="",
        vfile="",
    )
    graph._op = op
    graph_def = op.eval(leaf=False)
    return graph_def


def clear_mutation_cache(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        g = args[0]
        if func.__name__ in (
            "add_node",
            "add_edge",
            "add_nodes_from",
            "add_edges_from",
            "add_weighted_edges_from",
        ):
            g._clear_removing_cache()
        elif func.__name__ in (
            "remove_node",
            "remove_edge",
            "remove_nodes_from",
            "remove_edges_from",
        ):
            g._clear_adding_cache()
        else:
            if hasattr(g, "_graph"):
                g._graph._clear_removing_cache()
                g._graph._clear_adding_cache()
            else:
                g._clear_removing_cache()
                g._clear_adding_cache()
        return func(*args, **kwargs)

    return wrapper


def replace_with_inf(data):
    for k, v in data.items():
        if v == 1.7976931348623157e308:
            data[k] = float("inf")
    return data
