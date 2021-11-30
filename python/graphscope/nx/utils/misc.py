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


import json

import networkx.utils.misc
from networkx.exception import NetworkXError

from graphscope.client.session import get_session_by_id
from graphscope.framework import dag_utils
from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.utils.misc)


def empty_graph_in_engine(graph, directed, distributed):
    """create empty graph in grape_engine with the graph metadata.

    Parameters:
    -----------
    graph: the graph instance in python.
    graph_type: the graph type of graph (IMMUTABLE, ARROW, DYNAMIC).
    nx_graph_type: the networkx graph type of graph (Graph, DiGraph, MultiGraph, MultiDiGraph).

    """
    sess = get_session_by_id(graph.session_id)
    op = dag_utils.create_graph(
        sess.session_id,
        graph_type=graph._graph_type,
        directed=directed,
        distributed=distributed,
        efile="",
        vfile="",
    )
    graph_def = op.eval()
    return graph_def


def parse_ret_as_dict(func):
    def wrapper(*args, **kwargs):
        r = json.loads(func(*args, **kwargs))
        if not isinstance(r, list):
            return r
        ret = dict()
        for i in range(len(r[0])):
            key = tuple(r[0][i]) if isinstance(r[0][i], list) else r[0][i]
            ret[key] = r[1][i]
        return ret

    return wrapper


def check_node_is_legal(n):
    """check the node is legal or not.

    Parameters:
    -----------
    n: node

    Raises
    ------
    NetworkXError
        If the type of node is illegal.
    """

    def check_node_type(n):
        if not isinstance(n, (int, float, str, bool, type(None))):
            raise NetworkXError(
                "Node %s is illegal. Type of node must be one of [int, float, str, bool, NoneType], but got %s"
                % (n, type(n))
            )

    if isinstance(n, tuple):
        if len(n) != 2 or not isinstance(n[0], str):
            raise NetworkXError(
                "Labeled node %s must be a 2-tuple and label must be str" % (n,)
            )
        else:
            check_node_type(n[1])
    else:
        check_node_type(n)
