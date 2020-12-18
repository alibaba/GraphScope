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

from graphscope.client.session import get_session_by_id
from graphscope.framework import dag_utils


def empty_graph_in_engine(graph, directed):
    """create empty graph in grape_engine with the graph metadata.

    Parameters:
    -----------
    graph: the graph instance in python.
    graph_type: the graph type of graph (IMMUTABLE, ARROW, DYNAMIC).
    nx_graph_type: the networkx graph type of graph (Graph, DiGraph, MultiGraph, MultiDiGraph).

    """
    sess = get_session_by_id(graph._session_id)
    op = dag_utils.create_graph(
        sess.session_id,
        graph_type=graph._graph_type,
        directed=directed,
        efile="",
        vfile="",
    )
    graph_def = sess.run(op)
    return graph_def


def parse_ret_as_dict(func):
    def wrapper(*args, **kwargs):
        r = json.loads(func(*args, **kwargs))
        if not isinstance(r, list):
            return r
        ret = dict()
        graph = args[0]
        if graph.is_multigraph():
            for i in range(len(r[0])):
                key_attr = dict()
                for e in r[1][i]:
                    key_attr[e[0]] = e[1]
                ret[r[0][i]] = key_attr
        else:
            for i in range(len(r[0])):
                ret[r[0][i]] = r[1][i]
        return ret

    return wrapper
