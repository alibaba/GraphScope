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

import os

from graphscope.framework.graph import Graph
from graphscope.framework.loader import Loader


def load_modern_graph(sess, prefix, directed=True):
    """Load modern graph.
    Modern graph consist 6 vertices and 6 edges, useful to test the basic
    functionalities.

    Args:
        sess (:class:`graphscope.Session`): Load graph within the session.
        prefix (str): Data directory.
        directed (bool, optional): Determine to load a directed or undirected graph.
            Defaults to True.

    Returns:
        :class:`graphscope.Graph`: A Graph object which graph type is ArrowProperty
    """
    graph = sess.g(directed=directed)
    graph = (
        graph.add_vertices(
            Loader(os.path.join(prefix, "person.csv"), delimiter="|"),
            "person",
            ["name", ("age", "int")],
            "id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "software.csv"), delimiter="|"),
            "software",
            ["name", "lang"],
            "id",
        )
        .add_edges(
            Loader(os.path.join(prefix, "knows.csv"), delimiter="|"),
            "knows",
            ["weight"],
            src_label="person",
            dst_label="person",
            src_field="src_id",
            dst_field="dst_id",
        )
        .add_edges(
            Loader(os.path.join(prefix, "created.csv"), delimiter="|"),
            "created",
            ["weight"],
            src_label="person",
            dst_label="software",
            src_field="src_id",
            dst_field="dst_id",
        )
    )
    return graph
