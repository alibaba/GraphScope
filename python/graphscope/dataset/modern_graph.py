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
    graph = sess.load_from(
        edges={
            "knows": (
                Loader(
                    os.path.join(prefix, "knows.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["weight"],
                ("src_id", "person"),
                ("dst_id", "person"),
            ),
            "created": (
                Loader(
                    os.path.join(prefix, "created.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["weight"],
                ("src_id", "person"),
                ("dst_id", "software"),
            ),
        },
        vertices={
            "person": (
                Loader(
                    os.path.join(prefix, "person.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["name", ("age", "int")],
                "id",
            ),
            "software": (
                Loader(
                    os.path.join(prefix, "software.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["name", "lang"],
                "id",
            ),
        },
        directed=directed,
        generate_eid=True,
    )
    return graph
