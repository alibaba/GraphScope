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

from graphscope.client.session import get_default_session
from graphscope.dataset.io_utils import DATA_SITE
from graphscope.dataset.io_utils import download_file
from graphscope.framework.loader import Loader


def load_modern_graph(sess=None, prefix=None, directed=True):
    """Load modern graph.
    Modern graph consist 6 vertices and 6 edges, useful to test the basic
    functionalities.

    Args:
        sess (:class:`graphscope.Session`): Load graph within the session.
            Default session will be used when setting to None. Defaults to None.
        prefix (str): `PathLike` object that represents a path.
            With standalone mode, set prefix to None will try to download from
            source URL. Defaults to None.
        directed (bool, optional): Determine to load a directed or undirected graph.
            Defaults to True.

    Returns:
        :class:`graphscope.framework.graph.GraphDAGNode`:
            A Graph node which graph type is ArrowProperty, evaluated in eager mode.

        >>> # lazy mode
        >>> import graphscope
        >>> from graphscope.dataset. modern_graph import load_modern_graph
        >>> sess = graphscope.session(mode="lazy")
        >>> g = load_modern_graph(sess, "/path/to/dataset", True)
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset. modern_graph import load_modern_graph
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_modern_graph(sess, "/path/to/dataset", True)
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "modern_graph.tar.gz"
        origin = f"{DATA_SITE}/modern_graph.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="a67c02191ea9dfa618a83d94087349a25937b92973f42206a28fdf6fa5299dec",
        )
        # assumed dirname is modern_graph after extracting from modern_graph.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

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
