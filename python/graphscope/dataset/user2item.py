#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.
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


def load_u2i_graph(sess=None, prefix=None, directed=True):
    """Load user2item datasets.

    The user-2-item datasets consists of 5241 nodes, which represents both user and item node,
    42876 edges represents with buying relationship. And this dataset is owned by graphlearn, you
    can downloads from here:

        https://github.com/alibaba/graph-learn/blob/master/examples/data/u2i.py

    Args:
        sess (:class:`graphscope.Session`): Load graph within the session.
            Default session will be used when setting to None. Defaults to None.
        prefix: `PathLike` object that represents a path.
            With standalone mode, set prefix None will try to download from
            source URL. Defaults to None.
        directed (bool, optional): Determine to load a directed or undirected graph.
            Defaults to True.

    Returns:
        :class:`graphscope.framework.graph.GraphDAGNode`:
            A Graph node which graph type is ArrowProperty, evaluated in eager mode.

    Examples:
        .. code:: python

        >>> # lazy mode
        >>> import graphscope
        >>> from graphscope.dataset import load_u2i_graph
        >>> sess = graphscope.session(mode="lazy")
        >>> g = load_u2i_graph(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_u2i_graph
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_u2i_graph(sess, "/path/to/dataset")

    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "u2i.tar.gz"
        origin = f"{DATA_SITE}/u2i.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="b5475a0f6f13b0964ba0c38804d06003a44627653df3371d938e47fb9eedced6",
        )
        # assumed dirname is u2i after extracting from u2i.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g(directed=directed)
    graph = (
        graph.add_vertices(
            Loader(os.path.join(prefix, "node.csv"), delimiter="\t"),
            label="u",
            properties=[("feature", "str")],
            vid_field="id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "node.csv"), delimiter="\t"),
            label="i",
            properties=[("feature", "str")],
            vid_field="id",
        )
        .add_edges(
            Loader(os.path.join(prefix, "edge.csv"), delimiter="\t"),
            label="u-i",
            properties=["weight"],
            src_label="u",
            dst_label="i",
            src_field="src_id",
            dst_field="dst_id",
        )
        .add_edges(
            Loader(os.path.join(prefix, "edge.csv"), delimiter="\t"),
            label="u-i_reverse",
            properties=["weight"],
            src_label="i",
            dst_label="u",
            src_field="dst_id",
            dst_field="src_id",
        )
    )
    return graph
