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


def load_cora_graph(sess=None, prefix=None, directed=False):
    """Load cora datasets.

    The Cora dataset consists of 2708 scientific publications classified into one of seven classes.
    The citation network consists of 5429 links. Each publication in the dataset is described by a
    0/1-valued word vector indicating the absence/presence of the corresponding word from the dictionary.
    See more details here:

        https://linqs.soe.ucsc.edu/data

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
        >>> from graphscope.dataset import load_cora_graph
        >>> sess = graphscope.session(mode="lazy")
        >>> g = load_cora_graph(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_cora_graph
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_cora_graph(sess, "/path/to/dataset")
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "cora.tar.gz"
        origin = f"{DATA_SITE}/cora.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="2dae0c5ec6eca4321fc94614381d6c74a216726b930e4de228bc15fa1ab504e8",
        )
        # assumed dirname is ppi after extracting from ppi.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g(directed=directed)
    graph = graph.add_vertices(os.path.join(prefix, "node.csv"), "paper").add_edges(
        os.path.join(prefix, "edge.csv"),
        "cites",
        src_label="paper",
        dst_label="paper",
    )

    return graph
