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


def load_ppi(sess=None, prefix=None, directed=False):
    """Load protein-protein links datasets.

    In protein-protein links graph, every node represents a protein,and edges represent
    the links between them. See more details here:

        https://humgenomics.biomedcentral.com/articles/10.1186/1479-7364-3-3-291

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
        >>> from graphscope.dataset import load_ppi
        >>> sess = graphscope.session(mode="lazy")
        >>> g = load_ppi(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_ppi
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_ppi(sess, "/path/to/dataset")
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "ppi.tar.gz"
        origin = f"{DATA_SITE}/ppi.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="2ffe7207626f5b177cb05871b65ee7c95fc9ebc45cc9f628d36efef8b5c0b642",
        )
        # assumed dirname is ppi after extracting from ppi.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g(directed=directed)
    graph = graph.add_vertices(os.path.join(prefix, "node.csv"), "protein").add_edges(
        os.path.join(prefix, "edge.csv"),
        "link",
        src_label="protein",
        dst_label="protein",
    )

    return graph
