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


def load_ogbn_arxiv(sess=None, prefix=None):
    """Load ogbn_arxiv graph.
     The ogbn-arxiv dataset is a directed graph, representing the citation network
     between all Computer Science (CS) arXiv papers indexed by Microsoft Academic Graph (MAG).
     See more details here:

        https://ogb.stanford.edu/docs/nodeprop/#ogbn-arxiv

    Args:
        sess (:class:`graphscope.Session`): Load graph within the session.
            Default session will be used when setting to None. Defaults to None.
        prefix: `PathLike` object that represents a path.
            With standalone mode, set prefix None will try to download from
            source URL. Defaults to None.

    Returns:
        :class:`graphscope.framework.graph.GraphDAGNode`:
            A Graph node which graph type is ArrowProperty, evaluated in eager mode.

    Examples:
        .. code:: python

        >>> # lazy mode
        >>> import graphscope
        >>> from graphscope.dataset import load_ogbn_arsiv
        >>> sess = graphscope.session(mode="lazy")
        >>> g = load_ogbn_arxiv(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_ogbn_arxiv
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_ogbn_arxiv(sess, "/path/to/dataset")
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "ogbn_arxiv.tar.gz"
        origin = f"{DATA_SITE}/ogbn_arxiv.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="d920922681e8369da5dc8e0f28fffae2eb0db056dc626097f4159351d4ea4389",
        )
        # assumed dirname is ogbn_arxiv after extracting from ogbn_arxiv.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g()
    graph = graph.add_vertices(os.path.join(prefix, "nodes.csv"), "paper").add_edges(
        os.path.join(prefix, "edge.csv"), "citation"
    )

    return graph
