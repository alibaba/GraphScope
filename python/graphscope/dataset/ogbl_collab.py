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


def load_ogbl_collab(sess=None, prefix=None):
    """Load ogbl_collab graph.
     The ogbl-collab dataset is an undirected graph, representing a subset of the collaboration network between authors
     indexed by MAG. Each node represents an author and edges indicate the collaboration between authors. All nodes
     come with 128-dimensional features, obtained by averaging the word embeddings of papers that are published by the
     authors. All edges are associated with two meta-information: the year and the edge weight, representing the number
     of co-authored papers published in that year. The graph can be viewed as a dynamic multi-graph since there can be
     multiple edges between two nodes if they collaborate in more than one year.
     See more details here:

        https://ogb.stanford.edu/docs/linkprop/#ogbl-collab

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
        >>> g = load_ogbl_collab(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_ogbl_collab
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_ogbl_collab(sess, "/path/to/dataset")
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "ogbl_collab.tar.gz"
        origin = f"{DATA_SITE}/ogbl_collab.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="abb49a2f7c6c16ed355ea83ec7ce65ece1278eec40e6fef6ee9918b4383ae459",
        )
        # assumed dirname is ogbl_collab after extracting from ogbl_collab.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g()
    graph = graph.add_vertices(os.path.join(prefix, "nodes.csv"), "author").add_edges(
        os.path.join(prefix, "edge.csv"), "collaboration"
    )

    return graph
