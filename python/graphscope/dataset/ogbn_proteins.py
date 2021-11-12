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


def load_ogbn_proteins(sess=None, prefix=None):
    """Load ogbn_proteins graph.
     The ogbn-proteins dataset is an undirected, weighted, and typed (according to species) graph. Nodes represent
     proteins, and edges indicate different types of biologically meaningful associations between proteins, e.g.,
     physical interactions, co-expression or homology [1,2]. All edges come with 8-dimensional features, where each
     dimension represents the approximate confidence of a single association type and takes values between 0 and 1 (the
     larger the value is, the more confident we are about the association). The proteins come from 8 species.
     See more details here:

        https://ogb.stanford.edu/docs/nodeprop/#ogbn-proteins

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
        >>> g = load_ogbn_proteins(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_ogbn_proteins
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_ogbn_proteins(sess, "/path/to/dataset")
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "ogbn_proteins.tar.gz"
        origin = f"{DATA_SITE}/ogbn_proteins.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="ea427e520bf068f3d6788d940b3bdc6773b965d792f2fa4a52311eab478acbde",
        )
        # assumed dirname is ogbn_proteins after extracting from ogbn_proteins.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g()
    graph = graph.add_vertices(os.path.join(prefix, "nodes.csv"), "proteins").add_edges(
        os.path.join(prefix, "edge.csv"), "associations"
    )

    return graph
