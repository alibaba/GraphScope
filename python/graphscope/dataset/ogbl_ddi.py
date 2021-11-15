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


def load_ogbl_ddi(sess=None, prefix=None):
    """Load ogbl_ddi graph.
     The ogbl-ddi dataset is a homogeneous, unweighted, undirected graph, representing the drug-drug interaction
     network [1]. Each node represents an FDA-approved or experimental drug. Edges represent interactions between drugs
     and can be interpreted as a phenomenon where the joint effect of taking the two drugs together is considerably
     different from the expected effect in which drugs act independently of each other.
     See more details here:

        https://ogb.stanford.edu/docs/linkprop/#ogbl-ddi

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
        >>> g = load_ogbl_ddi(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_ogbl_ddi
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_ogbl_ddi(sess, "/path/to/dataset")
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "ogbl_ddi.tar.gz"
        origin = f"{DATA_SITE}/ogbl_ddi.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="2a66bf265a217fd6148ba1f0ed9c9a297e778bf539b2b7262edf4a0dc1f4c8b9",
        )
        # assumed dirname is ogbl_ddi after extracting from ogbl_ddi.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g()
    graph = graph.add_vertices(os.path.join(prefix, "nodes.csv"), "drug").add_edges(
        os.path.join(prefix, "edge.csv"), "effect"
    )

    return graph
