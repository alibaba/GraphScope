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


def load_p2p_network(sess=None, prefix=None, directed=False, generate_eid=True):
    """Load p2p graph.
    A peer-to-peer dataset derived from Gnutella peer-to-peer network, August 31 2002,
    with generated data on vertices and edges. See more details here:

        http://snap.stanford.edu/data/p2p-Gnutella31.html

    Args:
        sess (:class:`graphscope.Session`): Load graph within the session.
            Default session will be used when setting to None. Defaults to None.
        prefix: `PathLike` object that represents a path.
            With standalone mode, set prefix None will try to download from
            source URL. Defaults to None.
        directed (bool, optional): Determine to load a directed or undirected graph.
            Defaults to True.
        generate_eid (bool, optional): True will generate id for each edge as the first property.
            Defaults to True.

    Returns:
        :class:`graphscope.framework.graph.GraphDAGNode`:
            A Graph node which graph type is ArrowProperty, evaluated in eager mode.

    Examples:
        .. code:: python

        >>> # lazy mode
        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(mode="lazy")
        >>> g = load_p2p_network(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_p2p_network
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_p2p_network(sess, "/path/to/dataset")
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "p2p_network.tar.gz"
        origin = f"{DATA_SITE}/p2p_network.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="117131735186caff23ea127beec61b5396662c0815fc7918186451fe957e8c2f",
        )
        # assumed dirname is p2p_network after extracting from p2p_network.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g(directed=directed, generate_eid=generate_eid)
    graph = graph.add_vertices(
        os.path.join(prefix, "p2p-31_property_v_0"), "host"
    ).add_edges(
        os.path.join(prefix, "p2p-31_property_e_0"),
        "connect",
        src_label="host",
        dst_label="host",
    )

    return graph
