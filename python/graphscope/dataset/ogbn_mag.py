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


def load_ogbn_mag(sess=None, prefix=None):
    """Load ogbn_mag graph.
    The ogbn-mag dataset is a heterogeneous network composed of a subset
    of the Microsoft Academic Graph (MAG). See more details here:

        https://ogb.stanford.edu/docs/nodeprop/#ogbn-mag

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
        >>> from graphscope.dataset import load_ogbn_mag
        >>> sess = graphscope.session(mode="lazy")
        >>> g = load_ogbn_mag(sess, "/path/to/dataset")
        >>> g1 = sess.run(g)

        >>> # eager mode
        >>> import graphscope
        >>> from graphscope.dataset import load_ogbn_mag
        >>> sess = graphscope.session(mode="eager")
        >>> g = load_ogbn_mag(sess, "/path/to/dataset")
    """
    if prefix is not None:
        prefix = os.path.expandvars(prefix)
    else:
        fname = "ogbn_mag_small.tar.gz"
        origin = f"{DATA_SITE}/ogbn_mag_small.tar.gz"
        fpath = download_file(
            fname,
            origin=origin,
            extract=True,
            file_hash="ccd128ab673e5d7dd1cceeaa4ba5d65b67a18212c4a27b0cd090359bd7042b10",
        )
        # assumed dirname is ogbn_mag_small after extracting from ogbn_mag_small.tar.gz
        prefix = fpath[0:-7]

    if sess is None:
        sess = get_default_session()

    graph = sess.g()
    graph = (
        graph.add_vertices(os.path.join(prefix, "paper.csv"), "paper")
        .add_vertices(os.path.join(prefix, "author.csv"), "author")
        .add_vertices(os.path.join(prefix, "institution.csv"), "institution")
        .add_vertices(os.path.join(prefix, "field_of_study.csv"), "field_of_study")
        .add_edges(
            os.path.join(prefix, "author_affiliated_with_institution.csv"),
            "affiliated",
            src_label="author",
            dst_label="institution",
        )
        .add_edges(
            os.path.join(prefix, "paper_has_topic_field_of_study.csv"),
            "hasTopic",
            src_label="paper",
            dst_label="field_of_study",
        )
        .add_edges(
            os.path.join(prefix, "paper_cites_paper.csv"),
            "cites",
            src_label="paper",
            dst_label="paper",
        )
        .add_edges(
            os.path.join(prefix, "author_writes_paper.csv"),
            "writes",
            src_label="author",
            dst_label="paper",
        )
    )

    return graph
