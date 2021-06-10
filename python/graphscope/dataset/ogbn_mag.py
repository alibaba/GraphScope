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

from graphscope.framework.graph import Graph


def load_ogbn_mag(sess, prefix):
    """Load ogbn_mag graph.
    The ogbn-mag dataset is a heterogeneous network composed of a subset of the Microsoft Academic Graph (MAG).
    See more details here:
    https://ogb.stanford.edu/docs/nodeprop/#ogbn-mag

    Args:
        sess (:class:`graphscope.Session`): Load graph within the session.
        prefix (str): Data directory.
        directed (bool, optional): Determine to load a directed or undirected graph.
            Defaults to True.

    Returns:
        :class:`graphscope.Graph`: A Graph object which graph type is ArrowProperty
    """
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
