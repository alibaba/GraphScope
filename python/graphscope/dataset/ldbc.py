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
from graphscope.framework.loader import Loader


def load_ldbc(sess, prefix, directed=True):
    """Load ldbc dataset as a ArrowProperty Graph.

    Args:
        sess (:class:`graphscope.Session`): Load graph within the session.
        prefix (str): Data directory.
        directed (bool, optional): Determine to load a directed or undirected graph.
            Defaults to True.

    Returns:
        :class:`graphscope.Graph`: A Graph object which graph type is ArrowProperty
    """
    graph = sess.g(directed=directed)
    graph = (
        graph.add_vertices(
            Loader(os.path.join(prefix, "comment_0_0.csv"), delimiter="|"),
            "comment",
            ["creationDate", "locationIP", "browserUsed", "content", "length"],
            "id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "organisation_0_0.csv"), delimiter="|"),
            "organisation",
            ["type", "name", "url"],
            "id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "tagclass_0_0.csv"), delimiter="|"),
            "tagclass",
            ["name", "url"],
            "id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "person_0_0.csv"), delimiter="|"),
            "person",
            [
                "firstName",
                "lastName",
                "gender",
                "birthday",
                "creationDate",
                "locationIP",
                "browserUsed",
            ],
            "id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "forum_0_0.csv"), delimiter="|"),
            "forum",
            ["title", "creationDate"],
            "id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "place_0_0.csv"), delimiter="|"),
            "place",
            ["name", "url", "type"],
            "id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "post_0_0.csv"), delimiter="|"),
            "post",
            [
                "imageFile",
                "creationDate",
                "locationIP",
                "browserUsed",
                "language",
                "content",
                "length",
            ],
            "id",
        )
        .add_vertices(
            Loader(os.path.join(prefix, "tag_0_0.csv"), delimiter="|"),
            "tag",
            ["name", "url"],
            "id",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), delimiter="|"
            ),
            "replyOf",
            src_label="comment",
            dst_label="comment",
        )
        .add_edges(
            Loader(os.path.join(prefix, "comment_replyOf_post_0_0.csv"), delimiter="|"),
            "replyOf",
            src_label="comment",
            dst_label="post",
        )
        .add_edges(
            Loader(os.path.join(prefix, "place_isPartOf_place_0_0.csv"), delimiter="|"),
            "isPartOf",
            src_label="place",
            dst_label="place",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "tagclass_isSubclassOf_tagclass_0_0.csv"),
                delimiter="|",
            ),
            "isSubclassOf",
            src_label="tagclass",
            dst_label="tagclass",
        )
        .add_edges(
            Loader(os.path.join(prefix, "forum_hasTag_tag_0_0.csv"), delimiter="|"),
            "hasTag",
            src_label="forum",
            dst_label="tag",
        )
        .add_edges(
            Loader(os.path.join(prefix, "comment_hasTag_tag_0_0.csv"), delimiter="|"),
            "hasTag",
            src_label="comment",
            dst_label="tag",
        )
        .add_edges(
            Loader(os.path.join(prefix, "post_hasTag_tag_0_0.csv"), delimiter="|"),
            "hasTag",
            src_label="post",
            dst_label="tag",
        )
        .add_edges(
            Loader(os.path.join(prefix, "person_knows_person_0_0.csv"), delimiter="|"),
            "knows",
            ["creationDate"],
            src_label="person",
            dst_label="person",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "forum_hasModerator_person_0_0.csv"), delimiter="|"
            ),
            "hasModerator",
            src_label="forum",
            dst_label="person",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "person_hasInterest_tag_0_0.csv"), delimiter="|"
            ),
            "hasInterest",
            src_label="person",
            dst_label="tag",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "post_isLocatedIn_place_0_0.csv"), delimiter="|"
            ),
            "isLocatedIn",
            src_label="post",
            dst_label="place",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "comment_isLocatedIn_place_0_0.csv"), delimiter="|"
            ),
            "isLocatedIn",
            src_label="comment",
            dst_label="place",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "organisation_isLocatedIn_place_0_0.csv"),
                delimiter="|",
            ),
            "isLocatedIn",
            src_label="organisation",
            dst_label="place",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "person_isLocatedIn_place_0_0.csv"), delimiter="|"
            ),
            "isLocatedIn",
            src_label="person",
            dst_label="place",
        )
        .add_edges(
            Loader(os.path.join(prefix, "tag_hasType_tagclass_0_0.csv"), delimiter="|"),
            "hasType",
            src_label="tag",
            dst_label="tagclass",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "post_hasCreator_person_0_0.csv"), delimiter="|"
            ),
            "hasCreator",
            src_label="post",
            dst_label="person",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "comment_hasCreator_person_0_0.csv"), delimiter="|"
            ),
            "hasCreator",
            src_label="comment",
            dst_label="person",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "forum_containerOf_post_0_0.csv"), delimiter="|"
            ),
            "containerOf",
            src_label="forum",
            dst_label="post",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "forum_hasMember_person_0_0.csv"), delimiter="|"
            ),
            "hasMember",
            ["joinDate"],
            src_label="forum",
            dst_label="person",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "person_workAt_organisation_0_0.csv"),
                delimiter="|",
            ),
            "workAt",
            ["workFrom"],
            src_label="person",
            dst_label="organisation",
        )
        .add_edges(
            Loader(os.path.join(prefix, "person_likes_comment_0_0.csv"), delimiter="|"),
            "likes",
            ["creationDate"],
            src_label="person",
            dst_label="comment",
        )
        .add_edges(
            Loader(os.path.join(prefix, "person_likes_post_0_0.csv"), delimiter="|"),
            "likes",
            ["creationDate"],
            src_label="person",
            dst_label="post",
        )
        .add_edges(
            Loader(
                os.path.join(prefix, "person_studyAt_organisation_0_0.csv"),
                delimiter="|",
            ),
            "studyAt",
            ["classYear"],
            src_label="person",
            dst_label="organisation",
        )
    )
    return graph
