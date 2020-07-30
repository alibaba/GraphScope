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
    vertices = {
        "comment": (
            Loader(
                os.path.join(prefix, "comment_0_0.csv"), header_row=True, delimiter="|"
            ),
            ["creationDate", "locationIP", "browserUsed", "content", "length"],
            "id",
        ),
        "organisation": (
            Loader(
                os.path.join(prefix, "organisation_0_0.csv"),
                header_row=True,
                delimiter="|",
            ),
            ["type", "name", "url"],
            "id",
        ),
        "tagclass": (
            Loader(
                os.path.join(prefix, "tagclass_0_0.csv"), header_row=True, delimiter="|"
            ),
            ["name", "url"],
            "id",
        ),
        "person": (
            Loader(
                os.path.join(prefix, "person_0_0.csv"), header_row=True, delimiter="|"
            ),
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
        ),
        "forum": (
            Loader(
                os.path.join(prefix, "forum_0_0.csv"), header_row=True, delimiter="|"
            ),
            ["title", "creationDate"],
            "id",
        ),
        "place": (
            Loader(
                os.path.join(prefix, "place_0_0.csv"), header_row=True, delimiter="|"
            ),
            ["name", "url", "type"],
            "id",
        ),
        "post": (
            Loader(
                os.path.join(prefix, "post_0_0.csv"), header_row=True, delimiter="|"
            ),
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
        ),
        "tag": (
            Loader(os.path.join(prefix, "tag_0_0.csv"), header_row=True, delimiter="|"),
            ["name", "url"],
            "id",
        ),
    }
    edges = {
        "replyOf": [
            (
                Loader(
                    os.path.join(prefix, "comment_replyOf_comment_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Comment.id", "comment"),
                ("Comment.id.1", "comment"),
            ),
            (
                Loader(
                    os.path.join(prefix, "comment_replyOf_post_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Comment.id", "comment"),
                ("Post.id", "post"),
            ),
        ],
        "isPartOf": [
            (
                Loader(
                    os.path.join(prefix, "place_isPartOf_place_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Place.id", "place"),
                ("Place.id.1", "place"),
            )
        ],
        "isSubclassOf": [
            (
                Loader(
                    os.path.join(prefix, "tagclass_isSubclassOf_tagclass_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("TagClass.id", "tagclass"),
                ("TagClass.id.1", "tagclass"),
            )
        ],
        "hasTag": [
            (
                Loader(
                    os.path.join(prefix, "forum_hasTag_tag_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Forum.id", "forum"),
                ("Tag.id", "tag"),
            ),
            (
                Loader(
                    os.path.join(prefix, "comment_hasTag_tag_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Comment.id", "comment"),
                ("Tag.id", "tag"),
            ),
            (
                Loader(
                    os.path.join(prefix, "post_hasTag_tag_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Post.id", "post"),
                ("Tag.id", "tag"),
            ),
        ],
        "knows": [
            (
                Loader(
                    os.path.join(prefix, "person_knows_person_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["creationDate"],
                ("Person.id", "person"),
                ("Person.id.1", "person"),
            )
        ],
        "hasModerator": [
            (
                Loader(
                    os.path.join(prefix, "forum_hasModerator_person_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Forum.id", "forum"),
                ("Person.id", "person"),
            )
        ],
        "hasInterest": [
            (
                Loader(
                    os.path.join(prefix, "person_hasInterest_tag_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Person.id", "person"),
                ("Tag.id", "tag"),
            )
        ],
        "isLocatedIn": [
            (
                Loader(
                    os.path.join(prefix, "post_isLocatedIn_place_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Post.id", "post"),
                ("Place.id", "place"),
            ),
            (
                Loader(
                    os.path.join(prefix, "comment_isLocatedIn_place_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Comment.id", "comment"),
                ("Place.id", "place"),
            ),
            (
                Loader(
                    os.path.join(prefix, "organisation_isLocatedIn_place_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Organisation.id", "organisation"),
                ("Place.id", "place"),
            ),
            (
                Loader(
                    os.path.join(prefix, "person_isLocatedIn_place_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Person.id", "person"),
                ("Place.id", "place"),
            ),
        ],
        "hasType": [
            (
                Loader(
                    os.path.join(prefix, "tag_hasType_tagclass_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Tag.id", "tag"),
                ("TagClass.id", "tagclass"),
            )
        ],
        "hasCreator": [
            (
                Loader(
                    os.path.join(prefix, "post_hasCreator_person_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Post.id", "post"),
                ("Person.id", "person"),
            ),
            (
                Loader(
                    os.path.join(prefix, "comment_hasCreator_person_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Comment.id", "comment"),
                ("Person.id", "person"),
            ),
        ],
        "containerOf": [
            (
                Loader(
                    os.path.join(prefix, "forum_containerOf_post_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                [],
                ("Forum.id", "forum"),
                ("Post.id", "post"),
            )
        ],
        "hasMember": [
            (
                Loader(
                    os.path.join(prefix, "forum_hasMember_person_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["joinDate"],
                ("Forum.id", "forum"),
                ("Person.id", "person"),
            )
        ],
        "workAt": [
            (
                Loader(
                    os.path.join(prefix, "person_workAt_organisation_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["workFrom"],
                ("Person.id", "person"),
                ("Organisation.id", "organisation"),
            )
        ],
        "likes": [
            (
                Loader(
                    os.path.join(prefix, "person_likes_comment_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["creationDate"],
                ("Person.id", "person"),
                ("Comment.id", "comment"),
            ),
            (
                Loader(
                    os.path.join(prefix, "person_likes_post_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["creationDate"],
                ("Person.id", "person"),
                ("Post.id", "post"),
            ),
        ],
        "studyAt": [
            (
                Loader(
                    os.path.join(prefix, "person_studyAt_organisation_0_0.csv"),
                    header_row=True,
                    delimiter="|",
                ),
                ["classYear"],
                ("Person.id", "person"),
                ("Organisation.id", "organisation"),
            )
        ],
    }
    return sess.load_from(edges, vertices, directed, generate_eid=True)
