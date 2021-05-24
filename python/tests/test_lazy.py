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

import importlib
import logging
import os
import random
import string
import sys
import time

import numpy as np
import pytest

import graphscope

graphscope.set_option(show_log=True)

from graphscope import property_sssp
from graphscope import sssp
from graphscope.framework.loader import Loader

test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")
prefix = os.path.join(test_repo_dir, "ogbn_mag_small")

new_property_dir = os.path.join(test_repo_dir, "new_property", "v2_e2")


@pytest.fixture(scope="module")
def sess():
    session = graphscope.session(cluster_type="hosts", num_workers=2, mode="lazy")
    session.as_default()
    yield session
    session.close()


@pytest.fixture
def student_group_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/group.e" % data_dir, header_row=True, delimiter=",")


def arrow_property_graph(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "e0": [
                (
                    Loader(
                        "{}/twitter_e_0_0_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_0_1_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_0_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_1_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v1"),
                ),
            ],
            "e1": [
                (
                    Loader(
                        "{}/twitter_e_0_0_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_0_1_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_0_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_1_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v1"),
                ),
            ],
        },
        vertices={
            "v0": Loader("{}/twitter_v_0".format(new_property_dir), header_row=True),
            "v1": Loader("{}/twitter_v_1".format(new_property_dir), header_row=True),
        },
        generate_eid=False,
    )
    return g


@pytest.mark.skip(reason="waiting for optimization of dag")
def test_vertices_omitted_form_loader(sess, student_group_e):
    g = sess.g()
    g1 = g.add_edges(student_group_e)
    g2 = sess.run(g1)  # g2 is a Graph instance
    assert g2.loaded()


def test_context(sess):
    g = arrow_property_graph(sess)
    c = property_sssp(g, 20)
    r1 = c.to_numpy("r:v0.dist_0")
    r2 = c.to_dataframe({"id": "v:v0.id", "result": "r:v0.dist_0"})
    r3 = c.to_vineyard_tensor("v:v0.id")
    r4 = c.to_vineyard_dataframe(
        {"id": "v:v0.id", "data": "v:v0.dist", "result": "r:v0.dist_0"}
    )
    r = sess.run([r1, r2, r3, r4])
    assert r[0].shape == (40521,)
    assert r[1].shape == (40521, 2)
    assert r[2] is not None
    assert r[3] is not None


def test_query_after_project(sess):
    g = arrow_property_graph(sess)
    pg = g.project(vertices={"v0": ["id"]}, edges={"e0": ["weight"]})
    # property sssp on property graph
    # sssp on simple graph
    c = sssp(pg, 20)
    r1 = c.to_dataframe({"node": "v.id", "r": "r"})
    r = sess.run(r1)
    assert r.shape == (40521, 2)


def test_add_column(sess):
    g = arrow_property_graph(sess)
    pg = g.project(vertices={"v0": ["id"]}, edges={"e0": ["weight"]})
    c = sssp(pg, 20)
    g1 = g.add_column(c, {"id_col": "v.id", "data_col": "v.data", "result_col": "r"})
    sess.run(g1)
