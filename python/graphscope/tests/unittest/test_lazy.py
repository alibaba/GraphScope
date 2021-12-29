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

from graphscope.dataset import load_p2p_network
from graphscope.framework.app import AppAssets
from graphscope.framework.app import AppDAGNode
from graphscope.framework.errors import AnalyticalEngineInternalError
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.loader import Loader

test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")
prefix = os.path.join(test_repo_dir, "ogbn_mag_small")

property_dir = os.path.join(test_repo_dir, "property")
new_property_dir = os.path.join(test_repo_dir, "new_property", "v2_e2")


@pytest.fixture(scope="module")
def sess():
    session = graphscope.session(cluster_type="hosts", num_workers=2, mode="lazy")
    session.as_default()
    yield session
    session.close()


@pytest.fixture(scope="module")
def arrow_property_graph_lpa_u2i(sess):
    g = sess.g(generate_eid=False)
    g = g.add_vertices(f"{property_dir}/lpa_dataset/lpa_3000_v_0", "v0")
    g = g.add_vertices(f"{property_dir}/lpa_dataset/lpa_3000_v_1", "v1")
    g = g.add_edges(
        f"{property_dir}/lpa_dataset/lpa_3000_e_0", "e0", ["weight"], "v0", "v1"
    )
    return g


@pytest.fixture(scope="function")
def student_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/student.v" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def teacher_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/teacher.v" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def student_group_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/group.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def teacher_group_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/teacher_group.e" % data_dir, header_row=True, delimiter=",")


def test_vertices_omitted_form_loader(sess, student_group_e):
    g = sess.g()
    g1 = g.add_edges(student_group_e)
    g2 = sess.run(g1)  # g2 is a Graph instance
    assert g2.loaded()


def test_construct_graph_step_by_step(sess):
    _g = sess.g(generate_eid=False)
    g = sess.run(_g)
    _g1 = g.add_vertices(f"{new_property_dir}/twitter_v_0", "v0")
    g1 = sess.run(_g1)
    _g2 = g1.add_vertices(f"{new_property_dir}/twitter_v_1", "v1")
    g2 = sess.run(_g2)
    ug = g.unload()
    ug1 = g1.unload()
    ug2 = g2.unload()
    sess.run([ug, ug1, ug2])


def test_unload_graph(sess, student_v, teacher_v, student_group_e):
    # case 1
    # 1. load empty g
    # 2. unload g
    g = sess.g()
    ug = g.unload()
    assert sess.run(ug) is None

    # case 2
    g = sess.g()
    g1 = g.add_vertices(student_v, "student")
    g2 = g.add_vertices(teacher_v, "teacher")
    ug1 = g1.unload()
    ug2 = g2.unload()
    assert sess.run(ug1) is None
    assert sess.run(ug2) is None

    # case 3
    g = sess.g()
    g1 = g.add_vertices(student_v, "student")
    g2 = g1.add_vertices(teacher_v, "teacher")
    g3 = g2.add_edges(
        student_group_e, "group", src_label="student", dst_label="student"
    )
    ug = g.unload()
    ug1 = g1.unload()
    ug2 = g2.unload()
    ug3 = g3.unload()
    sess.run([ug, ug1, ug2, ug3])

    # case 4
    # test unload twice
    g = sess.g()
    ug = g.unload()
    assert sess.run(ug) is None
    assert sess.run(ug) is None


def test_error_using_unload_graph(sess, student_v):
    with pytest.raises(AnalyticalEngineInternalError):
        g = sess.g()
        ug = g.unload()
        g1 = g.add_vertices(student_v, "student")
        sess.run([ug, g1])


def test_unload_app(sess, arrow_property_graph_lpa_u2i):
    # case 1
    a1 = AppDAGNode(
        arrow_property_graph_lpa_u2i,
        AppAssets(algo="lpau2i", context="labeled_vertex_property"),
    )
    ua1 = a1.unload()
    assert sess.run(ua1) is None

    # case 2
    # unload app twice
    a1 = AppDAGNode(
        arrow_property_graph_lpa_u2i,
        AppAssets(algo="lpau2i", context="labeled_vertex_property"),
    )
    ua1 = a1.unload()
    assert sess.run(ua1) is None
    assert sess.run(ua1) is None

    # case 3
    # load app after unload
    a1 = AppDAGNode(
        arrow_property_graph_lpa_u2i,
        AppAssets(algo="lpau2i", context="labeled_vertex_property"),
    )
    ua1 = a1.unload()
    assert sess.run(ua1) is None
    c1 = a1(max_round=10)
    r1 = c1.to_numpy("r:v0.label_0")
    r = sess.run(r1)


def test_graph_to_numpy(sess):
    g = load_p2p_network(sess)
    pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
    c = graphscope.sssp(pg, 20)
    ctx_out_np = c.to_numpy("r")
    g2 = g.add_column(c, {"result_0": "r"})
    graph_out_np = g2.to_numpy("v:host.result_0")
    r = sess.run([ctx_out_np, graph_out_np])
    assert np.all(r[0] == r[1])


def test_graph_to_dataframe(sess):
    g = load_p2p_network(sess)
    pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
    c = graphscope.sssp(pg, 20)
    ctx_out_df = c.to_dataframe({"result": "r"})
    g2 = g.add_column(c, {"result_0": "r"})
    graph_out_df = g2.to_dataframe({"result": "v:host.result_0"})
    r = sess.run([ctx_out_df, graph_out_df])
    assert r[0].equals(r[1])


def test_context(sess):
    g = load_p2p_network(sess)
    pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
    c = graphscope.sssp(pg, 20)
    r1 = c.to_numpy("r")
    r2 = c.to_dataframe({"id": "v.id", "result": "r"})
    r3 = c.to_vineyard_tensor("r")
    r4 = c.to_vineyard_dataframe({"id": "v.id", "data": "v.data", "result": "r"})
    r = sess.run([r1, r2, r3, r4])
    assert r[0].shape == (62586,)
    assert r[1].shape == (62586, 2)
    assert r[2] is not None
    assert r[3] is not None


def test_error_selector_context(sess):
    # vertex data context
    g = load_p2p_network(sess)
    pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
    c = graphscope.sssp(pg, 20)
    with pytest.raises(SyntaxError, match="Invalid selector"):
        r = c.to_dataframe({"id": "v.ID"})
    with pytest.raises(ValueError, match="selector of to_dataframe must be a dict"):
        r = c.to_dataframe("id")


def test_add_column(sess):
    g = load_p2p_network(sess)
    pg = g.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
    c = graphscope.sssp(pg, 20)
    g1 = g.add_column(c, {"id_col": "v.id", "data_col": "v.data", "result_col": "r"})
    sess.run(g1)


def test_multi_src_dst_edge_loader(
    sess, student_group_e, teacher_group_e, student_v, teacher_v
):
    graph = sess.g()
    graph = graph.add_vertices(
        student_v, "student", ["name", "lesson_nums", "avg_score"], "student_id"
    )
    graph = graph.add_vertices(
        teacher_v, "teacher", ["student_num", "score", "email", "tel"], "teacher_id"
    )
    graph = graph.add_edges(
        student_group_e,
        "group",
        ["group_id", "member_size"],
        src_label="student",
        dst_label="student",
        src_field="leader_student_id",
        dst_field="member_student_id",
    )
    graph = graph.add_edges(
        teacher_group_e,
        "group",
        ["group_id", "member_size"],
        src_label="teacher",
        dst_label="teacher",
        src_field="leader_teacher_id",
        dst_field="member_teacher_id",
    )
    g = sess.run(graph)


def test_simulate_eager(sess):
    g1_node = load_p2p_network(sess)
    g1 = sess.run(g1_node)
    pg_node = g1.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})
    pg = sess.run(pg_node)
    c_node = graphscope.sssp(pg, 20)
    c = sess.run(c_node)
    g2_node = g1.add_column(
        c, {"id_col": "v.id", "data_col": "v.data", "result_col": "r"}
    )
    g2 = sess.run(g2_node)
