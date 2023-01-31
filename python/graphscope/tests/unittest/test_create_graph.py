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

import pandas as pd
import pytest
import vineyard

import graphscope
from graphscope.framework.errors import AnalyticalEngineInternalError
from graphscope.framework.loader import Loader
from graphscope.proto import graph_def_pb2


@pytest.fixture(scope="function")
def lesson_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/lesson.v" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def student_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/student.v" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def teacher_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/teacher.v" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def score_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/score.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def student_teacher_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/student_teacher.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def teacher_lesson_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/teacher_lesson.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def student_group_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/group.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def teacher_group_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/teacher_group.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def friend_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/friend.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture(scope="function")
def student_group_e_df(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    df = pd.read_csv(
        os.path.join(data_dir, "group.e"),
        sep=",",
    )
    return df


@pytest.fixture(scope="function")
def student_group_e_array(student_group_e_df):
    array = [
        student_group_e_df[col].values
        for col in ["leader_student_id", "member_student_id"]
    ]
    return array


@pytest.fixture(scope="function")
def student_v_df(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    df = pd.read_csv(
        os.path.join(data_dir, "student.v"),
        sep=",",
    )
    return df


@pytest.fixture(scope="function")
def student_v_array(student_v_df):
    return [student_v_df[col].values for col in student_v_df.columns.values]


@pytest.fixture(scope="function")
def teacher_v_oss():
    return "oss://siyuan-transfer/property_graph/teacher.v"


@pytest.fixture(scope="function")
def lesson_v_mars():
    return "mars://lesson.v"


@pytest.fixture(scope="function")
def one_column_file(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/one_column.e" % data_dir, header_row=True)


@pytest.fixture(scope="function")
def double_type_id_file(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/double_type_id_file" % data_dir)


@pytest.fixture(scope="function")
def empty_file(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/empty_file" % data_dir)


def test_dict_in_dict_form_loader_deprecated(
    graphscope_session, student_group_e, student_v
):
    g = graphscope_session.load_from(
        edges={
            "group": {
                "loader": student_group_e,
                "properties": ["member_size"],
                "source": ("leader_student_id", "student"),
                "destination": ("member_student_id", "student"),
                "load_strategy": "both_out_in",
            }
        },
        vertices={
            "student": {
                "loader": student_v,
                "properties": ["name", "lesson_nums", "avg_score"],
                "vid": "student_id",
            }
        },
    )


def test_complete_form_loader(graphscope_session, student_group_e, student_v):
    # a complete form for loading from ev files.
    # types are inferred from Loader.
    graph = graphscope_session.g()
    graph = graph.add_vertices(
        student_v, "student", ["name", "lesson_nums", "avg_score"], "student_id"
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
    assert graph.loaded()


def test_complete_form_loader_deprecated(
    graphscope_session, student_group_e, student_v
):
    graph = graphscope_session.load_from(
        edges={
            "group": (
                student_group_e,
                ["group_id", "member_size"],
                (
                    "leader_student_id",
                    "student",
                ),
                ("member_student_id", "student"),
                "both_out_in",
            )
        },
        vertices={
            "student": (
                student_v,
                ["name", "lesson_nums", "avg_score"],
                "student_id",
            )
        },
    )
    assert graph.loaded()


def test_default_prop_is_none_loader(graphscope_session, student_group_e, student_v):
    graph = graphscope_session.g(generate_eid=False, retain_oid=False)
    graph = graph.add_vertices(student_v, "student")
    graph = graph.add_edges(student_group_e, "group")
    assert len(graph.schema.get_vertex_properties("student")) == 3
    assert len(graph.schema.get_edge_properties("group")) == 2


def test_prop_is_empty_loader(graphscope_session, student_group_e, student_v):
    graph = graphscope_session.g(generate_eid=False, retain_oid=False)
    graph = graph.add_vertices(student_v, "student", [], "student_id")
    graph = graph.add_edges(student_group_e, "group", [])
    assert len(graph.schema.get_vertex_properties("student")) == 0
    assert len(graph.schema.get_edge_properties("group")) == 0


def test_properties_omitted_loader_with_generate_eid(
    graphscope_session, student_group_e, student_v
):
    graph = graphscope_session.g(generate_eid=True, retain_oid=True)
    graph = graph.add_vertices(student_v, "student", None, "student_id")
    graph = graph.add_edges(student_group_e, "group", None)
    assert len(graph.schema.get_vertex_properties("student")) == 4
    assert len(graph.schema.get_edge_properties("group")) == 3


def test_loader_with_specified_data_type(
    graphscope_session, student_group_e, student_v
):
    # retain oid
    graph = graphscope_session.g(oid_type="string", generate_eid=False, retain_oid=True)
    graph = graph.add_vertices(
        student_v,
        "student",
        ["name", ("lesson_nums", "int"), ("avg_score", "float")],
        "student_id",
    )
    graph = graph.add_edges(
        student_group_e, "group", ["group_id", ("member_size", "int")]
    )
    assert [p.type for p in graph.schema.get_vertex_properties("student")] == [
        graph_def_pb2.STRING,
        graph_def_pb2.INT,
        graph_def_pb2.FLOAT,
        graph_def_pb2.STRING,
    ]
    assert [p.type for p in graph.schema.get_edge_properties("group")] == [
        graph_def_pb2.STRING,
        graph_def_pb2.INT,
    ]

    # don't retain oid
    graph = graphscope_session.g(
        oid_type="string", generate_eid=False, retain_oid=False
    )
    graph = graph.add_vertices(
        student_v,
        "student",
        ["name", ("lesson_nums", "int"), ("avg_score", "float")],
        "student_id",
    )
    graph = graph.add_edges(
        student_group_e, "group", ["group_id", ("member_size", "int")]
    )
    assert [p.type for p in graph.schema.get_vertex_properties("student")] == [
        graph_def_pb2.STRING,
        graph_def_pb2.INT,
        graph_def_pb2.FLOAT,
    ]
    assert [p.type for p in graph.schema.get_edge_properties("group")] == [
        graph_def_pb2.STRING,
        graph_def_pb2.INT,
    ]


@pytest.mark.skip(reason="waiting for vineyard support")
def test_multi_src_dst_edge_loader(
    graphscope_session, student_group_e, teacher_group_e, student_v, teacher_v
):
    graph = graphscope_session.g()
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
    assert graph.loaded()


def test_vid_omitted_form_loader(graphscope_session, student_group_e, student_v):
    # vid can be omit, the first column will be used as vid;
    graph = graphscope_session.g()
    graph = graph.add_vertices(
        student_v, "student", ["name", "lesson_nums", "avg_score"]
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
    assert graph.loaded()


def test_v_property_omitted_form_loader(graphscope_session, student_group_e, student_v):
    # properties for v can be omit, all columns will be load,
    # the first one used as vid by # default. default vlabel would be '_';
    graph = graphscope_session.g()
    graph = graph.add_vertices(student_v, "student")
    graph = graph.add_edges(
        student_group_e,
        "group",
        ["group_id", "member_size"],
        src_field="leader_student_id",
        dst_field="member_student_id",
    )
    assert graph.loaded()


def test_vertices_omitted_form_loader(graphscope_session, student_group_e):
    # vertices can be omit.
    graph = graphscope_session.g()
    graph = graph.add_edges(student_group_e)
    assert graph.loaded()


def test_all_omitted_form_loader(graphscope_session, student_group_e):
    graph = graphscope_session.g()
    graph = graph.add_edges(student_group_e, "group")
    assert graph.loaded()


def test_multiple_e_all_omitted_form_loader(
    graphscope_session, student_group_e, friend_e
):
    graph = graphscope_session.g()
    graph = graph.add_edges(student_group_e, "group")
    graph = graph.add_edges(friend_e, "friend")
    assert graph.loaded()


def test_load_from_numpy(graphscope_session, student_group_e_array, student_v_array):
    graph = graphscope_session.g()
    graph = graph.add_vertices(student_v_array)
    graph = graph.add_edges(student_group_e_array)
    assert graph.loaded()


def test_load_from_pandas(graphscope_session, student_group_e_df, student_v_df):
    graph = graphscope_session.g()
    graph = graph.add_vertices(student_v_df)
    graph = graph.add_edges(student_group_e_df)
    assert graph.loaded()


def test_load_from_oneline_pandas(graphscope_session, student_group_e_df):
    e_oneline = student_group_e_df[:1]
    graph = graphscope_session.g()
    graph = graph.add_edges(e_oneline)
    assert graph.loaded()


# test errors
def test_errors_on_files(
    graphscope_session, one_column_file, double_type_id_file, empty_file
):
    with pytest.raises(AnalyticalEngineInternalError, match="Object not exists"):
        graphscope_session.g(vineyard.ObjectName("non_exist_vy_name"))


def test_error_on_non_default_and_non_existing_v_label(
    graphscope_session, student_group_e, student_v
):
    graph = graphscope_session.g()
    graph = graph.add_vertices(student_v, "student")
    with pytest.raises(
        ValueError, match="must be both specified or either unspecified"
    ):
        graph = graph.add_edges(student_group_e, src_label="v")


def test_error_on_src_dst_refer_to_same_col(graphscope_session, student_group_e):
    graph = graphscope_session.g()
    with pytest.raises(AssertionError, match="cannot refer to the same field"):
        graph = graph.add_edges(
            student_group_e,
            "group",
            src_field="leader_student_id",
            dst_field="leader_student_id",
        )
    with pytest.raises(AssertionError, match="cannot refer to the same field"):
        graph = graph.add_edges(student_group_e, "group", src_field=0, dst_field=0)


def test_error_on_ambigious_default_label(
    graphscope_session, student_group_e, student_v, teacher_v
):
    graph = graphscope_session.g()
    graph = graph.add_vertices(student_v, "student")
    graph = graph.add_vertices(teacher_v, "teacher")

    with pytest.raises(AssertionError, match="Ambiguous vertex label"):
        graph = graph.add_edges(student_group_e, "group")


def test_error_on_duplicate_labels(graphscope_session, student_group_e, student_v):
    graph = graphscope_session.g()
    graph = graph.add_vertices(student_v, "student")
    with pytest.raises(ValueError, match="Label student already existed in graph"):
        graph = graph.add_vertices(student_v, "student")
    graph = graph.add_edges(student_group_e, "group")
    with pytest.raises(ValueError, match="already existed in graph"):
        graph = graph.add_edges(student_group_e, "group")


def test_load_complex_graph(
    graphscope_session,
    score_e,
    student_teacher_e,
    teacher_lesson_e,
    student_v,
    teacher_v,
    lesson_v,
):
    graph = graphscope_session.g(oid_type="string")
    graph = graph.add_vertices(
        student_v, "student", ["name", "lesson_nums", "avg_score"], "student_id"
    )
    graph = graph.add_vertices(
        teacher_v, "teacher", ["student_num", "score", "email", "tel"]
    )
    graph = graph.add_vertices(lesson_v, "lesson")
    graph = graph.add_edges(
        score_e,
        "score",
        ["score", "score_id"],
        src_label="student",
        dst_label="lesson",
        src_field="student_id",
        dst_field="subject",
    )
    graph = graph.add_edges(
        student_teacher_e,
        "student_teacher",
        ["teaching_score"],
        src_label="student",
        dst_label="teacher",
        src_field="student_id",
        dst_field="teacher_id",
    )
    graph = graph.add_edges(
        teacher_lesson_e,
        "teacher_lesson",
        ["times"],
        src_label="teacher",
        dst_label="lesson",
        src_field="teacher_id",
        dst_field="lesson",
    )
    assert graph.schema is not None


def test_load_complex_graph_by_index(
    graphscope_session,
    score_e,
    student_teacher_e,
    teacher_lesson_e,
    student_v,
    teacher_v,
    lesson_v,
):
    graph = graphscope_session.g(oid_type="string")
    graph = graph.add_vertices(
        student_v, "student", ["name", "lesson_nums", "avg_score"], 0
    )
    graph = graph.add_vertices(
        teacher_v, "teacher", ["student_num", "score", "email", "tel"]
    )
    graph = graph.add_vertices(lesson_v, "lesson")
    graph = graph.add_edges(
        score_e,
        "score",
        ["score", "score_id"],
        src_label="student",
        dst_label="lesson",
        src_field=0,
        dst_field=1,
    )
    graph = graph.add_edges(
        student_teacher_e,
        "student_teacher",
        ["teaching_score"],
        src_label="student",
        dst_label="teacher",
        src_field=0,
        dst_field=1,
    )
    graph = graph.add_edges(
        teacher_lesson_e,
        "teacher_lesson",
        ["times"],
        src_label="teacher",
        dst_label="lesson",
        src_field=0,
        dst_field=1,
    )
    assert graph.schema is not None


@pytest.mark.skip(reason="vineyard, mars variants not ready")
def test_Load_complex_graph_variants(
    graphscope_session,
    score_e,
    student_group_e_df,
    student_v_array,
    teacher_v_oss,
    lesson_v_mars,
):
    graph = graphscope_session.g()
    graph = graph.add_vertices(
        student_v_array, "student", ["name", "lesson_nums", "avg_score"], "student_id"
    )
    graph = graph.add_vertices(
        teacher_v_oss, "teacher", ["student_num", "score", "email", "tel"]
    )
    graph = graph.add_vertices(lesson_v_mars, "lesson")
    graph = graph.add_edges(
        score_e,
        "score",
        ["score", "score_id"],
        src_label="student",
        dst_label="lesson",
        src_field="studnet_id",
        dst_field="subject",
    )
    graph = graph.add_edges(
        student_group_e_df,
        "group",
        ["member_size"],
        src_label="student",
        dst_label="student",
        src_field="leader_studnet_id",
        dst_field="member_student_id",
    )
    assert graph.schema is not None


def test_local_vertex_map_e_file(graphscope_session, student_group_e):
    graph = graphscope_session.g(vertex_map="local")
    graph = graph.add_edges(student_group_e)
    assert graph.schema is not None
    ret = graphscope.wcc(graph)
    df = ret.to_dataframe({"id": "v.id", "community": "r"})
    assert df.shape == (10, 2)


def test_local_vertex_map_complete_form_loader(
    graphscope_session, student_group_e, student_v
):
    graph = graphscope_session.load_from(
        edges={
            "group": (
                student_group_e,
                ["group_id", "member_size"],
                (
                    "leader_student_id",
                    "student",
                ),
                ("member_student_id", "student"),
                "both_out_in",
            )
        },
        vertices={
            "student": (
                student_v,
                ["name", "lesson_nums", "avg_score"],
                "student_id",
            )
        },
        vertex_map="local",
    )
    assert graph.schema is not None
    ret = graphscope.wcc(graph)
    df = ret.to_dataframe({"id": "v.id", "community": "r"})
    assert df.shape == (10, 2)


def test_local_vertex_map_e_file_str(graphscope_session, student_group_e):
    graph = graphscope_session.g(oid_type="str", vertex_map="local")
    graph = graph.add_edges(student_group_e)
    assert graph.schema is not None
    ret = graphscope.wcc(graph)
    df = ret.to_dataframe({"id": "v.id", "community": "r"})
    assert df.shape == (10, 2)
