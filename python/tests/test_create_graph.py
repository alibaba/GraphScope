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

import numpy as np
import pandas as pd
import pytest
import vineyard

import graphscope
from graphscope.framework.errors import AnalyticalEngineInternalError
from graphscope.framework.loader import Loader


@pytest.fixture
def lesson_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/lesson.v" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def student_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/student.v" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def teacher_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/teacher.v" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def score_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/score.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def student_teacher_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/student_teacher.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def teacher_lesson_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/teacher_lesson.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def student_group_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/group.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def teacher_group_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/teacher_group.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def friend_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/friend.e" % data_dir, header_row=True, delimiter=",")


@pytest.fixture
def student_group_e_df(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    df = pd.read_csv(
        os.path.join(data_dir, "group.e"),
        sep=",",
        usecols=["leader_student_id", "member_student_id", "member_size"],
    )
    return df


@pytest.fixture
def student_group_e_array(student_group_e_df):
    array = [
        student_group_e_df[col].values
        for col in ["leader_student_id", "member_student_id", "member_size"]
    ]
    array[2] = array[2].astype("double")
    return array


@pytest.fixture
def student_v_df(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    df = pd.read_csv(
        os.path.join(data_dir, "student.v"),
        sep=",",
        usecols=["student_id", "lesson_nums", "avg_score"],
    )
    return df


@pytest.fixture
def student_v_array(student_v_df):
    return [
        student_v_df[col].values for col in ["student_id", "lesson_nums", "avg_score"]
    ]


@pytest.fixture
def teacher_v_oss():
    return "oss://siyuan-transfer/property_graph/teacher.v"


@pytest.fixture
def lesson_v_mars():
    return "mars://lesson.v"


@pytest.fixture
def one_column_file(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/one_column.e" % data_dir, header_row=True)


@pytest.fixture
def double_type_id_file(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/double_type_id_file" % data_dir)


@pytest.fixture
def empty_file(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return Loader("%s/empty_file" % data_dir)


def test_dict_in_dict_form_loader(graphscope_session, student_group_e, student_v):
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

    g = graphscope_session.load_from(
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


def test_properties_omitted_loader(graphscope_session, student_group_e, student_v):
    g = graphscope_session.load_from(
        edges={
            "group": (
                student_group_e,
                [],
                ("leader_student_id", "student"),
                ("member_student_id", "student"),
            )
        },
        vertices={
            "student": (
                student_v,
                [],
                "student_id",
            )
        },
        generate_eid=False,
    )
    assert len(g.schema.vertex_properties[0]) == 4
    assert len(g.schema.edge_properties[0]) == 2


def test_properties_omitted_loader_with_generate_eid(
    graphscope_session, student_group_e, student_v
):
    g2 = graphscope_session.load_from(
        edges={
            "group": (
                student_group_e,
                [],
                ("leader_student_id", "student"),
                ("member_student_id", "student"),
            )
        },
        vertices={
            "student": (
                student_v,
                [],
                "student_id",
            )
        },
        generate_eid=True,
    )
    assert len(g2.schema.vertex_properties[0]) == 4
    assert len(g2.schema.edge_properties[0]) == 3


def test_loader_with_specified_data_type(
    graphscope_session, student_group_e, student_v
):
    # a complete form for loading from ev files.
    # types are inferred from Loader.

    # vid is the column name, used as vertex_id, this one can be omit,
    # when omit, will use the first column as the vertex_id;
    g = graphscope_session.load_from(
        edges={
            "group": (
                student_group_e,
                ["group_id", ("member_size", "int")],
                (
                    "leader_student_id",
                    "student",
                ),  # use src column as the source, the vlabel should be 'v';
                ("member_student_id", "student"),
                "both_out_in",
            )
        },
        vertices={
            "student": (
                student_v,
                ["name", ("lesson_nums", "int"), ("avg_score", "float")],
                "student_id",
            )
        },
        oid_type="string",
        generate_eid=False,
    )
    assert g.schema.vertex_properties == [
        {"name": 21, "lesson_nums": 11, "avg_score": 18, "student_id": 21}
    ]
    assert g.schema.edge_properties == [{"group_id": 21, "member_size": 11}]


def test_dict_multi_src_dst_edge_loader(
    graphscope_session, student_group_e, teacher_group_e, student_v, teacher_v
):
    g = graphscope_session.load_from(
        edges={
            "group": [
                {
                    "loader": teacher_group_e,
                    "properties": ["group_id", "member_size"],
                    "source": ("leader_teacher_id", "teacher"),
                    "destination": ("member_teacher_id", "teacher"),
                },
                {
                    "loader": student_group_e,
                    "properties": ["group_id", "member_size"],
                    "source": ("leader_student_id", "student"),
                    "destination": ("member_student_id", "student"),
                },
            ]
        },
        vertices={
            "student": {
                "loader": student_v,
                "properties": ["name", "lesson_nums", "avg_score"],
                "vid": "student_id",
            },
            "teacher": {
                "loader": teacher_v,
                "properties": ["student_num", "score", "email", "tel"],
                "vid": "teacher_id",
            },
        },
    )


def test_multi_src_dst_edge_loader(
    graphscope_session, student_group_e, teacher_group_e, student_v, teacher_v
):
    g = graphscope_session.load_from(
        edges={
            "group": [
                (
                    teacher_group_e,
                    ["group_id", "member_size"],
                    ("leader_teacher_id", "teacher"),
                    ("member_teacher_id", "teacher"),
                ),
                (
                    student_group_e,
                    ["group_id", "member_size"],
                    ("leader_student_id", "student"),
                    ("member_student_id", "student"),
                ),
            ]
        },
        vertices={
            "student": (
                student_v,
                ["name", "lesson_nums", "avg_score"],
                "student_id",
            ),
            "teacher": (
                teacher_v,
                ["student_num", "score", "email", "tel"],
                "teacher_id",
            ),
        },
    )


def test_vid_omitted_form_loader(graphscope_session, student_group_e, student_v):
    # vid can be omit, the first column will be used as vid;
    g = graphscope_session.load_from(
        edges={
            "group": (
                student_group_e,
                ["group_id", "member_size"],
                ("leader_student_id", "student"),
                ("member_student_id", "student"),
                "both_out_in",
            )
        },
        vertices={
            "student": (
                student_v,
                ["name", "lesson_nums", "avg_score"],
            )
        },
    )


def test_v_property_omitted_form_loader(graphscope_session, student_group_e, student_v):
    # properties for v can be omit, all columns will be load,
    # the first one used as vid by # default. default vlabel would be '_';
    g = graphscope_session.load_from(
        edges={
            "e": (
                student_group_e,
                ["group_id", "member_size"],
                "leader_student_id",
                "member_student_id",
                "both_out_in",
            )
        },
        vertices={"student": student_v},
    )


def test_vertices_omitted_form_loader(graphscope_session, student_group_e):
    # vertices can be omit.
    g = graphscope_session.load_from(
        edges={
            "e": (
                student_group_e,
                ["group_id", "member_size"],
                "leader_student_id",
                "member_student_id",
                "both_out_in",
            )
        }
    )


def test_label_omitted_form_loader(graphscope_session, student_group_e, student_v):
    # labels can be omitted, if there is only one label for edges/vertices,
    # a default label `_` is assigned.
    g = graphscope_session.load_from(
        edges=(
            student_group_e,
            ["group_id", "member_size"],
            "leader_student_id",
            "member_student_id",
            "both_out_in",
        ),
        vertices=student_v,
    )


def test_src_dst_omitted_form_loader(graphscope_session, student_group_e, student_v):
    # or totally omit the src/dst. the 1st, and 2nd columns will be used by default.
    g = graphscope_session.load_from(
        edges=(
            student_group_e,
            ["group_id", "member_size"],
        ),
        vertices=student_v,
    )


def test_all_omitted_form_loader(graphscope_session, student_group_e):
    g = graphscope_session.load_from(edges=(student_group_e, []))


def test_multiple_e_all_omitted_form_loader(
    graphscope_session, student_group_e, friend_e
):
    g6 = graphscope_session.load_from(
        edges={
            "e1": student_group_e,
            "e2": friend_e,
        }
    )


def test_load_from_numpy(graphscope_session, student_group_e_array, student_v_array):
    g = graphscope_session.load_from(
        edges=student_group_e_array, vertices=student_v_array
    )


def test_load_from_pandas(graphscope_session, student_group_e_df, student_v_df):
    g = graphscope_session.load_from(edges=student_group_e_df, vertices=student_v_df)


# test errors
def test_error_on_location(graphscope_session):
    with pytest.raises(AnalyticalEngineInternalError, match="IOError"):
        non_existing_file = "file:///abc"
        g = graphscope_session.load_from(edges=Loader(non_existing_file))


def test_errors_on_file_format(
    graphscope_session, one_column_file, double_type_id_file, empty_file
):
    with pytest.raises(AnalyticalEngineInternalError, match="Index out of range: 1"):
        g1 = graphscope_session.load_from(edges=one_column_file)
    with pytest.raises(
        AnalyticalEngineInternalError, match="CSV conversion error to int64"
    ):
        g2 = graphscope_session.load_from(edges=double_type_id_file)
    with pytest.raises(AnalyticalEngineInternalError, match="End Of File"):
        g3 = graphscope_session.load_from(edges=empty_file)
    with pytest.raises(AnalyticalEngineInternalError, match="Object not exists"):
        g4 = graphscope_session.load_from(vineyard.ObjectName("non_exist_vy_name"))


def test_error_on_non_existing_load_strategy(
    graphscope_session, student_group_e, student_v
):
    # non-existing load strategy
    with pytest.raises(AssertionError, match="invalid load strategy: wrong_strategy"):
        g = graphscope_session.load_from(
            edges=(
                student_group_e,
                ["group_id", "member_size"],
                "leader_student_id",
                "member_student_id",
                "wrong_strategy",
            ),
            vertices=student_v,
        )


def test_error_on_non_default_and_non_existing_v_label(
    graphscope_session, student_group_e, student_v
):
    with pytest.raises(AssertionError, match="label not found in vertex labels"):
        g = graphscope_session.load_from(
            edges={
                "group": (
                    student_group_e,
                    ["group_id", "member_size"],
                    ("leader_student_id", "v"),
                    ("member_student_id", "v"),
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


def test_error_on_too_much_src_dst(graphscope_session, student_group_e, student_v):
    with pytest.raises(AssertionError, match="Too many arguments for a edge label"):
        g = graphscope_session.load_from(
            edges={
                "group": (
                    student_group_e,
                    ["group_id", "member_size"],
                    ("leader_student_id", "student"),
                    ("member_student_id", "student"),
                    ("extra_vid", "student"),
                    "both_out_in",
                )
            },
            vertices={"student": student_v},
        )


def test_error_on_src_dst_refer_to_same_col(
    graphscope_session, student_group_e, student_v
):
    with pytest.raises(AssertionError, match="cannot refer to the same col"):
        g = graphscope_session.load_from(
            edges={
                "group": (
                    student_group_e,
                    ["group_id", "member_size"],
                    ("leader_student_id", "student"),
                    ("leader_student_id", "student"),
                    "both_out_in",
                )
            },
            vertices={"student": student_v},
        )


def test_error_on_missing_edges(graphscope_session, student_v):
    with pytest.raises(
        TypeError, match="missing 1 required positional argument: 'edges'"
    ):
        g = graphscope_session.load_from(vertices={"v": student_v})


def test_error_on_ambigious_default_label(
    graphscope_session, student_group_e, student_v, teacher_v
):
    with pytest.raises(AssertionError, match="ambiguous vertex label"):
        g = graphscope_session.load_from(
            edges={
                "group": (
                    student_group_e,
                    ["group_id", "member_size"],
                    "leader_student_id",
                    "member_student_id",
                    "both_out_in",
                )
            },
            vertices={
                "student": student_v,
                "teacher": teacher_v,
            },
        )


def test_error_on_duplicate_labels(graphscope_session, student_group_e, student_v):
    g = graphscope_session.load_from(
        edges={
            "group": (
                student_group_e,
                ["group_id", "member_size"],
                ("leader_student_id", "student"),
                ("member_student_id", "student"),
                "both_out_in",
            )
        },
        vertices={
            "student": student_v,
            "student": student_v,
        },
    )


def test_errors_on_unknown_parameters(graphscope_session, student_group_e, student_v):
    with pytest.raises(AssertionError, match="Too many arguments for a edge label"):
        g = graphscope_session.load_from(
            edges={
                "group": (
                    student_group_e,
                    "unknown",
                    ["group_id", "member_size"],
                    ("leader_student_id", "student"),
                    ("member_student_id", "student"),
                    ("extra_vid", "student"),
                    "both_out_in",
                )
            },
            vertices={"student": student_v},
        )


def test_load_complex_graph(
    graphscope_session,
    score_e,
    student_teacher_e,
    teacher_lesson_e,
    student_v,
    teacher_v,
    lesson_v,
):
    g = graphscope_session.load_from(
        edges={
            "score": (
                score_e,
                ["score", "score_id"],
                ("student_id", "student"),
                ("subject", "lesson"),
                "both_out_in",
            ),
            "student_teacher": (
                student_teacher_e,
                ["teaching_score"],
                ("student_id", "student"),
                ("teacher_id", "teacher"),
            ),
            "teacher_lesson": (
                teacher_lesson_e,
                ["times"],
                ("teacher_id", "teacher"),
                ("lesson", "lesson"),
                "only_out",
            ),
        },
        vertices={
            "student": (
                student_v,
                ["name", "lesson_nums", "avg_score"],
                "student_id",
            ),
            "teacher": (
                teacher_v,
                ["student_num", "score", "email", "tel"],
            ),
            "lesson": lesson_v,
        },
        oid_type="string",
    )


def test_load_complex_graph_by_index(
    graphscope_session,
    score_e,
    student_teacher_e,
    teacher_lesson_e,
    student_v,
    teacher_v,
    lesson_v,
):
    g = graphscope_session.load_from(
        edges={
            "score": (
                score_e,
                ["score", "score_id"],
                (0, "student"),
                (1, "lesson"),
                "both_out_in",
            ),
            "student_teacher": (
                student_teacher_e,
                ["teaching_score"],
                (0, "student"),
                (1, "teacher"),
            ),
            "teacher_lesson": (
                teacher_lesson_e,
                ["times"],
                (0, "teacher"),
                (1, "lesson"),
                "only_out",
            ),
        },
        vertices={
            "student": (
                student_v,
                ["name", "lesson_nums", "avg_score"],
                0,
            ),
            "teacher": (
                teacher_v,
                ["student_num", "score", "email", "tel"],
            ),
            "lesson": lesson_v,
        },
        oid_type="string",
    )


@pytest.mark.skip(reason="vineyard, mars variants not ready")
def test_Load_complex_graph_variants(
    graphscope_session,
    score_e,
    student_group_e_df,
    student_v_array,
    teacher_v_oss,
    lesson_v_mars,
):
    g = graphscope_session.load_from(
        edges={
            "score": (
                score_e,
                ["score", "score_id"],
                ("student_id", "student"),
                ("subject", "lesson"),
                "both_out_in",
            ),
            "group": (
                student_group_e_df,
                ["member_size"],
                ("leader_student_id", "student"),
                ("member_student_id", "student"),
            ),
        },
        vertices={
            "student": (
                student_v_array,
                ["name", "lesson_nums", "avg_score"],
                "student_id",
            ),
            "teacher": (
                teacher_v_oss,
                ["student_num", "score", "email", "tel"],
            ),
            "lesson": lesson_v_mars,
        },
    )
