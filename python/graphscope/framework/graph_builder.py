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

from typing import Mapping
from typing import Sequence
from typing import Union

try:
    import vineyard
except ImportError:
    vineyard = None

from graphscope.client.session import get_default_session
from graphscope.framework import dag_utils
from graphscope.framework import utils
from graphscope.framework.graph import Graph
from graphscope.framework.graph_utils import LoaderVariants
from graphscope.framework.graph_utils import VineyardObjectTypes
from graphscope.framework.graph_utils import normalize_parameter_edges
from graphscope.framework.graph_utils import normalize_parameter_vertices
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2

__all__ = ["load_from"]


def load_from(
    edges: Union[
        Mapping[str, Union[LoaderVariants, Sequence, Mapping]], LoaderVariants, Sequence
    ],
    vertices: Union[
        Mapping[str, Union[LoaderVariants, Sequence, Mapping]],
        LoaderVariants,
        Sequence,
        None,
    ] = None,
    directed=True,
    oid_type="int64_t",
    generate_eid=True,
) -> Graph:
    """Load a Arrow property graph using a list of vertex/edge specifications.

    .. deprecated:: version 0.3
       Use :class:`graphscope.Graph()` instead.

    - Use Dict of tuples to setup a graph.
        We can use a dict to set vertex and edge configurations,
        which can be used to build graphs.

        Examples:

        .. code:: ipython

            g = graphscope_session.load_from(
                edges={
                    "group": [
                        (
                            "file:///home/admin/group.e",
                            ["group_id", "member_size"],
                            ("leader_student_id", "student"),
                            ("member_student_id", "student"),
                        ),
                        (
                            "file:///home/admin/group_for_teacher_student.e",
                            ["group_id", "group_name", "establish_date"],
                            ("teacher_in_charge_id", "teacher"),
                            ("member_student_id", "student"),
                        ),
                    ]
                },
                vertices={
                    "student": (
                        "file:///home/admin/student.v",
                        ["name", "lesson_nums", "avg_score"],
                        "student_id",
                    ),
                    "teacher": (
                        "file:///home/admin/teacher.v",
                        ["name", "salary", "age"],
                        "teacher_id",
                    ),
                },
            )

        'e' is the label of edges, and 'v' is the label for vertices, edges are stored in the 'both_in_out' format
        edges with label 'e' linking from 'v' to 'v'.

    - Use Dict of dict to setup a graph.
        We can also give each element inside the tuple a meaningful name,
        makes it more understandable.

        Examples:

        .. code:: ipython

            g = graphscope_session.load_from(
                edges={
                    "group": [
                        {
                            "loader": "file:///home/admin/group.e",
                            "properties": ["group_id", "member_size"],
                            "source": ("leader_student_id", "student"),
                            "destination": ("member_student_id", "student"),
                        },
                        {
                            "loader": "file:///home/admin/group_for_teacher_student.e",
                            "properties": ["group_id", "group_name", "establish_date"],
                            "source": ("teacher_in_charge_id", "teacher"),
                            "destination": ("member_student_id", "student"),
                        },
                    ]
                },
                vertices={
                    "student": {
                        "loader": "file:///home/admin/student.v",
                        "properties": ["name", "lesson_nums", "avg_score"],
                        "vid": "student_id",
                    },
                    "teacher": {
                        "loader": "file:///home/admin/teacher.v",
                        "properties": ["name", "salary", "age"],
                        "vid": "teacher_id",
                    },
                },
            )

    Args:
        edges: Edge configuration of the graph
        vertices (optional): Vertices configurations of the graph. Defaults to None.
            If None, we assume all edge's src_label and dst_label are deduced and unambiguous.
        directed (bool, optional): Indicate whether the graph
            should be treated as directed or undirected.
        oid_type (str, optional): ID type of graph. Can be "int64_t" or "string". Defaults to "int64_t".
        generate_eid (bool, optional): Whether to generate a unique edge id for each edge. Generated eid will be placed
            in third column. This feature is for cooperating with interactive engine.
            If you only need to work with analytical engine, set it to False. Defaults to False.
    """

    # Don't import the :code:`nx` in top-level statments to improve the
    # performance of :code:`import graphscope`.
    from graphscope import nx

    sess = get_default_session()
    if isinstance(edges, (Graph, nx.Graph, *VineyardObjectTypes)):
        return sess.g(edges)
    oid_type = utils.normalize_data_type_str(oid_type)
    if oid_type not in ("int64_t", "std::string"):
        raise ValueError("oid_type can only be int64_t or string.")
    v_labels = normalize_parameter_vertices(vertices, oid_type)
    e_labels = normalize_parameter_edges(edges, oid_type)
    # generate and add a loader op to dag
    loader_op = dag_utils.create_loader(v_labels + e_labels)
    sess.dag.add_op(loader_op)
    # construct create graph op
    config = {
        types_pb2.DIRECTED: utils.b_to_attr(directed),
        types_pb2.OID_TYPE: utils.s_to_attr(oid_type),
        types_pb2.GENERATE_EID: utils.b_to_attr(generate_eid),
        types_pb2.VID_TYPE: utils.s_to_attr("uint64_t"),
        types_pb2.IS_FROM_VINEYARD_ID: utils.b_to_attr(False),
    }
    op = dag_utils.create_graph(
        sess.session_id, graph_def_pb2.ARROW_PROPERTY, inputs=[loader_op], attrs=config
    )
    graph = sess.g(op)
    return graph
