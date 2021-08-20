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

import logging
import os

import numpy as np
import pytest

import graphscope
from graphscope.client.connection import Graph
from graphscope.framework.record import EdgeRecordKey
from graphscope.framework.record import VertexRecordKey

logger = logging.getLogger("graphscope")


@pytest.fixture(scope="module")
def gs_conn():
    node_ip = os.environ["NODE_IP"]
    grpc_port = os.environ["GRPC_PORT"]
    gremlin_port = os.environ["GREMLIN_PORT"]
    grpc_endpoint = f"{node_ip}:{grpc_port}"
    gremlin_endpoint = f"{node_ip}:{gremlin_port}"
    yield graphscope.conn(grpc_endpoint, gremlin_endpoint)


def test_demo(gs_conn):
    graph = gs_conn.g()
    schema = graph.schema()
    schema.add_vertex_label("person").add_primary_key("id", "long").add_property(
        "name", "str"
    )
    schema.add_edge_label("knows").source("person").destination("person").add_property(
        "date", "str"
    )
    schema.update()
    load_script = os.environ["LOAD_DATA_SCRIPT"]
    os.system(load_script)

    interactive = gs_conn.gremlin()
    # TODO(zsy, tianli): Use more compilcated queries
    assert interactive.V().count().toList()[0] == 903
    assert interactive.E().count().toList()[0] == 6626


def test_realtime_write(gs_conn):
    graph: Graph = gs_conn.g()
    v_src = [VertexRecordKey("person", {"id": 99999}), {"name": "ci_person_99999"}]
    v_dst = [VertexRecordKey("person", {"id": 199999}), {"name": "ci_person_199999"}]
    v_srcs = [
        [
            VertexRecordKey("person", {"id": 100000 + i}),
            {"name": f"ci_person_{100000 + i}"},
        ]
        for i in range(10)
    ]
    v_dsts = [
        [
            VertexRecordKey("person", {"id": 200000 + i}),
            {"name": f"ci_person_{200000 + i}"},
        ]
        for i in range(10)
    ]
    v_update = [
        VertexRecordKey("person", {"id": 99999}),
        {"name": "ci_person_99999_updated"},
    ]
    graph.insert_vertex(*v_src)
    graph.insert_vertex(*v_dst)
    graph.insert_vertices(v_srcs)
    graph.insert_vertices(v_dsts)
    graph.update_vertex_properties(*v_update)

    edge = [EdgeRecordKey("knows", v_src, v_dst), {"date": "ci_edge_2000"}]
    edges = [
        [EdgeRecordKey("knows", src, dst), {"date": "ci_edge_2000"}]
        for src, dst in zip(v_srcs, v_dsts)
    ]
    edge_update = [EdgeRecordKey("knows", v_src, v_dst), {"date": "ci_edge_3000"}]
    graph.insert_edge(edge)
    graph.insert_edges(edges)
    graph.update_edge_properties(edge_update)
    graph.delete_edge(edge)
    graph.delete_edges(edges)
    graph.delete_vertex(v_src[0])
    graph.delete_vertex(v_dst[0])
    graph.delete_vertices([key[0] for key in v_srcs])
    graph.delete_vertices([key[0] for key in v_dsts])
