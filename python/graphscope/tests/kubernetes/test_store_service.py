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
import subprocess

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


def demo(gs_conn, restart):
    graph = gs_conn.g()
    if not restart:
        # Create schema
        schema = graph.schema()
        schema.add_vertex_label("person").add_primary_key("id", "long").add_property(
            "name", "str"
        )
        schema.add_edge_label("knows").source("person").destination(
            "person"
        ).add_property("date", "str")
        schema.update()
        # Bulk load data
        load_script = os.environ["LOAD_DATA_SCRIPT"]
        subprocess.check_call([load_script])

    interactive = gs_conn.gremlin()
    assert interactive.V().count().toList()[0] == 903
    assert interactive.E().count().toList()[0] == 6626

    # Realtime write
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
    v_update = [v_src[0], {"name": "ci_person_99999_updated"}]
    graph.insert_vertex(*v_src)
    graph.insert_vertex(*v_dst)
    graph.insert_vertices(v_srcs)
    snapshot_id = graph.insert_vertices(v_dsts)

    assert gs_conn.remote_flush(snapshot_id, timeout_ms=5000)

    assert interactive.V().count().toList()[0] == 925
    snapshot_id = graph.update_vertex_properties(*v_update)

    assert gs_conn.remote_flush(snapshot_id)

    assert (
        interactive.V().has("id", v_src[0].primary_key["id"]).values("name").toList()[0]
        == "ci_person_99999_updated"
    )

    edge = [EdgeRecordKey("knows", v_src[0], v_dst[0]), {"date": "ci_edge_2000"}]
    edges = [
        [EdgeRecordKey("knows", src[0], dst[0]), {"date": "ci_edge_3000"}]
        for src, dst in zip(v_srcs, v_dsts)
    ]
    edge_update = [edge[0], {"date": "ci_edge_4000"}]
    snapshot_id = graph.insert_edge(*edge)

    assert gs_conn.remote_flush(snapshot_id)

    edge[0].eid = (
        interactive.V()
        .has("id", edge[0].src_vertex_key.primary_key["id"])
        .outE()
        .toList()[0]
        .id
    )
    snapshot_id = graph.insert_edges(edges)

    assert gs_conn.remote_flush(snapshot_id)

    assert interactive.E().count().toList()[0] == 6637

    for e in edges:
        e[0].eid = (
            interactive.V()
            .has("id", e[0].src_vertex_key.primary_key["id"])
            .outE()
            .toList()[0]
            .id
        )
    snapshot_id = graph.update_edge_properties(*edge_update)

    assert gs_conn.remote_flush(snapshot_id)

    assert (
        interactive.V()
        .has("id", edge[0].src_vertex_key.primary_key["id"])
        .outE()
        .values("date")
        .toList()[0]
        == "ci_edge_4000"
    )

    graph.delete_edge(edge[0])
    graph.delete_edges([e[0] for e in edges])

    graph.delete_vertex(v_src[0])
    graph.delete_vertex(v_dst[0])
    graph.delete_vertices([key[0] for key in v_srcs])
    snapshot_id = graph.delete_vertices([key[0] for key in v_dsts])

    assert gs_conn.remote_flush(snapshot_id)

    assert interactive.V().count().toList()[0] == 903
    assert interactive.E().count().toList()[0] == 6626


def test_demo_fresh(gs_conn):
    demo(gs_conn, False)


def test_demo_after_restart(gs_conn):
    demo(gs_conn, True)
