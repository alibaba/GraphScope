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
import graphscope as gs
from graphscope.framework.record import EdgeRecordKey, VertexRecordKey
from gremlin_python.driver.client import Client


node_ip = os.environ.get("NODE_IP", "127.0.0.1")
grpc_port = os.environ.get("GRPC_PORT", "55556")
gremlin_port = os.environ.get("GREMLIN_PORT", "12312")
grpc_endpoint = f"{node_ip}:{grpc_port}"
gremlin_endpoint = f"{node_ip}:{gremlin_port}"


def get_client():
    graph_url = f"ws://{gremlin_endpoint}/gremlin"
    return Client(graph_url, "g")


def query(client, query_str):
    return client.submit(query_str).all().result()


def statistics(client):
    print("vertices: ", query(client, "g.V().count()"))
    print("edges: ", query(client, "g.E().count()"))


def get_conn():
    return gs.conn(grpc_endpoint, gremlin_endpoint)


def create_modern_graph_schema(graph):
    schema = graph.schema()
    schema.add_vertex_label("person").add_primary_key("id", "long").add_property(
        "name", "str"
    ).add_property("age", "int")
    schema.add_vertex_label("software").add_primary_key("id", "long").add_property(
        "name", "str"
    ).add_property("lang", "str")
    schema.add_edge_label("knows").source("person").destination("person").add_property(
        "edge_id", "long"
    ).add_property("weight", "double")
    schema.add_edge_label("created").source("person").destination(
        "software"
    ).add_property("edge_id", "long").add_property("weight", "double")
    schema.update()


def create_crew_graph_schema(graph):
    schema = graph.schema()
    schema.add_vertex_label("person").add_primary_key("id", "long").add_property(
        "name", "str"
    ).add_property("location", "str")
    schema.add_vertex_label("software").add_primary_key("id", "long").add_property(
        "name", "str"
    )
    schema.add_edge_label("develops").source("person").destination(
        "software"
    ).add_property("id", "long").add_property("since", "int")
    schema.add_edge_label("traverses").source("software").destination(
        "software"
    ).add_property("id", "long")
    schema.add_edge_label("uses").source("person").destination("software").add_property(
        "id", "long"
    ).add_property("skill", "int")
    schema.update()


def load_data_of_modern_graph(conn, graph, prefix):
    person = pd.read_csv(os.path.join(prefix, "person.csv"), sep="|")
    software = pd.read_csv(os.path.join(prefix, "software.csv"), sep="|")
    knows = pd.read_csv(os.path.join(prefix, "knows.csv"), sep="|")
    created = pd.read_csv(os.path.join(prefix, "created.csv"), sep="|")
    vertices = []
    vertices.extend(
        [
            [VertexRecordKey("person", {"id": v[0]}), {"name": v[1], "age": v[2]}]
            for v in person.itertuples(index=False)
        ]
    )
    vertices.extend(
        [
            [VertexRecordKey("software", {"id": v[0]}), {"name": v[1], "lang": v[2]}]
            for v in software.itertuples(index=False)
        ]
    )
    edges = []
    edges.extend(
        [
            [
                EdgeRecordKey(
                    "knows",
                    VertexRecordKey("person", {"id": e[0]}),
                    VertexRecordKey("person", {"id": e[1]}),
                ),
                {"weight": e[2]},
            ]
            for e in knows.itertuples(index=False)
        ]
    )
    edges.extend(
        [
            [
                EdgeRecordKey(
                    "created",
                    VertexRecordKey("person", {"id": e[0]}),
                    VertexRecordKey("software", {"id": e[1]}),
                ),
                {"weight": e[2]},
            ]
            for e in created.itertuples(index=False)
        ]
    )

    snapshot_id = graph.insert_vertices(vertices)
    snapshot_id = graph.insert_edges(edges)
    assert conn.remote_flush(snapshot_id, timeout_ms=5000)
    print("load modern graph done")


def load_data_of_crew_graph(conn, graph, prefix):
    person = pd.read_csv(os.path.join(prefix, "person.dat"), sep=",")
    software = pd.read_csv(os.path.join(prefix, "software.dat"), sep=",")
    develops = pd.read_csv(os.path.join(prefix, "develops_vineyard.dat"), sep=",")
    traverses = pd.read_csv(os.path.join(prefix, "traverses_vineyard.dat"), sep=",")
    uses = pd.read_csv(os.path.join(prefix, "uses_vineyard.dat"), sep=",")
    vertices = []
    vertices.extend(
        [
            [VertexRecordKey("person", {"id": v[0]}), {"name": v[1], "location": v[2]}]
            for v in person.itertuples(index=False)
        ]
    )
    vertices.extend(
        [
            [VertexRecordKey("software", {"id": v[0]}), {"name": v[1]}]
            for v in software.itertuples(index=False)
        ]
    )
    edges = []
    edges.extend(
        [
            [
                EdgeRecordKey(
                    "develops",
                    VertexRecordKey("person", {"id": e[0]}),
                    VertexRecordKey("software", {"id": e[1]}),
                ),
                {"id": e[2], "since": e[3]},
            ]
            for e in develops.itertuples(index=False)
        ]
    )
    edges.extend(
        [
            [
                EdgeRecordKey(
                    "traverses",
                    VertexRecordKey("software", {"id": e[0]}),
                    VertexRecordKey("software", {"id": e[1]}),
                ),
                {"id": e[2]},
            ]
            for e in traverses.itertuples(index=False)
        ]
    )
    edges.extend(
        [
            [
                EdgeRecordKey(
                    "uses",
                    VertexRecordKey("person", {"id": e[0]}),
                    VertexRecordKey("software", {"id": e[1]}),
                ),
                {"id": e[2], "skill": e[3]},
            ]
            for e in uses.itertuples(index=False)
        ]
    )

    snapshot_id = graph.insert_vertices(vertices)
    snapshot_id = graph.insert_edges(edges)
    assert conn.remote_flush(snapshot_id, timeout_ms=5000)
    print("load crew graph done")


def create_modern_graph(conn, graph, client):
    create_modern_graph_schema(graph)
    load_data_of_modern_graph(conn, graph, "/home/graphscope/modern_graph")
    statistics(client)


def create_crew_graph(conn, graph, client):
    create_crew_graph_schema(graph)
    load_data_of_crew_graph(conn, graph, "/home/graphscope/crew")
    statistics(client)


if __name__ == "__main__":
    client = get_client()
    conn = get_conn()
    graph = conn.g()
    create_modern_graph(conn, graph, client)
    # create_crew_graph(conn, graph, client)
