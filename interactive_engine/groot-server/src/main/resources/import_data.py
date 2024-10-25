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
import argparse

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
    schema.add_vertex_label("person").add_primary_key("id", "int").add_property(
        "name", "str"
    ).add_property("age", "int")
    schema.add_vertex_label("software").add_primary_key("id", "int").add_property(
        "name", "str"
    ).add_property("lang", "str")
    schema.add_edge_label("knows").source("person").destination("person").add_property(
        "edge_id", "int"
    ).add_property("weight", "double")
    schema.add_edge_label("created").source("person").destination(
        "software"
    ).add_property("edge_id", "int").add_property("weight", "double")
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

def create_ldbc_graph_schema(graph):
    schema = graph.schema()
    schema.add_vertex_label('PLACE').add_primary_key('id', 'long').add_property('name', 'str').add_property('url', 'str').add_property('type', 'str')
    schema.add_vertex_label('PERSON').add_primary_key('id', 'long').add_property('firstName', 'str').add_property('lastName', 'str').add_property('gender', 'str').add_property('birthday', 'long').add_property('creationDate', 'long').add_property('locationIP', 'str').add_property('browserUsed', 'str').add_property('language', 'str').add_property('email', 'str')
    schema.add_vertex_label('COMMENT').add_primary_key('id', 'long').add_property('creationDate', 'long').add_property('locationIP', 'str').add_property('browserUsed', 'str').add_property('content', 'str').add_property('length','int')
    schema.add_vertex_label('POST').add_primary_key('id', 'long').add_property('imageFile', 'str').add_property('creationDate', 'long').add_property('locationIP', 'str').add_property('browserUsed', 'str').add_property('language', 'str').add_property('content', 'str').add_property('length', 'int')
    schema.add_vertex_label('FORUM').add_primary_key('id', 'long').add_property('title', 'str').add_property('creationDate', 'str')
    schema.add_vertex_label('ORGANISATION').add_primary_key('id', 'long').add_property('type', 'str').add_property('name', 'str').add_property('url', 'str')
    schema.add_vertex_label('TAGCLASS').add_primary_key('id', 'long').add_property('name', 'str').add_property('url', 'str')
    schema.add_vertex_label('TAG').add_primary_key('id', 'long').add_property('name', 'str').add_property('url', 'str')
    schema.add_edge_label('HASCREATOR').source('COMMENT').destination('PERSON').source('POST').destination('PERSON')
    schema.add_edge_label('HASTAG').source('COMMENT').destination('TAG').source('POST').destination('TAG').source('FORUM').destination('TAG')
    schema.add_edge_label('ISLOCATEDIN').source('COMMENT').destination('PLACE').source('POST').destination('PLACE').source('PERSON').destination('PLACE').source('ORGANISATION').destination('PLACE')
    schema.add_edge_label('REPLYOF').source('COMMENT').destination('COMMENT').source('COMMENT').destination('POST')
    schema.add_edge_label('CONTAINEROF').source('FORUM').destination('POST')
    schema.add_edge_label('HASMEMBER').source('FORUM').destination('PERSON').add_property('joinDate','long')
    schema.add_edge_label('HASMODERATOR').source('FORUM').destination('PERSON')
    schema.add_edge_label('HASINTEREST').source('PERSON').destination('TAG')
    schema.add_edge_label('KNOWS').source('PERSON').destination('PERSON').add_property('creationDate','long')
    schema.add_edge_label('LIKES').source('PERSON').destination('COMMENT').source('PERSON').destination('POST').add_property('creationDate','long')
    schema.add_edge_label('STUDYAT').source('PERSON').destination('ORGANISATION').add_property('classYear','long')
    schema.add_edge_label('WORKAT').source('PERSON').destination('ORGANISATION').add_property('workFrom','long')
    schema.add_edge_label('ISPARTOF').source('PLACE').destination('PLACE')
    schema.add_edge_label('ISSUBCLASSOF').source('TAGCLASS').destination('TAGCLASS')
    schema.add_edge_label('HASTYPE').source('TAG').destination('TAGCLASS')
    schema.update()

def create_movie_graph_schema(graph):
    schema = graph.schema()
    schema.add_vertex_label('Movie').add_primary_key('id', 'int').add_property('released', 'int').add_property('tagline', 'str').add_property('title', 'str')
    schema.add_vertex_label('Person').add_primary_key('id', 'int').add_property('born', 'int').add_property('name', 'str')
    schema.add_vertex_label('User').add_primary_key('id', 'int').add_property('born', 'int').add_property('name', 'str')
    schema.add_edge_label('ACTED_IN').source('Person').destination('Movie')
    schema.add_edge_label('DIRECTED').source('Person').destination('Movie')
    schema.add_edge_label('REVIEW').source('Person').destination('Movie').add_property('rating', 'int')
    schema.add_edge_label('FOLLOWS').source('User').destination('Person')
    schema.add_edge_label('WROTE').source('Person').destination('Movie')
    schema.add_edge_label('PRODUCED').source('Person').destination('Movie')
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

def batch_insert_vertices(conn, graph, vertices, batch_size=10000):
    for i in range(0, len(vertices), batch_size):
        batch = vertices[i:i + batch_size]
        snapshot_id = graph.insert_vertices(batch)
        assert conn.remote_flush(snapshot_id, timeout_ms=5000)

def batch_insert_edges(conn, graph, edges, batch_size=10000):
    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        snapshot_id = graph.insert_edges(batch)
        assert conn.remote_flush(snapshot_id, timeout_ms=5000)


def prepare_vertices(vertices, data, vertex_type, properties):
    vertices.extend(
        [
            [VertexRecordKey(vertex_type, {"id": v[0]}), {prop: v[i + 1] for i, prop in enumerate(properties)}]
            for v in data.itertuples(index=False)
        ]
    )

def prepare_edges(edges, data, edge_type, source_type, destination_type, properties):
    edges.extend(
        [
            EdgeRecordKey(edge_type,
                          VertexRecordKey(source_type, {"id": e[0]}),
                          VertexRecordKey(destination_type, {"id": e[1]})),
            {
                prop: e[i+2] for i, prop in enumerate(properties)
            }
        ]
        for e in data.itertuples(index=False)
    )


def load_data_of_ldbc_graph(conn, graph, prefix):
    place = pd.read_csv(os.path.join(prefix, "place_0_0.csv"), sep="|")
    person = pd.read_csv(os.path.join(prefix, "person_0_0.csv"), sep="|")
    comment = pd.read_csv(os.path.join(prefix, "comment_0_0.csv"), sep="|")
    post = pd.read_csv(os.path.join(prefix, "post_0_0.csv"), sep="|")
    forum = pd.read_csv(os.path.join(prefix, "forum_0_0.csv"), sep="|")
    organisation = pd.read_csv(os.path.join(prefix, "organisation_0_0.csv"), sep="|")
    tagclass = pd.read_csv(os.path.join(prefix, "tagclass_0_0.csv"), sep="|")
    tag = pd.read_csv(os.path.join(prefix, "tag_0_0.csv"), sep="|")
    comment_hascreator = pd.read_csv(os.path.join(prefix, "comment_hasCreator_person_0_0.csv"), sep="|")
    post_hascreator = pd.read_csv(os.path.join(prefix, "post_hasCreator_person_0_0.csv"), sep="|")
    comment_hastag = pd.read_csv(os.path.join(prefix, "comment_hasTag_tag_0_0.csv"), sep="|")
    post_hastag = pd.read_csv(os.path.join(prefix, "post_hasTag_tag_0_0.csv"), sep="|")
    forum_hastag = pd.read_csv(os.path.join(prefix, "forum_hasTag_tag_0_0.csv"), sep="|")
    comment_islocatedin = pd.read_csv(os.path.join(prefix, "comment_isLocatedIn_place_0_0.csv"), sep="|")
    post_islocatedin = pd.read_csv(os.path.join(prefix, "post_isLocatedIn_place_0_0.csv"), sep="|")
    person_islocatedin = pd.read_csv(os.path.join(prefix, "person_isLocatedIn_place_0_0.csv"), sep="|")
    organisation_islocatedin = pd.read_csv(os.path.join(prefix, "organisation_isLocatedIn_place_0_0.csv"), sep="|")
    comment_replyof_comment = pd.read_csv(os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), sep="|")
    comment_replyof_post = pd.read_csv(os.path.join(prefix, "comment_replyOf_post_0_0.csv"), sep="|")
    forum_containerof_post = pd.read_csv(os.path.join(prefix, "forum_containerOf_post_0_0.csv"), sep="|")
    forum_hasmember_person = pd.read_csv(os.path.join(prefix, "forum_hasMember_person_0_0.csv"), sep="|")
    forum_hasmoderator_person = pd.read_csv(os.path.join(prefix, "forum_hasModerator_person_0_0.csv"), sep="|")
    person_hasinterest_tag = pd.read_csv(os.path.join(prefix, "person_hasInterest_tag_0_0.csv"), sep="|")
    person_knows_person = pd.read_csv(os.path.join(prefix, "person_knows_person_0_0.csv"), sep="|")
    person_likes_comment = pd.read_csv(os.path.join(prefix, "person_likes_comment_0_0.csv"), sep="|")
    person_likes_post = pd.read_csv(os.path.join(prefix, "person_likes_post_0_0.csv"), sep="|")
    person_studyat_organisation = pd.read_csv(os.path.join(prefix, "person_studyAt_organisation_0_0.csv"), sep="|")
    person_workat_organisation = pd.read_csv(os.path.join(prefix, "person_workAt_organisation_0_0.csv"), sep="|")
    place_ispartof_place = pd.read_csv(os.path.join(prefix, "place_isPartOf_place_0_0.csv"), sep="|")
    tagclass_isSubclassOf_tagclass = pd.read_csv(os.path.join(prefix, "tagclass_isSubclassOf_tagclass_0_0.csv"), sep="|")
    tag_hastype_tagclass = pd.read_csv(os.path.join(prefix, "tag_hasType_tagclass_0_0.csv"), sep="|")
    vertices = []
    prepare_vertices(vertices, place, "PLACE", ["name", "url", "type"])
    prepare_vertices(vertices, person, "PERSON", ["firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed", "language", "email"])
    prepare_vertices(vertices, comment, "COMMENT", ["creationDate", "locationIP", "browserUsed", "content", "length"])
    prepare_vertices(vertices, post, "POST", ["imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length"])
    prepare_vertices(vertices, forum, "FORUM", ["title", "creationDate"])
    prepare_vertices(vertices, organisation, "ORGANISATION", ["type", "name", "url"])
    prepare_vertices(vertices, tagclass, "TAGCLASS", ["name", "url"])
    prepare_vertices(vertices, tag, "TAG", ["name", "url"])
    edges = []
    prepare_edges(edges, comment_hascreator, "HASCREATOR", "COMMENT", "PERSON", [])
    prepare_edges(edges, post_hascreator, "HASCREATOR", "POST", "PERSON", [])
    prepare_edges(edges, comment_hastag, "HASTAG", "COMMENT", "TAG", [])
    prepare_edges(edges, post_hastag, "HASTAG", "POST", "TAG", [])
    prepare_edges(edges, forum_hastag, "HASTAG", "FORUM", "TAG", [])
    prepare_edges(edges, comment_islocatedin, "ISLOCATEDIN", "COMMENT", "PLACE", [])
    prepare_edges(edges, post_islocatedin, "ISLOCATEDIN", "POST", "PLACE", [])
    prepare_edges(edges, person_islocatedin, "ISLOCATEDIN", "PERSON", "PLACE", [])
    prepare_edges(edges, organisation_islocatedin, "ISLOCATEDIN", "ORGANISATION", "PLACE", [])
    prepare_edges(edges, comment_replyof_comment, "REPLYOF", "COMMENT", "COMMENT", [])
    prepare_edges(edges, comment_replyof_post, "REPLYOF", "COMMENT", "POST", [])
    prepare_edges(edges, forum_containerof_post, "CONTAINEROF", "FORUM", "POST", [])
    prepare_edges(edges, forum_hasmember_person, "HASMEMBER", "FORUM", "PERSON", ["joinDate"])
    prepare_edges(edges, forum_hasmoderator_person, "HASMODERATOR", "FORUM", "PERSON", [])
    prepare_edges(edges, person_hasinterest_tag, "HASINTEREST", "PERSON", "TAG", [])
    prepare_edges(edges, person_knows_person, "KNOWS", "PERSON", "PERSON", ["creationDate"])
    prepare_edges(edges, person_likes_comment, "LIKES", "PERSON", "COMMENT", ["creationDate"])
    prepare_edges(edges, person_likes_post, "LIKES", "PERSON", "POST", ["creationDate"])
    prepare_edges(edges, person_studyat_organisation, "STUDYAT", "PERSON", "ORGANISATION", ["classYear"])
    prepare_edges(edges, person_workat_organisation, "WORKAT", "PERSON", "ORGANISATION", ["workFrom"])
    prepare_edges(edges, place_ispartof_place, "ISPARTOF", "PLACE", "PLACE", [])
    prepare_edges(edges, tagclass_isSubclassOf_tagclass, "ISSUBCLASSOF", "TAGCLASS", "TAGCLASS", [])
    prepare_edges(edges, tag_hastype_tagclass, "HASTYPE", "TAG", "TAGCLASS", [])

    batch_insert_vertices(conn, graph, vertices)
    batch_insert_edges(conn, graph, edges)
    print("load ldbc graph done")

def load_data_of_movie_graph(conn, graph, prefix):
    movie = pd.read_csv(os.path.join(prefix, "Movie.csv"), sep="|")
    person = pd.read_csv(os.path.join(prefix, "Person.csv"), sep="|")
    user = pd.read_csv(os.path.join(prefix, "User.csv"), sep="|")
    acted_in = pd.read_csv(os.path.join(prefix, "ACTED_IN.csv"), sep="|")
    directed = pd.read_csv(os.path.join(prefix, "DIRECTED.csv"), sep="|")
    review = pd.read_csv(os.path.join(prefix, "REVIEWED.csv"), sep="|")
    follows = pd.read_csv(os.path.join(prefix, "FOLLOWS.csv"), sep="|")
    wrote = pd.read_csv(os.path.join(prefix, "WROTE.csv"), sep="|")
    produced = pd.read_csv(os.path.join(prefix, "PRODUCED.csv"), sep="|")
    vertices = []
    prepare_vertices(vertices, movie, "Movie", ["released", "tagline", "title"])
    prepare_vertices(vertices, person, "Person", ["born", "name"])
    prepare_vertices(vertices, user, "User", ["born", "name"])
    edges = []
    prepare_edges(edges, acted_in, "ACTED_IN", "Person", "Movie", [])
    prepare_edges(edges, directed, "DIRECTED", "Person", "Movie", [])
    prepare_edges(edges, review, "REVIEW", "Person", "Movie", ["rating"])
    prepare_edges(edges, follows, "FOLLOWS", "User", "Person", [])
    prepare_edges(edges, wrote, "WROTE", "Person", "Movie", [])
    prepare_edges(edges, produced, "PRODUCED", "Person", "Movie", [])
    snapshot_id = graph.insert_vertices(vertices)
    snapshot_id = graph.insert_edges(edges)
    assert conn.remote_flush(snapshot_id, timeout_ms=5000)
    print("load movie graph done")


def create_modern_graph(conn, graph, client, data_path):
    print("create modern graph", data_path)
    create_modern_graph_schema(graph)
    load_data_of_modern_graph(conn, graph, data_path)
    statistics(client)


def create_crew_graph(conn, graph, client, data_path):
    create_crew_graph_schema(graph)
    load_data_of_crew_graph(conn, graph, data_path)
    statistics(client)

def create_ldbc_graph(conn, graph, client, data_path):
    create_ldbc_graph_schema(graph)
    load_data_of_ldbc_graph(conn, graph, data_path)
    statistics(client)

def create_movie_graph(conn, graph, client, data_path):
    create_movie_graph_schema(graph)
    load_data_of_movie_graph(conn, graph, data_path)
    statistics(client)

def main():
    client = get_client()
    conn = get_conn()
    graph = conn.g()
    
    parser = argparse.ArgumentParser(description="Import specific graph data.")
    parser.add_argument(
        '--graph', 
        choices=['modern', 'crew', 'ldbc', 'movie'],
        required=True,
        help="The graph to import: 'modern', 'crew', 'ldbc', or 'movie'."
    )
    parser.add_argument(
        '--data_path',
        required=True,
        help="The path to the input data file."
    )

    args = parser.parse_args()

    if args.graph == 'modern':
        create_modern_graph(conn, graph, client, args.data_path)
    elif args.graph == 'crew':
        create_crew_graph(conn, graph, client, args.data_path)
    elif args.graph == 'ldbc':
        create_ldbc_graph(conn, graph, client, args.data_path)
    elif args.graph == 'movie':
        create_movie_graph(conn, graph, client, args.data_path)

if __name__ == "__main__":
    main()
