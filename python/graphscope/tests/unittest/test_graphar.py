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

import pytest

from graphscope import pagerank
from graphscope.framework.graph import Graph

logger = logging.getLogger("graphscope")
graphar_temp_dir = (
    os.path.expandvars("${TMPDIR}")
    if os.path.expandvars("${TMPDIR}").endswith(os.sep)
    else os.path.expandvars("${TMPDIR}") + os.sep
)


def test_save_full_ldbc_to_graphar_and_load_back(graphscope_session, ldbc_graph):
    output_dir = graphar_temp_dir + "graphar" + os.sep
    r = ldbc_graph.save_to(
        output_dir,
        format="graphar",
        graphar_graph_name="ldbc_sample",
        graphar_file_type="parquet",
        graphar_vertex_chunk_size=1000000,
        graphar_edge_chunk_size=1000000,
    )
    assert r == {
        "type": "graphar",
        "URI": "graphar+file://{}ldbc_sample.graph.yaml".format(output_dir),
    }
    g = Graph.load_from(r["URI"], sess=graphscope_session)
    assert g.schema.to_dict() == ldbc_graph.schema.to_dict()

    # do some graph processing
    pg = g.project(vertices={"person": ["id"]}, edges={"knows": []})
    ctx = pagerank(pg, max_round=10)
    df = ctx.to_dataframe(selector={"id": "v.data", "r": "r"})
    del g


def test_save_to_graphar_with_selector_and_load_back_1(graphscope_session, ldbc_graph):
    output_dir = graphar_temp_dir + "graphar_subgraph" + os.sep
    selector = {
        "vertices": {
            "person": ["id", "firstName", "lastName"],
            "comment": ["content", "creationDate"],
        },
        "edges": {
            "knows": ["creationDate"],
            "likes": ["creationDate"],
        },
    }

    r = ldbc_graph.save_to(
        "file://" + output_dir,
        format="graphar",
        selector=selector,
        graphar_graph_name="ldbc_sample",
        graphar_file_type="parquet",
        graphar_vertex_chunk_size=256,
        graphar_edge_chunk_size=1024,
    )
    assert r == {
        "type": "graphar",
        "URI": "graphar+file://{}ldbc_sample.graph.yaml".format(output_dir),
    }
    g = Graph.load_from(r["URI"], sess=graphscope_session)
    assert g.schema.vertex_label_num == 2 and g.schema.edge_label_num == 2
    assert "person" in g.schema.vertex_labels and "comment" in g.schema.vertex_labels
    assert "knows" in g.schema.edge_labels and "likes" in g.schema.edge_labels
    assert (
        g.schema.vertex_properties_num("person") == 3
        and g.schema.vertex_properties_num("comment") == 2
    )
    assert (
        g.schema.edge_properties_num("knows") == 1
        and g.schema.edge_properties_num("likes") == 1
    )

    # do some graph processing
    pg = g.project(vertices={"person": ["id"]}, edges={"knows": []})
    ctx = pagerank(pg, max_round=10)
    df = ctx.to_dataframe(selector={"id": "v.data", "r": "r"})
    del g


def test_save_to_graphar_with_selector_and_load_back_2(graphscope_session, ldbc_graph):
    output_dir = graphar_temp_dir + "graphar_subgraph2" + os.sep
    selector = {
        "vertices": {
            "person": ["id", "firstName", "lastName"],
            "comment": ["content", "creationDate"],
        },
        "edges": {
            "knows": [],
            "likes": [],
        },
    }

    r = ldbc_graph.save_to(
        output_dir,
        format="graphar",
        selector=selector,
        graphar_graph_name="ldbc_sample",
        graphar_file_type="parquet",
        graphar_vertex_chunk_size=256,
        graphar_edge_chunk_size=1024,
    )
    assert r == {
        "type": "graphar",
        "URI": "graphar+file://{}ldbc_sample.graph.yaml".format(output_dir),
    }
    g = Graph.load_from(r["URI"], sess=graphscope_session)
    assert g.schema.vertex_label_num == 2 and g.schema.edge_label_num == 2
    assert "person" in g.schema.vertex_labels and "comment" in g.schema.vertex_labels
    assert "knows" in g.schema.edge_labels and "likes" in g.schema.edge_labels
    assert (
        g.schema.vertex_properties_num("person") == 3
        and g.schema.vertex_properties_num("comment") == 2
    )
    assert (
        g.schema.edge_properties_num("knows") == 0
        and g.schema.edge_properties_num("likes") == 0
    )

    # do some graph processing
    pg = g.project(vertices={"person": ["id"]}, edges={"knows": []})
    ctx = pagerank(pg, max_round=10)
    df = ctx.to_dataframe(selector={"id": "v.data", "r": "r"})


def test_save_to_graphar_with_selector_and_load_back_3(graphscope_session, ldbc_graph):
    output_dir = graphar_temp_dir + "graphar_subgraph3" + os.sep
    selector = {
        "vertices": {
            "person": ["id", "firstName", "lastName"],
            "comment": None,
        },
        "edges": {
            "knows": None,
            "likes": None,
        },
    }

    r = ldbc_graph.save_to(
        output_dir,
        format="graphar",
        selector=selector,
        graphar_graph_name="ldbc_sample",
        graphar_file_type="parquet",
        graphar_vertex_chunk_size=256,
        graphar_edge_chunk_size=1024,
    )
    assert r == {
        "type": "graphar",
        "URI": "graphar+file://{}ldbc_sample.graph.yaml".format(output_dir),
    }
    g = Graph.load_from(r["URI"], sess=graphscope_session)
    print(g.schema)
    assert g.schema.vertex_label_num == 2 and g.schema.edge_label_num == 2
    assert "person" in g.schema.vertex_labels and "comment" in g.schema.vertex_labels
    assert "knows" in g.schema.edge_labels and "likes" in g.schema.edge_labels
    assert (
        g.schema.vertex_properties_num("person") == 3
        and g.schema.vertex_properties_num("comment") == 6
    )
    assert (
        g.schema.edge_properties_num("knows") == 2
        and g.schema.edge_properties_num("likes") == 2
    )

    # do some graph processing
    pg = g.project(vertices={"person": ["id"]}, edges={"knows": []})
    ctx = pagerank(pg, max_round=10)
    df = ctx.to_dataframe(selector={"id": "v.data", "r": "r"})
    del g


@pytest.mark.dependency(depends=["test_save_full_ldbc_to_graphar_and_load_back"])
def test_load_from_graphar_with_selector(graphscope_session):
    graph_uri = "graphar+file://{}graphar{}ldbc_sample.graph.yaml".format(
        graphar_temp_dir,
        os.sep,
    )
    selector = {
        "vertices": {
            "person": None,
            "comment": None,
        },
        "edges": {
            "knows": None,
            "likes": None,
        },
    }
    g = Graph.load_from(graph_uri, sess=graphscope_session, selector=selector)
    assert g.schema.vertex_label_num == 2 and g.schema.edge_label_num == 2
    assert "person" in g.schema.vertex_labels and "comment" in g.schema.vertex_labels
    assert "knows" in g.schema.edge_labels and "likes" in g.schema.edge_labels
    assert (
        g.schema.vertex_properties_num("person") == 8
        and g.schema.vertex_properties_num("comment") == 6
    )
    assert (
        g.schema.edge_properties_num("knows") == 2
        and g.schema.edge_properties_num("likes") == 2
    )

    # do some graph processing
    pg = g.project(vertices={"person": ["id"]}, edges={"knows": []})
    ctx = pagerank(pg, max_round=10)
    df = ctx.to_dataframe(selector={"id": "v.data", "r": "r"})
    del g
