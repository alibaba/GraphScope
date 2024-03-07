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

import pytest

from graphscope.framework.graph import Graph

graphar_test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")
graphar_temp_dir = os.path.expandvars("${TMPDIR}")


def test_save_full_ldbc_to_graphar_and_load_back(ldbc_graph, graphscope_session):
    output_dir = graphar_temp_dir + "graphar/"
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
    g = Graph.load_from(r["URI"], graphscope_session)
    assert g.schema.to_dict() == ldbc_graph.schema.to_dict()
    del g


def test_save_to_graphar_with_selector_and_load_back_1(ldbc_graph):
    output_dir = graphar_temp_dir + "graphar_subgraph/"
    selector = {
        "vertices": {
            "person": ["id", "firstName", "lastName"],
            "organisation": ["name", "type"],
        },
        "edges": {
            "studyAt": ["classYear"],
            "workAt": ["workFrom"],
        },
    }

    r = ldbc_graph.save_to(
        "file://" + output_dir,
        format="graphar",
        selector=selector,
        graphar_graph_name="ldbc_sample",
        graphar_file_type="orc",
        graphar_vertex_chunk_size=256,
        graphar_edge_chunk_size=1024,
    )
    assert r == {
        "type": "graphar",
        "URI": "graphar+file://{}ldbc_sample.graph.yaml".format(output_dir),
    }
    g = Graph.load_from(r["URI"])
    assert g.schema.vertex_label_num == 2 and g.schema.edge_label_num == 2
    assert (
        "person" in g.schema.vertex_labels and "organisation" in g.schema.vertex_labels
    )
    assert "studyAt" in g.schema.edge_labels and "workAt" in g.schema.edge_labels
    assert (
        g.schema.vertex_properties_num("person") == 3
        and g.schema.vertex_properties_num("organisation") == 2
    )
    assert (
        g.schema.edge_properties_num("studyAt") == 1
        and g.schema.edge_properties_num("workAt") == 1
    )
    del g


def test_save_to_graphar_with_selector_and_load_back_2(ldbc_graph):
    output_dir = graphar_temp_dir + "graphar_subgraph2/"
    selector = {
        "vertices": {
            "person": ["id", "firstName", "lastName"],
            "organisation": ["name", "type"],
        },
        "edges": {
            "studyAt": [],
            "workAt": [],
        },
    }

    r = ldbc_graph.save_to(
        output_dir,
        format="graphar",
        selector=selector,
        graphar_graph_name="ldbc_sample",
        graphar_file_type="orc",
        graphar_vertex_chunk_size=256,
        graphar_edge_chunk_size=1024,
    )
    assert r == {
        "type": "graphar",
        "URI": "graphar+file://{}ldbc_sample.graph.yaml".format(output_dir),
    }
    g = Graph.load_from(r["URI"])
    assert g.schema.vertex_label_num == 2 and g.schema.edge_label_num == 2
    assert (
        "person" in g.schema.vertex_labels and "organisation" in g.schema.vertex_labels
    )
    assert "studyAt" in g.schema.edge_labels and "workAt" in g.schema.edge_labels
    assert (
        g.schema.vertex_properties_num("person") == 3
        and g.schema.vertex_properties_num("organisation") == 2
    )
    assert (
        g.schema.edge_properties_num("studyAt") == 0
        and g.schema.edge_properties_num("workAt") == 0
    )
    del g


def test_save_to_graphar_with_selector_and_load_back_3(ldbc_graph):
    output_dir = graphar_temp_dir + "graphar_subgraph3/"
    selector = {
        "vertices": {
            "person": ["id", "firstName", "lastName"],
            "organisation": None,
        },
        "edges": {
            "studyAt": None,
            "workAt": None,
        },
    }

    r = ldbc_graph.save_to(
        output_dir,
        format="graphar",
        selector=selector,
        graphar_graph_name="ldbc_sample",
        graphar_file_type="orc",
        graphar_vertex_chunk_size=256,
        graphar_edge_chunk_size=1024,
    )
    assert r == {
        "type": "graphar",
        "URI": "graphar+file://${}ldbc_sample.graph.yaml".format(output_dir),
    }
    g = Graph.load_from(r["URI"])
    assert g.schema.vertex_label_num == 2 and g.schema.edge_label_num == 2
    assert (
        "person" in g.schema.vertex_labels and "organisation" in g.schema.vertex_labels
    )
    assert "studyAt" in g.schema.edge_labels and "workAt" in g.schema.edge_labels
    assert (
        g.schema.vertex_properties_num("person") == 3
        and g.schema.vertex_properties_num("organisation") == 4
    )
    assert (
        g.schema.edge_properties_num("studyAt") == 2
        and g.schema.edge_properties_num("workAt") == 2
    )
    del g


@pytest.mark.dependency(depends=["test_save_full_ldbc_to_graphar_and_load_back"])
def test_load_from_graphar_with_selector(graphscope_session):
    graph_uri = "graphar+file://{}graphar/ldbc_sample.graph.yaml".format(
        graphar_temp_dir
    )
    selector = {
        "vertices": {
            "person": None,
            "organisation": None,
        },
        "edges": {
            "studyAt": None,
            "workAt": None,
        },
    }
    g = Graph.load_from(graph_uri, selector=selector)
    assert g.schema.vertex_label_num == 2 and g.schema.edge_label_num == 2
    assert (
        "person" in g.schema.vertex_labels and "organisation" in g.schema.vertex_labels
    )
    assert "studyAt" in g.schema.edge_labels and "workAt" in g.schema.edge_labels
    assert (
        g.schema.vertex_properties_num("person") == 8
        and g.schema.vertex_properties_num("organisation") == 4
    )
    assert (
        g.schema.edge_properties_num("studyAt") == 2
        and g.schema.edge_properties_num("workAt") == 2
    )
    del g
