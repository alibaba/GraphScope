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

from graphscope.framework.graph import Graph
from graphscope import pagerank

graphar_test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")


@pytest.mark.skip(reason="Issue 3162")
def test_load_from_graphar(graphscope_session):
    # graph_yaml = os.path.join(
    #     graphar_test_repo_dir, "graphar/ldbc_sample/parquet/ldbc_sample.graph.yml"
    # )
    # graph_yaml_path = "graphar+file://" + graph_yaml
    graph_yaml_path = "graphar+file:///tmp/graphar/ldbc_sample.graph.yml" 
    print(graph_yaml_path)
    g = Graph.load_from(graph_yaml_path, graphscope_session)
    ldbc_simple = g.project(vertices={"person": []}, edges={"knows": []})
    ctx = pagerank(ldbc_simple)
    print(ctx.to_dataframe({"id": "v.id", "value": "r"}))
    assert g.schema is not None
    del g


@pytest.mark.skip(reason="Issue 3162")
def test_save_to_graphar(ldbc_graph):
    graphar_options = {
        "graph_name": "ldbc_sample",
        "file_type": "parquet",
        "vertex_block_size": 500,
        "edge_block_size": 500,
    }
    ldbc_graph.save_to("/tmp/graphar/", format="graphar", graphar_options=graphar_options)
