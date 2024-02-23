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

import itertools

from graphscope import flash


def test_flash_pagerank_on_projected_graph(arrow_property_graph_directed):
    for v, e in itertools.product(["v0", "v1"], ["e0", "e1"]):
        g = arrow_property_graph_directed.project(
            vertices={v: []}, edges={e: ["weight"]}
        )
        ctx = flash.pagerank(g)
        df = ctx.to_dataframe({"node": "v.id", "r": "r"})
        print(df)


def test_flash_clustering_on_projected_graph(ldbc_graph):
    pg1 = ldbc_graph.project(
        vertices={"post": [], "tag": [], "tagclass": []},
        edges={"hasTag": [], "isSubclassOf": []},
    )
    pg2 = pg1.project(vertices={"tagclass": []}, edges={"isSubclassOf": []})
    pr_context = flash.clustering_coefficient(pg2)
    df = pr_context.to_dataframe(selector={"id": "v.id", "dist": "r"})
    print(df)


def test_flash_triangle_counting_on_projected_graph(arrow_property_graph_directed):
    for v, e in itertools.product(["v0", "v1"], ["e0", "e1"]):
        g = arrow_property_graph_directed.project(
            vertices={v: []}, edges={e: ["weight"]}
        )
        ctx = flash.triangle_counting(g)


def test_flash_bfs_on_projected_graph(ldbc_graph):
    g = ldbc_graph.project(
        vertices={"person": []},
        edges={"knows": []},
    )
    bfs_context = flash.bfs(g, source=65)
    df = bfs_context.to_dataframe(selector={"id": "v.id", "dist": "r"}).sort_values(
        by=["id"]
    )
    print(df)


def test_flash_cc_on_projected_graph(ldbc_graph):
    g = ldbc_graph.project(
        vertices={"person": []},
        edges={"knows": []},
    )
    cc_context = flash.cc(g)
    df = cc_context.to_dataframe(selector={"id": "v.id", "cc": "r"}).sort_values(
        by=["id"]
    )
    print(df)


def test_flash_cc_on_string_id_graph(p2p_project_directed_graph_string):
    ctx = flash.pagerank(p2p_project_directed_graph_string)
    df = ctx.to_dataframe({"id": "v.id", "cc": "r"}).sort_values(by=["id"])
    print(df)
