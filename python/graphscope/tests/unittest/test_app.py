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
import os

import networkx as nx
import numpy as np
import pytest

import graphscope
from graphscope import avg_clustering
from graphscope import bfs
from graphscope import clustering  # directed / undirected would call different app
from graphscope import custom_analytical_algorithm
from graphscope import degree_centrality
from graphscope import eigenvector_centrality
from graphscope import hits
from graphscope import is_simple_path
from graphscope import k_shell
from graphscope import katz_centrality
from graphscope import louvain
from graphscope import lpa
from graphscope import pagerank  # directed / undirected would call different app
from graphscope import sssp
from graphscope import triangles
from graphscope import wcc
from graphscope.framework.app import AppAssets
from graphscope.framework.errors import InvalidArgumentError


def context_to_np(
    ctx, selector={"node": "v.id", "r": "r"}, vertex_range=None, dtype=float
):
    return (
        ctx.to_dataframe(selector, vertex_range=vertex_range)
        .sort_values(by=["node"])
        .to_numpy(dtype=dtype)
    )


def test_create_app():
    # builtin app on arrow projected graph
    a1 = AppAssets(algo="sssp", context="vertex_data")
    # on dynamic projected graph
    a2 = AppAssets(algo="sssp_has_path", context="tensor")


def test_compatible_with_dynamic_graph(dynamic_property_graph):
    # bfs
    with pytest.raises(
        InvalidArgumentError,
        match="isn't compatible for",
    ):
        bfs(dynamic_property_graph, src=4)


def test_run_app_on_property_graph(arrow_property_graph, twitter_sssp_result):
    ctx = graphscope.sssp(arrow_property_graph, src=4, weight="weight")
    r = context_to_np(ctx, dtype=float)
    assert np.allclose(r, twitter_sssp_result)


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_run_app_on_pandas_graph(p2p_graph_from_pandas, sssp_result):
    ctx = sssp(p2p_graph_from_pandas, src=6, weight="dist")
    r = context_to_np(ctx, dtype=float)
    r[r == 1.7976931348623157e308] = float("inf")  # replace limit::max with inf
    assert np.allclose(r, sssp_result["directed"])


def test_other_app_on_directed_graph(
    p2p_project_directed_graph,
    hits_result,
    dc_result,
    ev_result,
    clustering_result,
):
    # hits
    ctx_hits = hits(p2p_project_directed_graph, tolerance=0.001)
    ret_hub = context_to_np(
        ctx_hits, selector={"node": "v.id", "hub": "r.hub"}, dtype=float
    )
    ret_auth = context_to_np(
        ctx_hits, selector={"node": "v.id", "hub": "r.auth"}, dtype=float
    )
    assert np.allclose(ret_hub, hits_result["hub"])
    assert np.allclose(ret_auth, hits_result["auth"])

    # simple_path
    assert is_simple_path(p2p_project_directed_graph, [1, 10])

    with pytest.raises(
        InvalidArgumentError, match="Louvain not support directed graph."
    ):
        louvain(p2p_project_directed_graph)

    # avg_clustering
    ctx_avg_clustering = avg_clustering(p2p_project_directed_graph)
    ret_avg_clustering = ctx_avg_clustering.to_numpy("r", axis=0)[0]
    assert ret_avg_clustering is not None

    # degree_centrality
    ctx_dc = degree_centrality(p2p_project_directed_graph)
    ret_dc = context_to_np(ctx_dc, dtype=float)
    assert np.allclose(ret_dc, dc_result["directed"])

    # eigenvector_centrality
    ctx_ev = eigenvector_centrality(p2p_project_directed_graph)
    ret_ev = context_to_np(ctx_ev, dtype=float)
    assert np.allclose(ret_ev, ev_result["directed"])

    # katz_centrality
    ctx_katz = katz_centrality(p2p_project_directed_graph)

    ctx = clustering(p2p_project_directed_graph)
    r = context_to_np(ctx, dtype=float)
    assert np.allclose(r, clustering_result["directed"])


def test_bfs(p2p_project_directed_graph, p2p_project_undirected_graph, bfs_result):
    ctx = bfs(p2p_project_directed_graph, src=6)
    r = context_to_np(ctx, dtype=int)

    assert np.all(r == bfs_result["directed"])
    r = context_to_np(ctx, vertex_range={"begin": 1, "end": 4}, dtype=int)
    expected = [[1, 5], [2, 5], [3, 6]]
    assert np.all(r == expected)

    ctx = bfs(p2p_project_undirected_graph, src=6)
    r = context_to_np(ctx, dtype=int)
    assert np.all(r == bfs_result["undirected"])
    r = context_to_np(ctx, vertex_range={"begin": 1, "end": 4}, dtype=int)
    expected = [[1, 1], [2, 2], [3, 2]]
    assert np.all(r == expected)


def test_lpa(p2p_project_undirected_graph, lpa_result):
    ctx = lpa(p2p_project_undirected_graph, max_round=10)
    r = context_to_np(ctx, dtype=int)
    assert np.all(r == lpa_result)
    r = context_to_np(ctx, vertex_range={"begin": 1, "end": 4}, dtype=int)
    expected = [[1, 1], [2, 2], [3, 2]]
    assert np.all(r == expected)


def test_lcc(p2p_project_undirected_graph, lcc_result):
    ctx = graphscope.lcc(p2p_project_undirected_graph)
    r = context_to_np(ctx, dtype=float)
    assert np.allclose(r, lcc_result["undirected"])


def test_sssp(p2p_project_directed_graph, p2p_project_undirected_graph, sssp_result):
    ctx = sssp(p2p_project_directed_graph, src=6)
    r = context_to_np(ctx, dtype=float)
    r[r == 1.7976931348623157e308] = float("inf")  # replace limit::max with inf
    assert np.allclose(r, sssp_result["directed"])
    r = context_to_np(ctx, vertex_range={"begin": 1, "end": 4}, dtype=float)
    expected = [[1.0, 260.0], [2.0, 229.0], [3.0, 310.0]]
    assert np.allclose(r, expected)

    r = sssp(p2p_project_directed_graph, 100000000)
    assert r is not None

    ctx = sssp(p2p_project_undirected_graph, src=6)
    r = context_to_np(ctx, dtype=float)
    r[r == 1.7976931348623157e308] = float("inf")  # replace limit<double>::max with inf
    assert np.allclose(r, sssp_result["undirected"])
    r = context_to_np(ctx, vertex_range={"begin": 1, "end": 4}, dtype=float)
    expected = [[1.0, 31.0], [2.0, 39.0], [3.0, 78.0]]
    assert np.allclose(r, expected)


def test_wcc(
    p2p_project_directed_graph,
    p2p_project_undirected_graph,
    wcc_result,
    wcc_auto_result,
):
    ctx = wcc(p2p_project_undirected_graph)
    r: np.ndarray = context_to_np(ctx, dtype=int)
    assert np.all(r == wcc_auto_result)
    r = context_to_np(ctx, vertex_range={"begin": 1, "end": 4}, dtype=int)
    expected = [[1, 1], [2, 1], [3, 1]]
    assert np.all(r == expected)

    with pytest.raises(InvalidArgumentError, match="isn't compatible"):
        wcc(p2p_project_directed_graph)


def test_pagerank(
    p2p_project_directed_graph, p2p_project_undirected_graph, pagerank_result
):
    ctx = pagerank(p2p_project_directed_graph, delta=0.85, max_round=10)
    r = context_to_np(ctx, dtype=float)
    # assert np.allclose(r, pagerank_result["directed"])
    print(r)
    ctx = pagerank(p2p_project_undirected_graph, delta=0.85, max_round=10)
    r = context_to_np(ctx, dtype=float)
    assert np.allclose(r, pagerank_result["undirected"])


def test_other_app_on_undirected_graph(
    p2p_project_undirected_graph,
    triangles_result,
    kshell_result,
):
    # kshell
    ctx = k_shell(p2p_project_undirected_graph, k=3)
    r = context_to_np(ctx, dtype=int)
    assert np.all(r == kshell_result)
    r = context_to_np(ctx, vertex_range={"begin": 1, "end": 4}, dtype=int)
    expected = [[1, 0], [2, 0], [3, 0]]
    assert np.all(r == expected)

    # triangles
    ctx = triangles(p2p_project_undirected_graph)
    r = context_to_np(ctx, dtype=int)
    assert np.allclose(r, triangles_result["undirected"])

    # louvain
    ctx = louvain(p2p_project_undirected_graph, min_progress=50, progress_tries=2)
    assert ctx is not None
    # simple_path
    ctx = is_simple_path(p2p_project_undirected_graph, [1, 10])
    assert ctx is not None


def test_run_app_on_string_oid_graph(p2p_project_undirected_graph_string):
    ctx = sssp(p2p_project_undirected_graph_string, src="6")
    r1 = ctx.to_dataframe({"node": "v.id", "r": "r"})
    assert r1[r1["node"] == "6"].r.values[0] == 0.0
    ctx = graphscope.wcc(p2p_project_undirected_graph_string)
    r1 = ctx.to_dataframe({"node": "v.id", "r": "r"})


def test_error_on_run_app(projected_pg_no_edge_data):
    # compile error: wrong type of edge data with sssp
    with pytest.raises(ValueError):
        sssp(projected_pg_no_edge_data, src=4)

    with pytest.raises(
        graphscope.AnalyticalEngineInternalError,
        match="args_num >= query_args.args_size()",
    ):
        custom_analytical_algorithm(projected_pg_no_edge_data, "wcc", 1, 2, 3)


def test_app_on_local_vm_graph(
    p2p_property_graph_undirected_local_vm,
    p2p_property_graph_undirected_local_vm_string,
    p2p_property_graph_undirected_local_vm_int32,
    wcc_auto_result,
):
    # on default int64 oid
    ctx = graphscope.wcc(p2p_property_graph_undirected_local_vm)
    r = context_to_np(ctx, dtype=int)
    assert np.all(r == wcc_auto_result)

    # Test compile, on string oid
    ctx = graphscope.wcc(p2p_property_graph_undirected_local_vm_string)
    r = context_to_np(ctx, dtype=int)
    assert r is not None

    # Test compile, on int32 oid
    ctx = graphscope.wcc(p2p_property_graph_undirected_local_vm_int32)
    r = context_to_np(ctx, dtype=int)
    assert r is not None


def test_app_on_compact_graph(
    p2p_property_graph_undirected_compact,
    wcc_auto_result,
):
    ctx = graphscope.wcc_auto(p2p_property_graph_undirected_compact)
    r = context_to_np(ctx, dtype=int)
    assert np.all(r == wcc_auto_result)


def test_app_on_perfect_hash_graph(
    p2p_property_graph_undirected_perfect_hash,
    wcc_auto_result,
):
    # on default int64 oid
    ctx = graphscope.wcc_auto(p2p_property_graph_undirected_perfect_hash)
    r = context_to_np(ctx, dtype=int)
    assert np.all(r == wcc_auto_result)


def test_wcc_on_flatten_graph(arrow_modern_graph_undirected):
    ctx = graphscope.wcc_auto(arrow_modern_graph_undirected)
    df = ctx.to_dataframe({"node": "v.id", "r": "r"})
    # The component id is all 1
    assert sum(df.r.values) == 6


def test_wcc_on_flatten_ldbc_graph(ldbc_graph):
    graph = ldbc_graph.project(
        vertices={"person": [], "place": []}, edges={"knows": [], "isLocatedIn": []}
    )
    ctx = graphscope.wcc_projected(graph)
    df = ctx.to_dataframe({"id": "v.id", "result": "r"}).sort_values(by=["id"])
    assert len(df) == 2363


def test_voterank_on_flatten_ldbc_graph(ldbc_graph):
    graph = ldbc_graph.project(
        vertices={"person": [], "place": []}, edges={"knows": [], "isLocatedIn": []}
    )
    ctx = graphscope.voterank(graph, 10)
    df = ctx.to_dataframe({"id": "v.id", "result": "r"}).sort_values(by=["id"])
    assert len(df) == 2363


def test_louvain_on_projected_graph(arrow_property_graph_undirected):
    for v, e in itertools.product(["v0", "v1"], ["e0", "e1"]):
        g = arrow_property_graph_undirected.project(
            vertices={v: []}, edges={e: ["weight"]}
        )
        ctx = louvain(g)
        ctx.to_dataframe({"node": "v.id", "r": "r"})


def test_pagerank_nx_on_projected_projected(ldbc_graph):
    pg1 = ldbc_graph.project(
        vertices={"post": [], "tag": [], "tagclass": []},
        edges={"hasTag": [], "isSubclassOf": []},
    )
    pg2 = pg1.project(vertices={"tagclass": []}, edges={"isSubclassOf": []})
    pr_context = graphscope.pagerank_nx(pg2, alpha=0.85, max_iter=100, tol=1e-06)
    df = pr_context.to_dataframe(selector={"id": "v.id", "dist": "r"})
    assert df.shape == (71, 2)  # V(tagclass)

    # check existence of OIDs
    for oid in [349, 211, 239, 0, 98, 233, 41, 243, 242, 67]:
        assert oid in df["id"].values


def test_pagerank_nx_on_flatten(ldbc_graph):
    pg = ldbc_graph.project(vertices={"post": [], "tag": []}, edges={"hasTag": []})
    pr_context = graphscope.pagerank_nx(pg, alpha=0.85, max_iter=100, tol=1e-06)
    df = pr_context.to_dataframe(selector={"id": "v.id", "dist": "r"})
    assert df.shape == (95056, 2)  # V(post) + V(tag)

    # check existence of OIDs
    for oid in [
        618475290624,
        3,
        412316860420,
        412316860421,
        412316860422,
        16075,
        16076,
        16077,
        16078,
    ]:
        assert oid in df["id"].values
