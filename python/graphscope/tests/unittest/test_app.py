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

import networkx as nx
import numpy as np
import pytest

import graphscope
from graphscope import avg_clustering
from graphscope import bfs
from graphscope import clustering
from graphscope import degree_centrality
from graphscope import eigenvector_centrality
from graphscope import hits
from graphscope import is_simple_path
from graphscope import k_core
from graphscope import k_shell
from graphscope import katz_centrality
from graphscope import louvain
from graphscope import lpa
from graphscope import pagerank
from graphscope import sssp
from graphscope import triangles
from graphscope import wcc
from graphscope.framework.app import AppAssets
from graphscope.framework.errors import InvalidArgumentError


def test_create_app():
    # builtin app on arrow projected graph
    a1 = AppAssets(algo="sssp", context="vertex_data")
    # on dynamic projected graph
    a2 = AppAssets(algo="sssp_has_path", context="tensor")


def test_compatible_with_dynamic_graph(dynamic_property_graph):
    # bfs
    with pytest.raises(
        InvalidArgumentError,
        match="Not compatible for arrow_property dynamic_property type",
    ):
        bfs(dynamic_property_graph, src=4)


def test_run_app_on_property_graph(arrow_property_graph, twitter_sssp_result):
    ctx1 = graphscope.sssp(arrow_property_graph, src=4, weight="weight")
    r1 = (
        ctx1.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(r1, twitter_sssp_result)


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_run_app_on_pandas_graph(p2p_graph_from_pandas, sssp_result):
    ctx1 = sssp(p2p_graph_from_pandas, src=6, weight="dist")
    r1 = (
        ctx1.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r1[r1 == 1.7976931348623157e308] = float("inf")  # replace limit::max with inf
    assert np.allclose(r1, sssp_result["directed"])


def test_run_app_on_directed_graph(
    p2p_project_directed_graph,
    sssp_result,
    pagerank_local_result,
    hits_result,
    bfs_result,
    clustering_result,
    dc_result,
    ev_result,
    katz_result,
):
    # sssp
    ctx1 = sssp(p2p_project_directed_graph, src=6)
    r1 = (
        ctx1.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r1[r1 == 1.7976931348623157e308] = float("inf")  # replace limit::max with inf
    assert np.allclose(r1, sssp_result["directed"])
    ctx2 = sssp(p2p_project_directed_graph, 6)
    r2 = (
        ctx2.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r2[r2 == 1.7976931348623157e308] = float("inf")  # replace limit::max with inf
    assert np.allclose(r2, sssp_result["directed"])
    assert np.allclose(
        ctx2.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy(),
        [[1.0, 260.0], [2.0, 229.0], [3.0, 310.0]],
    )
    assert np.allclose(
        sorted(ctx1.to_numpy("r", vertex_range={"begin": 1, "end": 4})),
        sorted([260.0, 229.0, 310.0]),
    )

    r3 = sssp(p2p_project_directed_graph, 100000000)
    assert r3 is not None

    # pagerank
    ctx_pr = pagerank(p2p_project_directed_graph, delta=0.85, max_round=10)
    ret_pr = (
        ctx_pr.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(ret_pr, pagerank_local_result["directed"])

    # hits
    ctx_hits = hits(p2p_project_directed_graph, tolerance=0.001)
    ret_hub = (
        ctx_hits.to_dataframe({"node": "v.id", "hub": "r.hub"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    ret_auth = (
        ctx_hits.to_dataframe({"node": "v.id", "auth": "r.auth"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(ret_hub, hits_result["hub"])
    assert np.allclose(ret_auth, hits_result["auth"])

    # bfs
    ctx4 = bfs(p2p_project_directed_graph, src=6)
    r4 = (
        ctx4.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert np.all(r4 == bfs_result["directed"])
    ctx5 = bfs(p2p_project_directed_graph, 6)
    r5 = (
        ctx5.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert np.all(r5 == bfs_result["directed"])
    assert np.all(
        ctx5.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy()
        == [[1, 5], [2, 5], [3, 6]]
    )
    assert np.all(
        sorted(ctx5.to_numpy("r", vertex_range={"begin": 1, "end": 4})) == [5, 5, 6]
    )

    # simple_path
    assert is_simple_path(p2p_project_directed_graph, [1, 10])

    with pytest.raises(
        InvalidArgumentError, match="Louvain not support directed graph."
    ):
        louvain(p2p_project_directed_graph)

    # clustering
    ctx_clustering = clustering(p2p_project_directed_graph)
    ret_clustering = (
        ctx_clustering.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(ret_clustering, clustering_result["directed"])

    # avg_clustering
    ctx_avg_clustering = avg_clustering(p2p_project_directed_graph)
    ret_avg_clustering = ctx_avg_clustering.to_numpy("r", axis=0)[0]
    assert ret_avg_clustering is not None

    # degree_centrality
    ctx_dc = degree_centrality(p2p_project_directed_graph)
    ret_dc = (
        ctx_dc.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(ret_dc, dc_result["directed"])

    # eigenvector_centrality
    ctx_ev = eigenvector_centrality(p2p_project_directed_graph)
    ret_ev = (
        ctx_ev.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(ret_ev, ev_result["directed"])

    # katz_centrality
    ctx_katz = katz_centrality(p2p_project_directed_graph)


def test_app_on_undirected_graph(
    p2p_project_undirected_graph,
    sssp_result,
    pagerank_local_result,
    bfs_result,
    wcc_result,
    lpa_result,
    triangles_result,
    kshell_result,
):
    # sssp
    ctx1 = sssp(p2p_project_undirected_graph, src=6)
    r1 = (
        ctx1.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r1[r1 == 1.7976931348623157e308] = float(
        "inf"
    )  # replace limit<double>::max with inf
    assert np.allclose(r1, sssp_result["undirected"])
    assert np.allclose(
        ctx1.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy(),
        [[1.0, 31.0], [2.0, 39.0], [3.0, 78.0]],
    )
    assert np.allclose(
        sorted(ctx1.to_numpy("r", vertex_range={"begin": 1, "end": 4})),
        [31.0, 39.0, 78.0],
    )

    # pagerank (only work on undirected graph)
    ctx2 = pagerank(p2p_project_undirected_graph, delta=0.85, max_round=10)
    r2 = (
        ctx2.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(r2, pagerank_local_result["undirected"])
    ctx3 = pagerank(p2p_project_undirected_graph, 0.85, 10)
    r3 = (
        ctx3.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(r3, pagerank_local_result["undirected"])
    # r4 = pagerank(arrow_project_graph, 10, 0.85) # check max_round=10
    # assert r4 is not None
    ctx5 = pagerank(p2p_project_undirected_graph, "0.85", "10")
    r5 = (
        ctx5.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(r5, pagerank_local_result["undirected"])
    ctx6 = pagerank(p2p_project_undirected_graph)
    r6 = (
        ctx6.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(r6, pagerank_local_result["undirected"])
    assert np.allclose(
        ctx6.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy(),
        [
            [1.0, 3.851369917786616],
            [2.0, 5.808207281313435],
            [3.0, 1.0284419953876565],
        ],
    )
    assert np.allclose(
        sorted(ctx6.to_numpy("r", vertex_range={"begin": 1, "end": 4})),
        sorted([3.851369917786616, 5.808207281313435, 1.0284419953876565]),
    )

    # bfs
    ctx7 = bfs(p2p_project_undirected_graph, src=6)
    r7 = (
        ctx7.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert np.all(r7 == bfs_result["undirected"])
    assert np.all(
        ctx7.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy()
        == [[1, 1], [2, 2], [3, 2]]
    )
    assert np.all(
        sorted(ctx7.to_numpy("r", vertex_range={"begin": 1, "end": 4})) == [1, 2, 2]
    )

    # wcc
    ctx8 = wcc(p2p_project_undirected_graph)
    r8 = (
        ctx8.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert np.all(r8 == wcc_result)
    assert np.all(
        ctx8.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy()
        == [[1, 1], [2, 1], [3, 1]]
    )
    assert np.all(ctx8.to_numpy("r", vertex_range={"begin": 1, "end": 4}) == [1, 1, 1])

    # lpa
    ctx9 = lpa(p2p_project_undirected_graph, max_round=10)
    r9 = (
        ctx9.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert np.all(r9 == lpa_result)
    assert np.all(
        ctx9.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy()
        == [[1, 1], [2, 2], [3, 2]]
    )
    assert np.all(
        sorted(ctx9.to_numpy("r", vertex_range={"begin": 1, "end": 4})) == [1, 2, 2]
    )

    # kshell
    ctx10 = k_shell(p2p_project_undirected_graph, k=3)
    r10 = (
        ctx10.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert np.all(r10 == kshell_result)
    assert np.all(
        ctx10.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy()
        == [[1, 0], [2, 0], [3, 0]]
    )
    assert np.all(ctx10.to_numpy("r", vertex_range={"begin": 1, "end": 4}) == [0, 0, 0])

    # triangles
    ctx_triangles = triangles(p2p_project_undirected_graph)
    ret_triangles = (
        ctx_triangles.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(ret_triangles, triangles_result["undirected"])

    # louvain
    ctx10 = louvain(p2p_project_undirected_graph, min_progress=50, progress_tries=2)

    # simple_path
    assert is_simple_path(p2p_project_undirected_graph, [1, 10])


def test_run_app_on_string_oid_graph(p2p_project_directed_graph_string):
    ctx = sssp(p2p_project_directed_graph_string, src="6")
    r1 = ctx.to_dataframe({"node": "v.id", "r": "r"})
    assert r1[r1["node"] == "6"].r.values[0] == 0.0
    ctx = wcc(p2p_project_directed_graph_string)
    r1 = ctx.to_dataframe({"node": "v.id", "r": "r"})


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_error_on_run_app(projected_pg_no_edge_data):
    # compile error: wrong type of edge data with sssp
    with pytest.raises(graphscope.CompilationError):
        sssp(projected_pg_no_edge_data, src=4)


def test_app_on_local_vm_graph(
    p2p_property_graph_undirected_local_vm,
    p2p_property_graph_undirected_local_vm_string,
    p2p_property_graph_undirected_local_vm_int32,
    wcc_result,
):
    # on default int64 oid
    ctx1 = graphscope.wcc(p2p_property_graph_undirected_local_vm)
    r1 = (
        ctx1.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    # Test algorithm correctness
    assert np.all(r1 == wcc_result)

    # Test compile, on string oid
    ctx2 = graphscope.wcc(p2p_property_graph_undirected_local_vm_string)
    r2 = (
        ctx2.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert r2 is not None

    # Test compile, on int32 oid
    ctx2 = graphscope.wcc(p2p_property_graph_undirected_local_vm_int32)
    r2 = (
        ctx2.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert r2 is not None


def test_wcc_on_flatten_graph(arrow_modern_graph):
    ctx = graphscope.wcc_auto(arrow_modern_graph)
    df = ctx.to_dataframe({"node": "v.id", "r": "r"})
    # The component id is all 1
    assert sum(df.r.values) == 6
    ctx = graphscope.wcc_projected(arrow_modern_graph)
    df = ctx.to_dataframe({"node": "v.id", "r": "r"})
    # The component id is all 0
    assert sum(df.r.values) == 0
