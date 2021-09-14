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
from graphscope import bfs
from graphscope import cdlp
from graphscope import clustering
from graphscope import degree_centrality
from graphscope import eigenvector_centrality
from graphscope import hits
from graphscope import k_core
from graphscope import k_shell
from graphscope import katz_centrality
from graphscope import louvain
from graphscope import lpa
from graphscope import pagerank
from graphscope import property_sssp
from graphscope import sssp
from graphscope import triangles
from graphscope import wcc
from graphscope.framework.app import AppAssets
from graphscope.framework.errors import InvalidArgumentError


def test_create_app():
    # builtin-ldbc compatible graph: arrow_projected dynamic_projected
    # builtin-property compatible graph: arrow_property, append_only
    # builtin-property app on property graph
    a1 = AppAssets(algo="property_sssp", context="labeled_vertex_data")
    # builtin app on arrow projected graph
    a2 = AppAssets(algo="sssp", context="vertex_data")
    # on dynamic projected graph
    a3 = AppAssets(algo="sssp_has_path", context="tensor")


@pytest.mark.skipif(
    os.environ.get("NETWORKX") != "ON", reason="dynamic graph is in NETWORKX ON"
)
def test_compatible_with_dynamic_graph(dynamic_property_graph):
    # bfs
    with pytest.raises(
        InvalidArgumentError,
        match="Not compatible for arrow_property dynamic_property type",
    ):
        bfs(dynamic_property_graph, src=4)


def test_errors_on_create_app(arrow_property_graph, arrow_project_graph):
    # builtin-property app is incompatible with projected graph
    with pytest.raises(graphscope.CompilationError):
        a = AppAssets(algo="property_sssp", context="labeled_vertex_data")
        pg = arrow_project_graph._project_to_simple()
        a(pg, 4)

    # builtin app is incompatible with property graph
    with pytest.raises(graphscope.CompilationError):
        a = AppAssets(algo="sssp", context="vertex_data")
        a(arrow_property_graph, 4)

    # algo not exist
    with pytest.raises(
        KeyError,
        match="Algorithm does not exist in the gar resource",
    ):
        a = AppAssets(algo="invalid", context="vertex_data")
        a(arrow_property_graph, 4)


@pytest.mark.skipif(
    os.environ.get("NETWORKX") != "ON", reason="dynamic graph is in NETWORKX ON"
)
def test_errors_on_create_app_with_dynamic(dynamic_project_graph):
    with pytest.raises(graphscope.CompilationError):
        a = AppAssets(algo="property_sssp", context="labeled_vertex_data")
        a(dynamic_project_graph, 4)


def test_error_on_non_graph():
    eg1 = nx.Graph()  # networkx graph is unsupported
    with pytest.raises(
        InvalidArgumentError, match="Missing graph_type attribute in graph object"
    ):
        sssp(eg1, 4)


def test_run_app_on_directed_graph(
    p2p_project_directed_graph,
    sssp_result,
    pagerank_result,
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

    with pytest.raises(
        InvalidArgumentError, match="Louvain not support directed graph."
    ):
        louvain(p2p_project_directed_graph)


def test_app_on_undirected_graph(
    p2p_project_undirected_graph,
    sssp_result,
    pagerank_result,
    bfs_result,
    wcc_result,
    cdlp_result,
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
    assert np.allclose(r2, pagerank_result["undirected"])
    ctx3 = pagerank(p2p_project_undirected_graph, 0.85, 10)
    r3 = (
        ctx3.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(r3, pagerank_result["undirected"])
    # r4 = pagerank(arrow_project_graph, 10, 0.85) # check max_round=10
    # assert r4 is not None
    ctx5 = pagerank(p2p_project_undirected_graph, "0.85", "10")
    r5 = (
        ctx5.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(r5, pagerank_result["undirected"])
    ctx6 = pagerank(p2p_project_undirected_graph)
    r6 = (
        ctx6.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    assert np.allclose(r6, pagerank_result["undirected"])
    assert np.allclose(
        ctx6.to_dataframe(
            {"node": "v.id", "r": "r"}, vertex_range={"begin": 1, "end": 4}
        )
        .sort_values(by=["node"])
        .to_numpy(),
        [
            [1.0, 6.153724343761569e-05],
            [2.0, 9.280361872165397e-05],
            [3.0, 1.643246086005906e-05],
        ],
    )
    assert np.allclose(
        sorted(ctx6.to_numpy("r", vertex_range={"begin": 1, "end": 4})),
        sorted([6.153724343761569e-05, 9.280361872165397e-05, 1.643246086005906e-05]),
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

    # cdlp
    ctx9 = cdlp(p2p_project_undirected_graph, max_round=10)
    r9 = (
        ctx9.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=int)
    )
    assert np.all(r9 == cdlp_result)
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

    # louvain
    ctx10 = louvain(p2p_project_undirected_graph, min_progress=50, progress_tries=2)


def test_run_app_on_string_oid_graph(p2p_project_directed_graph_string):
    ctx = sssp(p2p_project_directed_graph_string, src="6")
    r1 = ctx.to_dataframe({"node": "v.id", "r": "r"})
    assert r1[r1["node"] == "6"].r.values[0] == 0.0


def test_error_on_parameters_not_correct(arrow_project_graph):
    # Incorrect type of parameters
    with pytest.raises(ValueError, match="could not convert string to float"):
        pagerank(arrow_project_graph, "delta=0.85", 10)
    with pytest.raises(ValueError, match=r"invalid literal for int\(\) with base 10"):
        pagerank(arrow_project_graph, 0.85, "max_round=10")
    with pytest.raises(TypeError):
        pagerank(arrow_project_graph, 0.85, 10, 100, 1000, 10000)


def test_error_on_run_app(projected_pg_no_edge_data):
    # compile error: wrong type of edge data with sssp
    with pytest.raises(graphscope.CompilationError):
        sssp(projected_pg_no_edge_data, src=4)
