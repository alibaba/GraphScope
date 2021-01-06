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

import networkx.algorithms as nxa

import graphscope
from graphscope.experimental import nx
from graphscope.experimental.nx.utils.compat import patch_docstring
from graphscope.framework.app import AppAssets


@patch_docstring(nxa.pagerank)
def pagerank(G, alpha=0.85, max_iter=100, tol=1.0e-6):
    raise NotImplementedError


@patch_docstring(nxa.hits)
def hits(G, max_iter=100, tol=1.0e-8, normalized=True):
    pg = G.project_to_simple()
    ctx = graphscope.hits(pg, tol, max_iter, normalized)
    return ctx.to_dataframe({"node": "v.id", "auth": "r.auth", "hub": "r.hub"})


@patch_docstring(nxa.degree_centrality)
def degree_centrality(G):
    pg = G.project_to_simple()
    ctx = graphscope.degree_centrality(pg, centrality_type="both")
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@patch_docstring(nxa.in_degree_centrality)
def in_degree_centrality(G):
    pg = G.project_to_simple()
    ctx = graphscope.degree_centrality(pg, centrality_type="in")
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@patch_docstring(nxa.out_degree_centrality)
def out_degree_centrality(G):
    pg = G.project_to_simple()
    ctx = graphscope.degree_centrality(pg, centrality_type="out")
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@patch_docstring(nxa.eigenvector_centrality)
def eigenvector_centrality(G, max_iter=100, tol=1e-06, weight=None):
    if weight is None:
        weight = "weight"
    pg = G.project_to_simple(e_prop=weight)
    ctx = graphscope.eigenvector_centrality(pg, tolerance=tol, max_round=max_iter)
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@patch_docstring(nxa.katz_centrality)
def katz_centrality(
    G,
    alpha=0.1,
    beta=1.0,
    max_iter=100,
    tol=1e-06,
    nstart=None,
    normalized=True,
    weight=None,
):
    # FIXME: nstart not support.
    if weight is None:
        weight = "weight"
    pg = G.project_to_simple(e_prop=weight)
    ctx = graphscope.katz_centrality(
        pg,
        alpha=alpha,
        beta=beta,
        tolerance=tol,
        max_round=max_iter,
        normalized=normalized,
    )
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@patch_docstring(nxa.has_path)
def has_path(G, source, target):
    pg = G.project_to_simple()
    return AppAssets(algo="sssp_has_path")(pg, source, target)


@patch_docstring(nxa.shortest_path)
def shortest_path(G, source=None, target=None, weight=None):
    # FIXME: target and method not support.
    if weight is None:
        weight = "weight"
        default = False
    else:
        default = True
    pg = G.project_to_simple(e_prop=weight)
    return AppAssets(algo="sssp_path")(pg, source, weight=default)


@patch_docstring(nxa.single_source_dijkstra_path_length)
def single_source_dijkstra_path_length(G, source, weight=None):
    if weight is None:
        weight = "weight"
    pg = G.project_to_simple(e_prop=weight)
    ctx = AppAssets(algo="sssp_projected")(pg, source)
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@patch_docstring(nxa.average_shortest_path_length)
def average_shortest_path_length(G, weight=None):
    pg = G.project_to_simple(e_prop=weight)
    ctx = AppAssets(algo="sssp_average_length")(pg, weight=True)
    return ctx.to_numpy("r", axis=0)[0]


@patch_docstring(nxa.bfs_edges)
def bfs_edges(G, source, reverse=False, depth_limit=None):
    # FIXME: reverse not support.
    pg = G.project_to_simple()
    ctx = AppAssets(algo="bfs_generic")(pg, source, depth_limit, format="edges")
    return ctx.to_numpy("r", axis=0).tolist()


@patch_docstring(nxa.bfs_predecessors)
def bfs_predecessors(G, source, depth_limit=None):
    pg = G.project_to_simple()
    return AppAssets(algo="bfs_generic")(pg, source, depth_limit, format="predecessors")


@patch_docstring(nxa.bfs_successors)
def bfs_successors(G, source, depth_limit=None):
    pg = G.project_to_simple()
    return AppAssets(algo="bfs_generic")(pg, source, depth_limit, format="successors")


@patch_docstring(nxa.bfs_tree)
def bfs_tree(G, source, reverse=False, depth_limit=None):
    T = nx.DiGraph()
    T.add_node(source)
    edges_gen = bfs_edges(G, source, reverse=reverse, depth_limit=depth_limit)
    T.add_edges_from(edges_gen)
    return T


@patch_docstring(nxa.k_core)
def k_core(G, k=None, core_number=None):
    # FIXME: core number not support.
    pg = G.project_to_simple()
    return graphscope.k_core(pg, k)


@patch_docstring(nxa.clustering)
def clustering(G, nodes=None, weight=None):
    # FIXME(weibin): clustering now only correct in directed graph.
    # FIXME: nodes and weight not support.
    pg = G.project_to_simple()
    ctx = graphscope.clustering(pg)
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@patch_docstring(nxa.triangles)
def triangles(G, nodes=None):
    # FIXME: nodes not support.
    pg = G.project_to_simple()
    ctx = graphscope.triangles(pg)
    return ctx.to_dataframe({"node": "v.id", "result": "r"})


@patch_docstring(nxa.transitivity)
def transitivity(G):
    # FIXME: nodes not support.
    pg = G.project_to_simple()
    return AppAssets(algo="transitivity")(pg)


@patch_docstring(nxa.average_clustering)
def average_clustering(G, nodes=None, weight=None, count_zeros=True):
    # FIXME: nodes, weight, count_zeros not support.
    pg = G.project_to_simple()
    ctx = AppAssets(algo="avg_clustering")(pg)
    return ctx.to_numpy("r")[0]


@patch_docstring(nxa.weakly_connected_components)
def weakly_connected_components(G):
    pg = G.project_to_simple()
    return AppAssets(algo="wcc_projected")(pg)
