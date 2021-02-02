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

import numpy as np
import pytest

import graphscope
import graphscope.experimental.nx as nx
from graphscope import property_sssp
from graphscope import sssp
from graphscope.client.session import default_session
from graphscope.dataset.ldbc import load_ldbc
from graphscope.dataset.modern_graph import load_modern_graph
from graphscope.framework.loader import Loader


@pytest.fixture(scope="module")
def graphscope_session():
    sess = graphscope.session(run_on_local=True, show_log=True)
    yield sess
    sess.close()


test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")
new_property_dir = os.path.join(test_repo_dir, "new_property", "v2_e2")
property_dir = os.path.join(test_repo_dir, "property")


@pytest.fixture(scope="module")
def arrow_property_graph(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "e0": [
                (
                    Loader(
                        "{}/twitter_e_0_0_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_0_1_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_0_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_1_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v1"),
                ),
            ],
            "e1": [
                (
                    Loader(
                        "{}/twitter_e_0_0_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_0_1_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_0_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_1_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v1"),
                ),
            ],
        },
        vertices={
            "v0": Loader("{}/twitter_v_0".format(new_property_dir), header_row=True),
            "v1": Loader("{}/twitter_v_1".format(new_property_dir), header_row=True),
        },
        generate_eid=False,
    )
    yield g
    g.unload()


@pytest.fixture(scope="module")
def arrow_property_graph_only_from_efile(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "e0": [
                (
                    Loader(
                        "{}/twitter_e_0_0_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_0_1_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_0_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_1_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v1"),
                ),
            ],
            "e1": [
                (
                    Loader(
                        "{}/twitter_e_0_0_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_0_1_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_0_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_1_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v1"),
                ),
            ],
        },
        generate_eid=False,
    )
    yield g
    g.unload()


@pytest.fixture(scope="module")
def arrow_modern_graph(graphscope_session):
    graph = load_modern_graph(
        graphscope_session, prefix="{}/modern_graph".format(test_repo_dir)
    )
    yield graph
    graph.unload()


@pytest.fixture(scope="module")
def modern_person():
    return "{}/modern_graph/person.csv".format(test_repo_dir)


@pytest.fixture(scope="module")
def modern_software():
    return "{}/modern_graph/software.csv".format(test_repo_dir)


@pytest.fixture(scope="module")
def twitter_v_0():
    return "{}/twitter_v_0".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_v_1():
    return "{}/twitter_v_1".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_e_0_0_0():
    return "{}/twitter_e_0_0_0".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_e_0_1_0():
    return "{}/twitter_e_0_1_0".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_e_1_0_0():
    return "{}/twitter_e_1_0_0".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_e_1_1_0():
    return "{}/twitter_e_1_1_0".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_e_0_0_1():
    return "{}/twitter_e_0_0_1".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_e_0_1_1():
    return "{}/twitter_e_0_1_1".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_e_1_0_1():
    return "{}/twitter_e_1_0_1".format(new_property_dir)


@pytest.fixture(scope="module")
def twitter_e_1_1_1():
    return "{}/twitter_e_1_1_1".format(new_property_dir)


@pytest.fixture(scope="module")
def arrow_property_graph_undirected(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "e0": [
                (
                    Loader(
                        "{}/twitter_e_0_0_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_0_1_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_0_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_1_0".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v1"),
                ),
            ],
            "e1": [
                (
                    Loader(
                        "{}/twitter_e_0_0_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_0_1_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_0_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        "{}/twitter_e_1_1_1".format(new_property_dir),
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v1"),
                ),
            ],
        },
        vertices={
            "v0": Loader("{}/twitter_v_0".format(new_property_dir), header_row=True),
            "v1": Loader("{}/twitter_v_1".format(new_property_dir), header_row=True),
        },
        directed=False,
        generate_eid=False,
    )
    yield g
    g.unload()


@pytest.fixture(scope="module")
def arrow_property_graph_lpa(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "e0": (
                Loader(
                    "{}/lpa_dataset/lpa_3000_e_0".format(property_dir), header_row=True
                ),
                ["weight"],
                ("src_id", "v0"),
                ("dst_id", "v1"),
            ),
        },
        vertices={
            "v0": Loader(
                "{}/lpa_dataset/lpa_3000_v_0".format(property_dir), header_row=True
            ),
            "v1": Loader(
                "{}/lpa_dataset/lpa_3000_v_1".format(property_dir), header_row=True
            ),
        },
        generate_eid=False,
    )
    yield g
    g.unload()


@pytest.fixture(scope="module")
def arrow_project_graph(arrow_property_graph):
    pg = arrow_property_graph.project_to_simple(0, 0, 0, 0)
    yield pg


@pytest.fixture(scope="module")
def arrow_project_undirected_graph(arrow_property_graph_undirected):
    pg = arrow_property_graph_undirected.project_to_simple(0, 0, 0, 0)
    yield pg


@pytest.fixture(scope="module")
def p2p_property_graph(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "knows": (
                Loader("{}/p2p-31_property_e_0".format(property_dir), header_row=True),
                ["src_label_id", "dst_label_id", "dist"],
                ("src_id", "person"),
                ("dst_id", "person"),
            ),
        },
        vertices={
            "person": Loader(
                "{}/p2p-31_property_v_0".format(property_dir), header_row=True
            ),
        },
        generate_eid=False,
    )
    yield g


@pytest.fixture(scope="module")
def p2p_property_graph_string(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "knows": (
                Loader("{}/p2p-31_property_e_0".format(property_dir), header_row=True),
                ["src_label_id", "dst_label_id", "dist"],
                ("src_id", "person"),
                ("dst_id", "person"),
            ),
        },
        vertices={
            "person": Loader(
                "{}/p2p-31_property_v_0".format(property_dir), header_row=True
            ),
        },
        generate_eid=False,
        oid_type="string",
    )
    yield g


@pytest.fixture(scope="module")
def p2p_property_graph_undirected(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "knows": (
                Loader("{}/p2p-31_property_e_0".format(property_dir), header_row=True),
                ["src_label_id", "dst_label_id", "dist"],
                ("src_id", "person"),
                ("dst_id", "person"),
            ),
        },
        vertices={
            "person": Loader(
                "{}/p2p-31_property_v_0#header_row=true".format(property_dir),
                header_row=True,
            ),
        },
        directed=False,
        generate_eid=False,
    )
    yield g


@pytest.fixture(scope="module")
def p2p_project_directed_graph(p2p_property_graph):
    pg = p2p_property_graph.project_to_simple(0, 0, 0, 2)
    yield pg


@pytest.fixture(scope="module")
def p2p_project_undirected_graph(p2p_property_graph_undirected):
    pg = p2p_property_graph_undirected.project_to_simple(0, 0, 0, 2)
    yield pg


@pytest.fixture(scope="module")
def p2p_project_directed_graph_string(p2p_property_graph_string):
    pg = p2p_property_graph_string.project_to_simple(0, 0, 0, 2)
    yield pg


@pytest.fixture(scope="module")
def projected_pg_no_edge_data(arrow_property_graph):
    pg = arrow_property_graph.project_to_simple(0, 0, None, None)
    yield pg


@pytest.fixture(scope="module")
def dynamic_property_graph(graphscope_session):
    with default_session(graphscope_session):
        g = nx.Graph()
    g.add_edges_from([(1, 2), (2, 3)], weight=3)
    yield g


@pytest.fixture(scope="module")
def dynamic_project_graph(graphscope_session):
    with default_session(graphscope_session):
        g = nx.Graph()
    g.add_edges_from([(1, 2), (2, 3)], weight=3)
    pg = g.project_to_simple(e_prop="weight")
    yield pg


@pytest.fixture(scope="module")
def arrow_empty_graph(property_dir=os.path.expandvars("${GS_TEST_DIR}/property")):
    return None


@pytest.fixture(scope="module")
def append_only_graph():
    return None


@pytest.fixture(scope="module")
def sssp_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/ldbc/p2p-31-SSSP-directed".format(property_dir), dtype=float
    )
    ret["undirected"] = np.loadtxt(
        "{}/ldbc/p2p-31-SSSP".format(property_dir), dtype=float
    )
    yield ret


@pytest.fixture(scope="module")
def wcc_result():
    ret = np.loadtxt("{}/../p2p-31-wcc_auto".format(property_dir), dtype=int)
    yield ret


@pytest.fixture(scope="module")
def kshell_result():
    ret = np.loadtxt("{}/../p2p-31-kshell-3".format(property_dir), dtype=int)
    yield ret


@pytest.fixture(scope="module")
def pagerank_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/ldbc/p2p-31-PR-directed".format(property_dir), dtype=float
    )
    ret["undirected"] = np.loadtxt(
        "{}/ldbc/p2p-31-PR".format(property_dir), dtype=float
    )
    yield ret


@pytest.fixture(scope="module")
def bfs_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/ldbc/p2p-31-BFS-directed".format(property_dir), dtype=int
    )
    ret["undirected"] = np.loadtxt("{}/ldbc/p2p-31-BFS".format(property_dir), dtype=int)
    yield ret


@pytest.fixture(scope="module")
def cdlp_result():
    ret = np.loadtxt("{}/ldbc/p2p-31-CDLP".format(property_dir), dtype=int)
    yield ret


@pytest.fixture(scope="module")
def clustering_result():
    ret = np.fromfile(
        "{}/results/twitter_property_clustering_ndarray".format(property_dir), sep="\n"
    )
    yield ret


@pytest.fixture(scope="module")
def dc_result():
    ret = np.fromfile(
        "{}/results/twitter_property_dc_ndarray".format(property_dir), sep="\n"
    )
    yield ret


@pytest.fixture(scope="module")
def ev_result():
    ret = np.fromfile(
        "{}/results/twitter_property_ev_ndarray".format(property_dir), sep="\n"
    )
    yield ret


@pytest.fixture(scope="module")
def katz_result():
    ret = np.fromfile(
        "{}/results/twitter_property_katz_ndarray".format(property_dir), sep="\n"
    )
    yield ret


@pytest.fixture(scope="module")
def triangles_result():
    ret = np.fromfile(
        "{}/results/twitter_property_triangles_ndarray".format(property_dir),
        dtype=np.int64,
        sep="\n",
    )
    yield ret


@pytest.fixture(scope="module")
def property_context(arrow_property_graph):
    return property_sssp(arrow_property_graph, 20)


@pytest.fixture(scope="module")
def simple_context(arrow_property_graph):
    sg = arrow_property_graph.project_to_simple(0, 0, 0, 0)
    return sssp(sg, 20)


@pytest.fixture(scope="module")
def ldbc_graph(graphscope_session):
    graph = load_ldbc(graphscope_session, prefix="{}/ldbc_sample".format(test_repo_dir))
    yield graph
    graph.unload()
