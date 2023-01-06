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
import pandas as pd
import pytest

import graphscope
import graphscope.nx as nx
from graphscope import sssp
from graphscope.client.session import default_session
from graphscope.dataset import load_ldbc
from graphscope.dataset import load_modern_graph
from graphscope.framework.loader import Loader


@pytest.fixture(scope="module")
def graphscope_session():
    graphscope.set_option(show_log=True)
    graphscope.set_option(log_level="DEBUG")
    if os.environ.get("DEPLOYMENT", None) == "standalone":
        sess = graphscope.session(cluster_type="hosts", num_workers=1)
    else:
        sess = graphscope.session(cluster_type="hosts")
    yield sess
    sess.close()


test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")
new_property_dir = os.path.join(test_repo_dir, "new_property", "v2_e2")
property_dir = os.path.join(test_repo_dir, "property")


@pytest.fixture(scope="module")
def arrow_modern_graph(graphscope_session):
    graph = load_modern_graph(
        graphscope_session, prefix="{}/modern_graph".format(test_repo_dir)
    )
    yield graph
    del graph


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
def modern_graph():
    return "{}/modern_graph".format(test_repo_dir)


@pytest.fixture(scope="module")
def ldbc_sample():
    return "{}/ldbc_sample".format(test_repo_dir)


@pytest.fixture(scope="module")
def p2p_property():
    return "{}/property".format(test_repo_dir)


@pytest.fixture(scope="module")
def ogbn_mag_small():
    return "{}/ogbn_mag_small".format(test_repo_dir)


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
    del g


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
    del g


# @pytest.fixture(scope="module")
# def arrow_property_graph(graphscope_session):
# g = graphscope_session.g(generate_eid=False)
# g = g.add_vertices(f"{new_property_dir}/twitter_v_0", "v0")
# g = g.add_vertices(f"{new_property_dir}/twitter_v_1", "v1")
# g = g.add_edges(f"{new_property_dir}/twitter_e_0_0_0", "e0", ["weight"], "v0", "v0")
# g = g.add_edges(f"{new_property_dir}/twitter_e_0_1_0", "e0", ["weight"], "v0", "v1")
# g = g.add_edges(f"{new_property_dir}/twitter_e_1_0_0", "e0", ["weight"], "v1", "v0")
# g = g.add_edges(f"{new_property_dir}/twitter_e_1_1_0", "e0", ["weight"], "v1", "v1")
# g = g.add_edges(f"{new_property_dir}/twitter_e_0_0_1", "e1", ["weight"], "v0", "v0")
# g = g.add_edges(f"{new_property_dir}/twitter_e_0_1_1", "e1", ["weight"], "v0", "v1")
# g = g.add_edges(f"{new_property_dir}/twitter_e_1_0_1", "e1", ["weight"], "v1", "v0")
# g = g.add_edges(f"{new_property_dir}/twitter_e_1_1_1", "e1", ["weight"], "v1", "v1")

# yield g
# del g


# @pytest.fixture(scope="module")
# def arrow_property_graph_only_from_efile(graphscope_session):
# g = graphscope_session.g(generate_eid=False)
# g = g.add_edges(f"{new_property_dir}/twitter_e_0_0_0", "e0", ["weight"], "v0", "v0")
# g = g.add_edges(f"{new_property_dir}/twitter_e_0_1_0", "e0", ["weight"], "v0", "v1")
# g = g.add_edges(f"{new_property_dir}/twitter_e_1_0_0", "e0", ["weight"], "v1", "v0")
# g = g.add_edges(f"{new_property_dir}/twitter_e_1_1_0", "e0", ["weight"], "v1", "v1")
# g = g.add_edges(f"{new_property_dir}/twitter_e_0_0_1", "e1", ["weight"], "v0", "v0")
# g = g.add_edges(f"{new_property_dir}/twitter_e_0_1_1", "e1", ["weight"], "v0", "v1")
# g = g.add_edges(f"{new_property_dir}/twitter_e_1_0_1", "e1", ["weight"], "v1", "v0")
# g = g.add_edges(f"{new_property_dir}/twitter_e_1_1_1", "e1", ["weight"], "v1", "v1")

# yield g
# del g


@pytest.fixture(scope="module")
def arrow_property_graph_undirected(graphscope_session):
    g = graphscope_session.g(directed=False, generate_eid=False)
    g = g.add_vertices(f"{new_property_dir}/twitter_v_0", "v0")
    g = g.add_vertices(f"{new_property_dir}/twitter_v_1", "v1")
    g = g.add_edges(f"{new_property_dir}/twitter_e_0_0_0", "e0", ["weight"], "v0", "v0")
    g = g.add_edges(f"{new_property_dir}/twitter_e_0_1_0", "e0", ["weight"], "v0", "v1")
    g = g.add_edges(f"{new_property_dir}/twitter_e_1_0_0", "e0", ["weight"], "v1", "v0")
    g = g.add_edges(f"{new_property_dir}/twitter_e_1_1_0", "e0", ["weight"], "v1", "v1")
    g = g.add_edges(f"{new_property_dir}/twitter_e_0_0_1", "e1", ["weight"], "v0", "v0")
    g = g.add_edges(f"{new_property_dir}/twitter_e_0_1_1", "e1", ["weight"], "v0", "v1")
    g = g.add_edges(f"{new_property_dir}/twitter_e_1_0_1", "e1", ["weight"], "v1", "v0")
    g = g.add_edges(f"{new_property_dir}/twitter_e_1_1_1", "e1", ["weight"], "v1", "v1")

    yield g
    del g


@pytest.fixture(scope="module")
def arrow_property_graph_lpa_u2i(graphscope_session):
    g = graphscope_session.g(generate_eid=False, directed=True)
    g = g.add_vertices(f"{property_dir}/lpa_dataset/lpa_3000_v_0", "v0")
    g = g.add_vertices(f"{property_dir}/lpa_dataset/lpa_3000_v_1", "v1")
    g = g.add_edges(
        f"{property_dir}/lpa_dataset/lpa_3000_e_0", "e0", ["weight"], "v0", "v1"
    )
    yield g
    del g


@pytest.fixture(scope="module")
def arrow_project_graph(arrow_property_graph):
    pg = arrow_property_graph.project(vertices={"v0": ["id"]}, edges={"e0": ["weight"]})
    yield pg


@pytest.fixture(scope="module")
def arrow_project_undirected_graph(arrow_property_graph_undirected):
    pg = arrow_property_graph_undirected.project(
        vertices={"v0": ["id"]}, edges={"e0": ["weight"]}
    )
    yield pg


@pytest.fixture(scope="module")
def p2p_property_graph(graphscope_session):
    g = graphscope_session.g(generate_eid=False, directed=True)
    g = g.add_vertices(f"{property_dir}/p2p-31_property_v_0", "person")
    g = g.add_edges(
        f"{property_dir}/p2p-31_property_e_0",
        label="knows",
        src_label="person",
        dst_label="person",
    )
    yield g
    del g


@pytest.fixture(scope="module")
def p2p_graph_from_pandas(graphscope_session):
    # set chunk size to 1k
    os.environ["GS_GRPC_CHUNK_SIZE"] = str(1024 - 1)
    df_v = pd.read_csv(f"{property_dir}/p2p-31_property_v_0", sep=",")
    df_e = pd.read_csv(f"{property_dir}/p2p-31_property_e_0", sep=",")
    g = graphscope_session.g(generate_eid=False, directed=False)
    g = g.add_vertices(df_v, "person")
    g = g.add_edges(df_e, label="knows", src_label="person", dst_label="person")
    yield g
    del g


@pytest.fixture(scope="module")
def p2p_property_graph_string(graphscope_session):
    g = graphscope_session.g(oid_type="string", generate_eid=False, directed=True)
    g = g.add_vertices(f"{property_dir}/p2p-31_property_v_0", "person")
    g = g.add_edges(
        f"{property_dir}/p2p-31_property_e_0",
        label="knows",
        src_label="person",
        dst_label="person",
    )
    yield g
    del g


@pytest.fixture(scope="module")
def p2p_property_graph_undirected(graphscope_session):
    g = graphscope_session.g(directed=False, generate_eid=False)
    g = g.add_vertices(f"{property_dir}/p2p-31_property_v_0", "person")
    g = g.add_edges(
        f"{property_dir}/p2p-31_property_e_0",
        label="knows",
        src_label="person",
        dst_label="person",
    )
    yield g
    del g


@pytest.fixture(scope="module")
def p2p_property_graph_undirected_local_vm(graphscope_session):
    g = graphscope_session.g(directed=False, generate_eid=False, vertex_map="local")
    g = g.add_edges(
        f"{property_dir}/p2p-31_property_e_0",
        label="knows",
        src_label="person",
        dst_label="person",
    )
    yield g
    del g


@pytest.fixture(scope="module")
def p2p_property_graph_undirected_local_vm_str(graphscope_session):
    g = graphscope_session.g(
        directed=False, generate_eid=False, vertex_map="local", oid_type="str"
    )
    g = g.add_edges(
        f"{property_dir}/p2p-31_property_e_0",
        label="knows",
        src_label="person",
        dst_label="person",
    )
    yield g
    del g


@pytest.fixture(scope="module")
def p2p_project_directed_graph(p2p_property_graph):
    pg = p2p_property_graph.project(
        vertices={"person": ["weight"]}, edges={"knows": ["dist"]}
    )
    yield pg


@pytest.fixture(scope="module")
def p2p_project_undirected_graph(p2p_property_graph_undirected):
    pg = p2p_property_graph_undirected.project(
        vertices={"person": ["weight"]}, edges={"knows": ["dist"]}
    )
    yield pg


@pytest.fixture(scope="module")
def p2p_project_directed_graph_string(p2p_property_graph_string):
    pg = p2p_property_graph_string.project(
        vertices={"person": ["weight"]}, edges={"knows": ["dist"]}
    )
    yield pg


@pytest.fixture(scope="module")
def projected_pg_no_edge_data(arrow_property_graph):
    pg = arrow_property_graph.project(vertices={"v0": []}, edges={"e0": []})
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
def twitter_sssp_result():
    rlt = np.loadtxt(
        "{}/results/twitter_property_sssp_4".format(property_dir), dtype=float
    )
    yield rlt


@pytest.fixture(scope="module")
def wcc_result():
    ret = np.loadtxt("{}/../p2p-31-wcc_auto".format(property_dir), dtype=int)
    yield ret


@pytest.fixture(scope="module")
def kshell_result():
    ret = np.loadtxt("{}/../p2p-31-kshell-3".format(property_dir), dtype=int)
    yield ret


@pytest.fixture(scope="module")
def pagerank_auto_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/ldbc/p2p-31-PR-directed".format(property_dir), dtype=float
    )
    ret["undirected"] = np.loadtxt(
        "{}/ldbc/p2p-31-PR".format(property_dir), dtype=float
    )
    yield ret


@pytest.fixture(scope="module")
def pagerank_local_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/ldbc/p2p-31-PR-LOCAL-directed".format(property_dir), dtype=float
    )
    ret["undirected"] = np.loadtxt(
        "{}/ldbc/p2p-31-PR-LOCAL".format(property_dir), dtype=float
    )
    yield ret


@pytest.fixture(scope="module")
def hits_result():
    ret = {}
    df = pd.read_csv(
        "{}/../p2p-31-hits-directed".format(property_dir),
        sep="\t",
        header=None,
        prefix="",
    )
    ret["hub"] = df.iloc[:, [0, 1]].to_numpy(dtype=float)
    ret["auth"] = df.iloc[:, [0, 2]].to_numpy(dtype=float)
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
def lpa_result():
    ret = np.loadtxt("{}/ldbc/p2p-31-CDLP".format(property_dir), dtype=int)
    yield ret


@pytest.fixture(scope="module")
def clustering_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/../p2p-31-clustering".format(property_dir), dtype=float
    )
    yield ret


@pytest.fixture(scope="module")
def dc_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/../p2p-31-degree_centrality".format(property_dir), dtype=float
    )
    yield ret


@pytest.fixture(scope="module")
def ev_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/../p2p-31-eigenvector".format(property_dir), dtype=float
    )
    yield ret


@pytest.fixture(scope="module")
def katz_result():
    ret = {}
    ret["directed"] = np.loadtxt("{}/../p2p-31-katz".format(property_dir), dtype=float)
    yield ret


@pytest.fixture(scope="module")
def triangles_result():
    ret = {}
    ret["undirected"] = np.loadtxt(
        "{}/../p2p-31-triangles".format(property_dir),
        dtype=np.int64,
    )
    yield ret


@pytest.fixture(scope="module")
def simple_context(arrow_property_graph):
    sg = arrow_property_graph.project(vertices={"v0": ["id"]}, edges={"e0": ["weight"]})
    return sssp(sg, 20)


@pytest.fixture(scope="module")
def ldbc_graph(graphscope_session):
    graph = load_ldbc(graphscope_session, prefix="{}/ldbc_sample".format(test_repo_dir))
    yield graph
    del graph


@pytest.fixture(scope="module")
def ldbc_graph_undirected(graphscope_session):
    graph = load_ldbc(
        graphscope_session,
        prefix="{}/ldbc_sample".format(test_repo_dir),
        directed=False,
    )
    yield graph
    del graph


@pytest.fixture(scope="module")
def modern_scripts():
    queries = [
        "g.V().has('name','marko').count()",
        "g.V().has('person','name','marko').count()",
        "g.V().has('person','name','marko').outE('created').count()",
        "g.V().has('person','name','marko').outE('created').inV().count()",
        "g.V().has('person','name','marko').out('created').count()",
        "g.V().has('person','name','marko').out('created').values('name').count()",
    ]
    return queries


@pytest.fixture(scope="module")
def modern_bytecode():
    def func(g):
        from gremlin_python.process.traversal import Order
        from gremlin_python.process.traversal import P

        assert g.V().has("name", "marko").count().toList()[0] == 1
        assert g.V().has("person", "name", "marko").count().toList()[0] == 1
        assert (
            g.V().has("person", "name", "marko").outE("created").count().toList()[0]
            == 1
        )
        assert (
            g.V()
            .has("person", "name", "marko")
            .outE("created")
            .inV()
            .count()
            .toList()[0]
            == 1
        )
        assert (
            g.V().has("person", "name", "marko").out("created").count().toList()[0] == 1
        )
        assert (
            g.V()
            .has("person", "name", "marko")
            .out("created")
            .values("name")
            .count()
            .toList()[0]
            == 1
        )
        assert (
            g.V()
            .hasLabel("person")
            .has("age", P.gt(30))
            .order()
            .by("age", Order.desc)
            .count()
            .toList()[0]
            == 2
        )

    return func


def pytest_collection_modifyitems(items):
    for item in items:
        timeout_marker = None
        if hasattr(item, "get_closest_marker"):
            timeout_marker = item.get_closest_marker("timeout")
        elif hasattr(item, "get_marker"):
            timeout_marker = item.get_marker("timeout")
        if timeout_marker is None:
            item.add_marker(pytest.mark.timeout(600))
