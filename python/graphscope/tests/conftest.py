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
        graphscope_session, prefix=f"{test_repo_dir}/modern_graph"
    )
    yield graph
    del graph


@pytest.fixture(scope="module")
def arrow_modern_graph_undirected(graphscope_session):
    graph = load_modern_graph(
        graphscope_session,
        prefix=f"{test_repo_dir}/modern_graph",
        directed=False,
    )
    yield graph
    del graph


@pytest.fixture(scope="module")
def modern_person():
    return f"{test_repo_dir}/modern_graph/person.csv"


@pytest.fixture(scope="module")
def modern_software():
    return f"{test_repo_dir}/modern_graph/software.csv"


@pytest.fixture(scope="module")
def twitter_v_0():
    return f"{new_property_dir}/twitter_v_0"


@pytest.fixture(scope="module")
def modern_graph():
    return f"{test_repo_dir}/modern_graph"


@pytest.fixture(scope="module")
def ldbc_sample():
    return f"{test_repo_dir}/ldbc_sample"


@pytest.fixture(scope="module")
def p2p_property():
    return f"{test_repo_dir}/property"


@pytest.fixture(scope="module")
def ogbn_mag_small():
    return f"{test_repo_dir}/ogbn_mag_small"


@pytest.fixture(scope="module")
def twitter_v_1():
    return f"{new_property_dir}/twitter_v_1"


@pytest.fixture(scope="module")
def twitter_e_0_0_0():
    return f"{new_property_dir}/twitter_e_0_0_0"


@pytest.fixture(scope="module")
def twitter_e_0_1_0():
    return f"{new_property_dir}/twitter_e_0_1_0"


@pytest.fixture(scope="module")
def twitter_e_1_0_0():
    return f"{new_property_dir}/twitter_e_1_0_0"


@pytest.fixture(scope="module")
def twitter_e_1_1_0():
    return f"{new_property_dir}/twitter_e_1_1_0"


@pytest.fixture(scope="module")
def twitter_e_0_0_1():
    return f"{new_property_dir}/twitter_e_0_0_1"


@pytest.fixture(scope="module")
def twitter_e_0_1_1():
    return f"{new_property_dir}/twitter_e_0_1_1"


@pytest.fixture(scope="module")
def twitter_e_1_0_1():
    return f"{new_property_dir}/twitter_e_1_0_1"


@pytest.fixture(scope="module")
def twitter_e_1_1_1():
    return f"{new_property_dir}/twitter_e_1_1_1"


def load_arrow_property_graph(session, directed=True):
    return session.load_from(
        edges={
            "e0": [
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_0_0_0",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_0_1_0",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_1_0_0",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_1_1_0",
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
                        f"{new_property_dir}/twitter_e_0_0_1",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_0_1_1",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_1_0_1",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_1_1_1",
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
            "v0": Loader(f"{new_property_dir}/twitter_v_0", header_row=True),
            "v1": Loader(f"{new_property_dir}/twitter_v_1", header_row=True),
        },
        generate_eid=False,
        retain_oid=True,
        directed=directed,
    )


@pytest.fixture(scope="module")
def arrow_property_graph(graphscope_session):
    g = load_arrow_property_graph(graphscope_session, directed=True)
    yield g
    del g


@pytest.fixture(scope="module")
def arrow_property_graph_undirected(graphscope_session):
    g = load_arrow_property_graph(graphscope_session, directed=False)
    yield g
    del g


@pytest.fixture(scope="module")
def arrow_property_graph_directed(graphscope_session):
    g = load_arrow_property_graph(graphscope_session, directed=True)
    yield g
    del g


@pytest.fixture(scope="module")
def arrow_property_graph_only_from_efile(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "e0": [
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_0_0_0",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_0_1_0",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_1_0_0",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_1_1_0",
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
                        f"{new_property_dir}/twitter_e_0_0_1",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_0_1_1",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v0"),
                    ("dst", "v1"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_1_0_1",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["weight"],
                    ("src", "v1"),
                    ("dst", "v0"),
                ),
                (
                    Loader(
                        f"{new_property_dir}/twitter_e_1_1_1",
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
        retain_oid=True,
    )
    yield g
    del g


@pytest.fixture(scope="module")
def arrow_property_graph_lpa_u2i(graphscope_session):
    g = graphscope_session.g(generate_eid=False, retain_oid=False, directed=True)
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
    g = graphscope_session.g(generate_eid=False, retain_oid=True, directed=True)
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
def p2p_multi_property_graph(graphscope_session):
    g = graphscope_session.g(generate_eid=False, retain_oid=True, directed=True)
    g = g.add_vertices(f"{property_dir}/p2p-31_multi_property_v_0", "person")
    g = g.add_edges(
        f"{property_dir}/p2p-31_multi_property_e_0",
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
    g = graphscope_session.g(generate_eid=False, retain_oid=False, directed=False)
    g = g.add_vertices(df_v, "person")
    g = g.add_edges(df_e, label="knows", src_label="person", dst_label="person")
    yield g
    del g


@pytest.fixture(scope="module")
def p2p_property_graph_string(graphscope_session):
    g = graphscope_session.g(
        oid_type="string", generate_eid=False, retain_oid=True, directed=True
    )
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
def p2p_property_graph_int32(graphscope_session):
    g = graphscope_session.g(
        oid_type="int32", generate_eid=False, retain_oid=True, directed=True
    )
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
def p2p_property_graph_uint32_vid(graphscope_session):
    g = graphscope_session.g(
        vid_type="uint32", generate_eid=False, retain_oid=True, directed=True
    )
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
    g = graphscope_session.g(directed=False, generate_eid=False, retain_oid=False)
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
def p2p_property_graph_undirected_string(graphscope_session):
    g = graphscope_session.g(
        oid_type="string", directed=False, generate_eid=False, retain_oid=False
    )
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
    g = graphscope_session.g(
        directed=False, generate_eid=False, retain_oid=True, vertex_map="local"
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
def p2p_property_graph_undirected_local_vm_string(graphscope_session):
    g = graphscope_session.g(
        directed=False,
        generate_eid=False,
        retain_oid=True,
        vertex_map="local",
        oid_type="str",
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
def p2p_property_graph_undirected_local_vm_int32(graphscope_session):
    g = graphscope_session.g(
        directed=False,
        generate_eid=False,
        retain_oid=True,
        vertex_map="local",
        oid_type="int32",
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
def p2p_property_graph_undirected_compact(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "knows": [
                (
                    Loader(
                        f"{property_dir}/p2p-31_property_e_0",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["dist"],
                    ("src_id", "person"),
                    ("dst_id", "person"),
                ),
            ],
        },
        vertices={
            "person": Loader(
                f"{property_dir}/p2p-31_property_v_0",
                header_row=True,
                delimiter=",",
            ),
        },
        generate_eid=False,
        retain_oid=True,
        directed=False,
        compact_edges=True,
    )
    yield g
    del g


@pytest.fixture(scope="module")
def p2p_property_graph_undirected_perfect_hash(graphscope_session):
    g = graphscope_session.load_from(
        edges={
            "knows": [
                (
                    Loader(
                        f"{property_dir}/p2p-31_property_e_0",
                        header_row=True,
                        delimiter=",",
                    ),
                    ["dist"],
                    ("src_id", "person"),
                    ("dst_id", "person"),
                ),
            ],
        },
        vertices={
            "person": Loader(
                f"{property_dir}/p2p-31_property_v_0",
                header_row=True,
                delimiter=",",
            ),
        },
        generate_eid=False,
        retain_oid=True,
        directed=False,
        compact_edges=False,
        use_perfect_hash=True,
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
def p2p_project_undirected_graph_string(p2p_property_graph_undirected_string):
    pg = p2p_property_graph_undirected_string.project(
        vertices={"person": ["weight"]}, edges={"knows": ["dist"]}
    )
    yield pg


@pytest.fixture(scope="module")
def p2p_project_directed_graph_int32(p2p_property_graph_int32):
    pg = p2p_property_graph_int32.project(
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
        f"{property_dir}/ldbc/p2p-31-SSSP-directed", dtype=float
    )
    ret["undirected"] = np.loadtxt(f"{property_dir}/ldbc/p2p-31-SSSP", dtype=float)
    yield ret


@pytest.fixture(scope="module")
def twitter_sssp_result():
    rlt = np.loadtxt(f"{property_dir}/results/twitter_property_sssp_4", dtype=float)
    yield rlt


@pytest.fixture(scope="module")
def wcc_result():
    ret = np.loadtxt(f"{property_dir}/ldbc/p2p-31-WCC", dtype=int)
    yield ret


@pytest.fixture(scope="module")
def wcc_auto_result():
    ret = np.loadtxt(f"{property_dir}/../p2p-31-wcc_auto", dtype=int)
    yield ret


@pytest.fixture(scope="module")
def pagerank_result():
    ret = {}
    ret["directed"] = np.loadtxt(f"{property_dir}/ldbc/p2p-31-PR-directed", dtype=float)
    ret["undirected"] = np.loadtxt(f"{property_dir}/ldbc/p2p-31-PR", dtype=float)
    yield ret


@pytest.fixture(scope="module")
def bfs_result():
    ret = {}
    ret["directed"] = np.loadtxt(
        "{}/ldbc/p2p-31-BFS-directed".format(property_dir), dtype=int
    )
    ret["undirected"] = np.loadtxt(f"{property_dir}/ldbc/p2p-31-BFS", dtype=int)
    yield ret


@pytest.fixture(scope="module")
def lpa_result():
    ret = np.loadtxt(f"{property_dir}/ldbc/p2p-31-CDLP", dtype=int)
    yield ret


@pytest.fixture(scope="module")
def clustering_result():
    ret = {}
    ret["directed"] = np.loadtxt(f"{property_dir}/../p2p-31-clustering", dtype=float)
    yield ret


@pytest.fixture(scope="module")
def lcc_result():
    ret = {}
    ret["undirected"] = np.loadtxt(f"{property_dir}/ldbc/p2p-31-LCC", dtype=float)
    yield ret


@pytest.fixture(scope="module")
def kshell_result():
    ret = np.loadtxt(f"{property_dir}/../p2p-31-kshell-3", dtype=int)
    yield ret


@pytest.fixture(scope="module")
def hits_result():
    ret = {}
    df = pd.read_csv(
        "{}/../p2p-31-hits-directed".format(property_dir),
        sep="\t",
        header=None,
    )
    ret["hub"] = df.iloc[:, [0, 1]].to_numpy(dtype=float)
    ret["auth"] = df.iloc[:, [0, 2]].to_numpy(dtype=float)
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
