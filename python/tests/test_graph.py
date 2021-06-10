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

import logging
import os

import numpy as np
import pytest
import vineyard

import graphscope
from graphscope import property_sssp
from graphscope import sssp
from graphscope.dataset.ldbc import load_ldbc
from graphscope.framework.errors import AnalyticalEngineInternalError
from graphscope.framework.errors import GRPCError
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.graph import Graph
from graphscope.framework.loader import Loader
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2

logger = logging.getLogger("graphscope")
prefix = os.path.expandvars("${GS_TEST_DIR}")


def test_graph_schema(arrow_property_graph):
    schema = arrow_property_graph.schema
    assert schema.vertex_labels == ["v0", "v1"]
    assert schema.edge_labels == ["e0", "e1"]


def test_load_graph_copy(graphscope_session, arrow_property_graph):
    g = arrow_property_graph
    g2 = graphscope_session.g(g)
    assert g.key != g2.key
    assert g.vineyard_id != g2.vineyard_id
    assert str(g.schema) == str(g2.schema)
    assert np.all(g.to_numpy("v:v0.id") == g2.to_numpy("v:v0.id"))
    g2.unload()
    assert not g2.loaded()
    # test load from vineyard's graph
    g3 = graphscope_session.g(vineyard.ObjectID(g.vineyard_id))
    assert g3.loaded()


def test_project_to_simple_with_name(p2p_property_graph, sssp_result):
    pg = p2p_property_graph.project(
        vertices={"person": ["weight"]}, edges={"knows": ["dist"]}
    )
    ctx = sssp(pg, src=6)
    r = (
        ctx.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r[r == 1.7976931348623157e308] = float("inf")  # replace limit::max with inf
    assert np.allclose(r, sssp_result["directed"])


def test_project_to_simple_with_id(p2p_property_graph, sssp_result):
    pg = p2p_property_graph.project(
        vertices={"person": ["id"]}, edges={"knows": ["dist"]}
    )
    ctx = sssp(pg, src=6)
    r = (
        ctx.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r[r == 1.7976931348623157e308] = float("inf")  # replace limit::max with inf
    assert np.allclose(r, sssp_result["directed"])


def test_error_label_on_project_to_simple(arrow_property_graph):
    g = arrow_property_graph
    # g has vertex labels: v0, v1, v2, v3, each label has a property: weight
    # g has edge label: e0, e1, each label has a property: dist
    with pytest.raises(KeyError, match="v4"):
        pg = g.project(vertices={"v4": []}, edges={"e0": []})
        pg._project_to_simple()

    with pytest.raises(KeyError, match="e2"):
        pg = g.project(vertices={"v0": []}, edges={"e2": []})
        pg._project_to_simple()

    with pytest.raises(KeyError, match="foo"):
        pg = g.project(vertices={"v0": ["foo"]}, edges={"e0": []})
        pg._project_to_simple()

    with pytest.raises(KeyError, match="foo"):
        pg = g.project(vertices={"v0": []}, edges={"e0": ["foo"]})
        pg._project_to_simple()


def test_error_relationship_on_project_to_simple(arrow_modern_graph):
    g = arrow_modern_graph
    with pytest.raises(
        ValueError,
        match="Cannot find a valid relation",
    ):
        pg = g.project(vertices={"person": []}, edges={"created": []})
        pg._project_to_simple()
    with pytest.raises(
        ValueError,
        match="Cannot find a valid relation",
    ):
        pg = g.project(vertices={"software": []}, edges={"knows": []})
        pg._project_to_simple()


def test_unload(graphscope_session):
    graph = graphscope_session.g()
    prefix = os.path.expandvars("${GS_TEST_DIR}/property")
    graph = (
        graphscope_session.g()
        .add_vertices(f"{prefix}/p2p-31_property_v_0", "person")
        .add_edges(f"{prefix}/p2p-31_property_e_0", "knows")
    )
    assert graph.loaded()
    assert graph.vineyard_id is not None
    graph.unload()

    assert not graph.loaded()

    with pytest.raises(RuntimeError, match="The graph is not loaded"):
        graph.unload()

    with pytest.raises(RuntimeError, match="The graph is not loaded"):
        pg = graph.project(vertices={"person": []}, edges={"knows": []})
        pg._project_to_simple()
    with pytest.raises(AssertionError):
        g2 = graphscope_session.g(graph)
    with pytest.raises(RuntimeError, match="The graph is not loaded"):
        property_sssp(graph, src=6)


def test_error_on_project_to_simple_wrong_graph_type(arrow_property_graph):
    sg = arrow_property_graph.project(vertices={"v0": []}, edges={"e0": []})
    pg = sg._project_to_simple()
    assert pg._graph_type == graph_def_pb2.ARROW_PROJECTED
    with pytest.raises(InvalidArgumentError):
        pg._project_to_simple()
    with pytest.raises(InvalidArgumentError):
        pg.project(vertices={"v0": []}, edges={"e0": []})


@pytest.mark.skipif(
    os.environ.get("NETWORKX") != "ON", reason="dynamic graph is in NETWORKX ON"
)
def test_error_on_project_to_simple_wrong_graph_type_2(dynamic_property_graph):
    sdg = dynamic_property_graph.project_to_simple()
    assert sdg._graph_type == graph_def_pb2.DYNAMIC_PROJECTED
    with pytest.raises(AssertionError):
        sdg.project_to_simple()


def test_error_on_operation_on_graph(graphscope_session):
    g = graphscope_session.g()
    with pytest.raises(KeyError, match="v"):
        pg = g.project(vertices={"v": []}, edges={"e": []})
        pg._project_to_simple()._ensure_loaded()


def test_error_on_app_query_non_compatible_graph(arrow_property_graph):
    pg = arrow_property_graph.project(vertices={"v0": []}, edges={"e0": []})
    # edata is empty, not compatible with sssp
    with pytest.raises(graphscope.framework.errors.CompilationError):
        sssp(pg, 4)


@pytest.mark.skip(reason="appendonly graph not ready.")
def test_append_only_graph():
    g = Graph
    ag = gs.to_appendable(g)
    g2 = gs.to_immutable(g)
    assert g == g2
    # FIXME:  open is a builtin function
    kr = open("kafka://xxxx", "r")
    kw = open("kafka://yyyy", "w")
    while kr.hasNextChunk():
        queries, updates = ProcessKafka(kr.nextChunk())
        ag.extend(updates)
    result = sampler(ag, queries)
    kw.output(result)


@pytest.mark.skip(reason="appendonly graph not ready.")
def test_error_on_append_graph():
    ag = get_append_only_graph()
    with pytest.raises(RuntimeError, match="data format is not supported"):
        ag.extend("not recognized data format")  # should be returned by ProcessKafka


@pytest.mark.skip(reason="appendonly graph not ready.")
def test_error_on_transform_graph():
    g = Graph()
    ag = gs.to_appendable(g)
    with pytest.raises(AssertionError, match="expect source graph is immutable"):
        agg = gs.to_appendable(ag)

    g2 = gs.to_immutable(ag)
    with pytest.raises(AssertionError, match="expect source graph is appendable"):
        g3 = gs.to_immutable(g2)


def test_load_only_from_efile(
    arrow_property_graph, arrow_property_graph_only_from_efile
):
    sg1 = arrow_property_graph.project(vertices={"v0": []}, edges={"e0": ["weight"]})
    sg2 = arrow_property_graph_only_from_efile.project(
        vertices={"v0": []}, edges={"e0": ["weight"]}
    )
    simple_ctx1 = sssp(sg1, 20)
    simple_ctx2 = sssp(sg2, 20)
    v_out1 = simple_ctx1.to_numpy("v.id")
    v_out2 = simple_ctx2.to_numpy("v.id")
    assert v_out1.shape == (40521,)
    assert v_out1.shape == v_out2.shape
    assert sorted(v_out1) == sorted(v_out2)
    r_out1 = simple_ctx1.to_numpy("r")
    r_out2 = simple_ctx2.to_numpy("r")
    assert r_out1.shape == (40521,)
    assert r_out1.shape == r_out2.shape
    assert sorted(r_out1) == sorted(r_out2)


def test_graph_to_numpy(arrow_property_graph):
    g = arrow_property_graph
    ret = property_sssp(g, 20)
    ctx_out_np_0 = ret.to_numpy("r:v0.dist_0")
    logger.info("ctx_out_np_0.shape = %r", ctx_out_np_0.shape)
    ctx_out_np_1 = ret.to_numpy("r:v1.dist_1")
    logger.info("ctx_out_np_1.shape = %r", ctx_out_np_1.shape)
    g2 = g.add_column(ret, {"result_0": "r:v0.dist_0", "result_1": "r:v1.dist_1"})
    out_np_0 = g2.to_numpy("v:v0.result_0")
    logger.info("out_np_0.shape = %r", out_np_0.shape)
    out_np_1 = g2.to_numpy("v:v1.result_1")
    logger.info("out_np_1.shape = %r", out_np_1.shape)


def test_graph_to_dataframe(arrow_property_graph):
    g = arrow_property_graph
    ret = property_sssp(g, 20)
    g2 = g.add_column(ret, {"result_0": "r:v0.dist_0", "result_1": "r:v1.dist_1"})
    out_df_0 = g2.to_dataframe({"id": "v:v0.id", "result": "v:v0.result_0"})
    logger.info("out_df_0.shape = %r", out_df_0.shape)
    out_df_1 = g2.to_dataframe({"id": "v:v1.id", "result": "v:v1.result_1"})
    logger.info("out_df_1.shape = %r", out_df_1.shape)


def test_error_on_add_column(arrow_property_graph, property_context):
    with pytest.raises(KeyError, match="non_exist_label"):
        out = arrow_property_graph.add_column(
            property_context,
            {"id": "v:non_exist_label.id", "result": "r:non_exist_label.age"},
        )

    with pytest.raises(KeyError, match="non_exist_prop"):
        out = arrow_property_graph.add_column(
            property_context, {"id": "v:v0.non_exist_prop"}
        )

    with pytest.raises(AssertionError, match="selector of add column must be a dict"):
        out = arrow_property_graph.add_column(property_context, selector=None)

    with pytest.raises(SyntaxError, match="Invalid selector"):
        out = arrow_property_graph.add_column(property_context, {"id": "xxx:a.b"})


@pytest.mark.skip(reason="Issue 366")
def test_project_to_simple_string_vprop(arrow_modern_graph):
    sg = arrow_modern_graph.project(
        vertices={"person": ["name"]}, edges={"knows": ["weight"]}
    )
    assert sg


@pytest.mark.skip(reason="String edata is not supported")
def test_project_to_simple_string_eprop(graphscope_session):
    data_dir = os.path.expandvars("${GS_TEST_DIR}/load_ldbc")
    g = load_ldbc(graphscope_session, data_dir)
    sg = g.project(
        vertices={"person": ["firstName"]}, edges={"knows": ["creationDate"]}
    )
    sg._project_to_simple()._ensure_loaded()


def test_add_vertices_edges(graphscope_session):
    prefix = os.path.expandvars("${GS_TEST_DIR}/modern_graph")
    graph = graphscope_session.g()
    graph = graph.add_vertices(Loader(f"{prefix}/person.csv", delimiter="|"), "person")
    graph = graph.add_edges(Loader(f"{prefix}/knows.csv", delimiter="|"), "knows")

    assert graph.schema.vertex_labels == ["person"]
    assert graph.schema.edge_labels == ["knows"]

    with pytest.raises(ValueError, match="src label or dst_label not existed in graph"):
        graph = graph.add_edges(
            Loader(f"{prefix}/created.csv", delimiter="|"),
            "created",
            src_label="person",
            dst_label="software",
        )

    graph = graph.add_vertices(
        Loader(f"{prefix}/software.csv", delimiter="|"), "software"
    )
    with pytest.raises(ValueError, match="Ambiguous vertex label"):
        graph = graph.add_edges(Loader(f"{prefix}/knows.csv", delimiter="|"), "created")

    with pytest.raises(ValueError, match="already existed in graph"):
        graph = graph.add_edges(
            Loader(f"{prefix}/knows.csv", delimiter="|"),
            "knows",
            src_label="software",
            dst_label="software",
        )

    graph = graph.add_edges(
        Loader(f"{prefix}/created.csv", delimiter="|"),
        "created",
        src_label="person",
        dst_label="software",
    )

    assert graph.schema.vertex_labels == ["person", "software"]
    assert graph.schema.edge_labels == ["knows", "created"]


@pytest.mark.skip("use project to simulate remove.")
def test_error_on_remove_vertices_edges(graphscope_session):
    prefix = os.path.expandvars("${GS_TEST_DIR}/modern_graph")
    graph = graphscope_session.g()
    graph = graph.add_vertices(Loader(f"{prefix}/person.csv", delimiter="|"), "person")
    graph = graph.add_edges(Loader(f"{prefix}/knows.csv", delimiter="|"), "knows")

    graph = graph.add_vertices(
        Loader(f"{prefix}/software.csv", delimiter="|"), "software"
    )
    graph = graph.add_edges(
        Loader(f"{prefix}/created.csv", delimiter="|"),
        "created",
        src_label="person",
        dst_label="software",
    )

    with pytest.raises(ValueError, match="Vertex software has usage in relation"):
        graph = graph.remove_vertices("software")

    with pytest.raises(ValueError, match="label xxx not in vertices"):
        graph = graph.remove_vertices("xxx")
    with pytest.raises(ValueError, match="label xxx not in edges"):
        graph = graph.remove_edges("xxx")
    with pytest.raises(ValueError, match="Cannot find edges to remove"):
        graph = graph.remove_edges("knows", src_label="xxx", dst_label="xxx")

    assert graph.loaded()
    with pytest.raises(
        ValueError, match="Remove vertices from a loaded graph doesn't supported yet"
    ):
        graph = graph.remove_vertices("person")
    with pytest.raises(
        ValueError, match="Remove edges from a loaded graph doesn't supported yet"
    ):
        graph = graph.remove_edges("knows")


@pytest.mark.skip("use project to simulate remove.")
def test_remove_vertices_edges(graphscope_session):
    prefix = os.path.expandvars("${GS_TEST_DIR}/modern_graph")
    graph = (
        graphscope_session.g()
        .add_vertices(Loader(f"{prefix}/person.csv", delimiter="|"), "person")
        .add_edges(Loader(f"{prefix}/knows.csv", delimiter="|"), "knows")
    )

    another_graph = graph.add_vertices(
        Loader(f"{prefix}/software.csv", delimiter="|"), "software"
    ).add_edges(
        Loader("{prefix}/created.csv", delimiter="|"),
        "created",
        src_label="person",
        dst_label="software",
    )

    another_graph = another_graph.remove_edges("created")
    another_graph = another_graph.remove_vertices("software")

    assert graph.schema.vertex_labels == another_graph.schema.vertex_labels
    assert graph.schema.edge_labels == another_graph.schema.edge_labels


def test_multiple_add_vertices_edges(graphscope_session):
    prefix = os.path.expandvars("${GS_TEST_DIR}/modern_graph")
    graph = graphscope_session.g()
    graph = graph.add_vertices(Loader(f"{prefix}/person.csv", delimiter="|"), "person")
    graph = graph.add_edges(Loader(f"{prefix}/knows.csv", delimiter="|"), "knows")
    graph = graph.add_vertices(
        Loader(f"{prefix}/software.csv", delimiter="|"), "software"
    )
    graph = graph.add_edges(
        Loader(f"{prefix}/created.csv", delimiter="|"),
        "created",
        src_label="person",
        dst_label="software",
    )

    assert graph.schema.vertex_labels == ["person", "software"]
    assert graph.schema.edge_labels == ["knows", "created"]

    graph = graph.add_vertices(Loader(f"{prefix}/person.csv", delimiter="|"), "person2")
    graph = graph.add_edges(
        Loader(f"{prefix}/knows.csv", delimiter="|"),
        "knows2",
        src_label="person2",
        dst_label="person2",
    )
    assert sorted(graph.schema.vertex_labels) == [
        "person",
        "person2",
        "software",
    ]
    assert sorted(graph.schema.edge_labels) == [
        "created",
        "knows",
        "knows2",
    ]
    graph = graph.add_vertices(
        Loader(f"{prefix}/software.csv", delimiter="|"), "software2"
    )
    graph = graph.add_edges(
        Loader(f"{prefix}/created.csv", delimiter="|"),
        "created2",
        src_label="person2",
        dst_label="software2",
    )

    assert sorted(graph.schema.vertex_labels) == [
        "person",
        "person2",
        "software",
        "software2",
    ]
    assert sorted(graph.schema.edge_labels) == [
        "created",
        "created2",
        "knows",
        "knows2",
    ]


def test_project_subgraph(arrow_modern_graph):
    graph = arrow_modern_graph

    sub_graph = graph.project(vertices={}, edges={})
    assert sub_graph.schema.vertex_labels == []
    assert sub_graph.schema.edge_labels == []
    with pytest.raises(
        ValueError,
        match="Check failed: Cannot project to simple, vertex label number is not one.",
    ):
        graphscope.wcc(sub_graph)

    # project a sub_graph only contain person nodes
    sub_graph = graph.project(vertices={"person": None}, edges={})
    assert sub_graph.schema.vertex_labels == ["person"]
    assert sub_graph.schema.edge_labels == []
    with pytest.raises(
        ValueError,
        match="Check failed: Cannot project to simple, edge label number is not one.",
    ):
        graphscope.wcc(sub_graph)

    graph = graph.project(
        vertices={"person": None, "software": ["name", "id"]},
        edges={"created": ["eid", "weight"], "knows": None},
    )
    assert graph.schema.vertex_labels == ["person", "software"]
    assert graph.schema.edge_labels == ["knows", "created"]
    assert [p.id for p in graph.schema.get_vertex_properties("person")] == [0, 1, 2]
    assert [p.id for p in graph.schema.get_vertex_properties("software")] == [0, 2]
    assert [p.id for p in graph.schema.get_edge_properties("created")] == [0, 1]
    assert [p.id for p in graph.schema.get_edge_properties("knows")] == [0, 1]

    graph = graph.project(edges={"knows": ["eid"]}, vertices={"person": None})

    assert graph.schema.vertex_labels == ["person"]
    assert graph.schema.edge_labels == ["knows"]
    assert [p.id for p in graph.schema.get_vertex_properties("person")] == [0, 1, 2]
    assert [p.id for p in graph.schema.get_edge_properties("knows")] == [0]

    with pytest.raises(ValueError, match="weight not exist in properties"):
        graph = graph.project(edges={"knows": ["weight"]}, vertices={"person": []})

    graph = graph.project(edges={"knows": []}, vertices={"person": []})
    assert not graph.schema.get_vertex_properties("person")
    assert not graph.schema.get_edge_properties("knows")

    ret = graphscope.wcc(graph)
    graph = graph.add_column(ret, {"cc": "r"})
    assert len(graph.schema.get_vertex_properties("person")) == 1
    assert graph.schema.get_vertex_properties("person")[0].name == "cc"


def test_error_on_project(arrow_property_graph, ldbc_graph):
    graph = arrow_property_graph
    with pytest.raises(ValueError, match="Cannot project to simple"):
        graphscope.sssp(graph, 4)
    g2 = graph.project(vertices={"v0": []}, edges={"e0": []})
    assert g2.schema.edge_relationships == [[("v0", "v0")]]

    ldbc = ldbc_graph
    # vertices empty
    with pytest.raises(
        ValueError,
        match="Cannot find a valid relation in given vertices and edges",
    ):
        sub_graph = ldbc.project(vertices={}, edges={"knows": None})

    # project not related vertex and edge
    with pytest.raises(
        ValueError,
        match="Cannot find a valid relation in given vertices and edges",
    ):
        sub_graph = ldbc.project(vertices={"person": None}, edges={"hasInterest": None})

    sub_graph = ldbc.project(
        vertices={"person": None, "tag": None, "tagclass": None},
        edges={"knows": None, "hasInterest": None},
    )

    # project with not existed vertex
    with pytest.raises(ValueError, match="comment not exists"):
        sub_graph.project(vertices={"comment": None}, edges={"knows": None})

    # project with not existed edge
    with pytest.raises(ValueError, match="isSubclassOf not exists"):
        sub_graph.project(vertices={"tagclass": None}, edges={"isSubclassOf": None})

    # more than one property on vertex can not project to simple
    sub_graph = ldbc.project(
        vertices={"person": ["id", "firstName"]}, edges={"knows": ["eid"]}
    )
    with pytest.raises(ValueError):
        sub_graph._project_to_simple()

    # more than one property on edge can not project to simple
    sub_graph = ldbc.project(
        vertices={"person": ["id"]}, edges={"knows": ["eid", "creationDate"]}
    )
    with pytest.raises(ValueError):
        sub_graph._project_to_simple()


def test_add_column(ldbc_graph, arrow_modern_graph):
    ldbc = ldbc_graph
    modern = arrow_modern_graph

    sub_graph_1 = ldbc.project(vertices={"person": []}, edges={"knows": ["eid"]})
    sub_graph_2 = ldbc.project(
        vertices={"person": None, "tag": None}, edges={"hasInterest": None}
    )
    sub_graph_3 = ldbc.project(
        vertices={"tag": None, "tagclass": None}, edges={"hasType": None}
    )
    sub_graph_4 = modern.project(vertices={"person": []}, edges={"knows": ["eid"]})

    ret = graphscope.wcc(sub_graph_1)

    # the ret can add to the graph queried on
    g1 = sub_graph_1.add_column(ret, selector={"cc": "r"})
    assert g1.schema.get_vertex_properties("person")[0].id == 8
    assert g1.schema.get_vertex_properties("person")[0].name == "cc"
    # the ret can add to the origin graph
    g2 = ldbc.add_column(ret, selector={"cc": "r"})
    assert g2.schema.get_vertex_properties("person")[8].id == 8
    assert g2.schema.get_vertex_properties("person")[8].name == "cc"
    # the ret can add to the graph tha contain the same vertex label with sub_graph_1
    g3 = sub_graph_2.add_column(ret, selector={"cc": "r"})
    assert g3.schema.get_vertex_properties("person")[8].id == 8
    assert g3.schema.get_vertex_properties("person")[8].name == "cc"
    # the ret can not add to sub_graph_3
    with pytest.raises(AnalyticalEngineInternalError):
        g4 = sub_graph_3.add_column(ret, selector={"cc": "r"})
        print(g4.schema)
    # the ret can not add to sub_graph_4
    with pytest.raises(AnalyticalEngineInternalError):
        g5 = sub_graph_4.add_column(ret, selector={"cc": "r"})
        print(g4.schema)

    # sub_graph_5 = sub_graph_3.add_vertices(
    #    Loader(os.path.join(prefix, "ldbc_sample/person_0_0.csv"), delimiter="|"),
    #    "person",
    #    [
    #        "firstName",
    #        "lastName",
    #        "gender",
    #        "birthday",
    #        "creationDate",
    #        "locationIP",
    #        "browserUsed",
    #    ],
    #    "id",
    # )
    # FIXME: raise error in add_column
    # g6 = sub_graph_5.add_column(ret, selector={"cc": "r"})
    # with pytest.raises(AnalyticalEngineInternalError):
    #     print(g6.schema)
