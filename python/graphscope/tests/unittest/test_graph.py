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

import graphscope
from graphscope import Graph
from graphscope import lpa_u2i
from graphscope import sssp
from graphscope.dataset import load_ldbc
from graphscope.dataset import load_ogbn_mag
from graphscope.dataset import load_p2p_network
from graphscope.framework.errors import AnalyticalEngineInternalError
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.loader import Loader
from graphscope.proto import graph_def_pb2

logger = logging.getLogger("graphscope")
prefix = os.path.expandvars("${GS_TEST_DIR}")


def test_graph_schema(arrow_property_graph):
    schema = arrow_property_graph.schema
    assert schema.vertex_labels == ["v0", "v1"]
    assert schema.edge_labels == ["e0", "e1"]


def test_graph_schema_todict(p2p_property_graph):
    rlt = {
        "vertices": [
            {
                "label": "person",
                "properties": [
                    {
                        "name": "weight",
                        "id": 0,
                        "type": "LONG",
                        "is_primary_key": False,
                        "comment": "",
                    },
                    {
                        "name": "id",
                        "id": 1,
                        "type": "LONG",
                        "is_primary_key": False,
                        "comment": "",
                    },
                ],
            }
        ],
        "edges": [
            {
                "label": "knows",
                "properties": [
                    {
                        "name": "src_label_id",
                        "id": 0,
                        "type": "LONG",
                        "is_primary_key": False,
                        "comment": "",
                    },
                    {
                        "name": "dst_label_id",
                        "id": 1,
                        "type": "LONG",
                        "is_primary_key": False,
                        "comment": "",
                    },
                    {
                        "name": "dist",
                        "id": 2,
                        "type": "LONG",
                        "is_primary_key": False,
                        "comment": "",
                    },
                ],
                "relations": [{"src_label": "person", "dst_label": "person"}],
            }
        ],
    }
    assert p2p_property_graph.schema.to_dict() == rlt


def test_load_graph_copy(graphscope_session, arrow_property_graph):
    import vineyard

    g = arrow_property_graph
    g2 = graphscope_session.g(g)
    assert g.key != g2.key
    assert g.vineyard_id != g2.vineyard_id
    assert str(g.schema) == str(g2.schema)
    assert np.all(g.to_numpy("v:v0.id") == g2.to_numpy("v:v0.id"))
    del g2
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
    g = load_p2p_network(graphscope_session)
    assert g.loaded()
    assert g.vineyard_id is not None
    del g


def test_error_on_project_to_simple_wrong_graph_type(arrow_property_graph):
    sg = arrow_property_graph.project(vertices={"v0": []}, edges={"e0": []})
    pg = sg._project_to_simple()
    assert pg._graph_type == graph_def_pb2.ARROW_PROJECTED
    with pytest.raises(InvalidArgumentError):
        pg._project_to_simple()
    with pytest.raises(InvalidArgumentError):
        pg.project(vertices={"v0": []}, edges={"e0": []})


def test_error_on_operation_on_graph(graphscope_session):
    g = graphscope_session.g()
    with pytest.raises(KeyError, match="v"):
        pg = g.project(vertices={"v": []}, edges={"e": []})
        pg._project_to_simple()._ensure_loaded()


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


def test_error_on_add_column(arrow_property_graph_lpa_u2i):
    property_context = lpa_u2i(arrow_property_graph_lpa_u2i, max_round=20)

    with pytest.raises(KeyError, match="non_exist_label"):
        out = arrow_property_graph_lpa_u2i.add_column(
            property_context,
            {"id": "v:non_exist_label.id", "result": "r:non_exist_label.age"},
        )

    with pytest.raises(KeyError, match="non_exist_prop"):
        out = arrow_property_graph_lpa_u2i.add_column(
            property_context, {"id": "v:v0.non_exist_prop"}
        )

    with pytest.raises(AssertionError, match="selector of add column must be a dict"):
        out = arrow_property_graph_lpa_u2i.add_column(property_context, selector=None)

    with pytest.raises(SyntaxError, match="Invalid selector"):
        out = arrow_property_graph_lpa_u2i.add_column(
            property_context, {"id": "xxx:a.b"}
        )


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

    graph = graph.add_vertices(
        Loader(f"{prefix}/software.csv", delimiter="|"), "software"
    )

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


def test_complicated_add_edges(graphscope_session):
    prefix = os.path.expandvars("${GS_TEST_DIR}/modern_graph")
    graph = graphscope_session.g()
    graph = graph.add_vertices(Loader(f"{prefix}/person.csv", delimiter="|"), "person")
    graph = graph.add_edges(
        Loader(f"{prefix}/knows.csv", delimiter="|"),
        "knows",
        src_label="v1",
        dst_label="v1",
    )
    assert "v1" in graph.schema.vertex_labels

    graph = graph.add_edges(
        Loader(f"{prefix}/knows.csv", delimiter="|"),
        "knows2",
        src_label="v1",
        dst_label="v2",
    )
    assert "v2" in graph.schema.vertex_labels

    with pytest.raises(AssertionError, match="Ambiguous vertex label"):
        graph = graph.add_edges(Loader(f"{prefix}/knows.csv", delimiter="|"), "knows")


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
        RuntimeError,
        match="Failed to project to simple graph as no vertex exists in this graph",
    ):
        graphscope.pagerank(sub_graph)

    # project a sub_graph only contain person nodes
    sub_graph = graph.project(vertices={"person": None}, edges={})
    assert sub_graph.schema.vertex_labels == ["person"]
    assert sub_graph.schema.edge_labels == []
    with pytest.raises(
        RuntimeError,
        match="Failed to project to simple graph as no edge exists in this graph",
    ):
        graphscope.pagerank(sub_graph)

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

    ret = graphscope.pagerank(graph)
    graph = graph.add_column(ret, {"pr": "r"})
    assert len(graph.schema.get_vertex_properties("person")) == 1
    assert graph.schema.get_vertex_properties("person")[0].name == "pr"


def test_project_project(ldbc_graph):
    pg1 = ldbc_graph.project(
        vertices={"post": [], "tag": [], "tagclass": []},
        edges={"hasTag": [], "isSubclassOf": []},
    )
    assert pg1.schema.vertex_labels == ["post", "tag", "tagclass"]
    assert pg1.schema.edge_labels == ["isSubclassOf", "hasTag"]

    pg2 = pg1.project(vertices={"tagclass": []}, edges={"isSubclassOf": []})
    assert pg2.schema.vertex_labels == ["tagclass"]
    assert pg2.schema.edge_labels == ["isSubclassOf"]


def test_error_on_project(arrow_property_graph, ldbc_graph):
    graph = arrow_property_graph
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


def test_transform(arrow_modern_graph):
    g = arrow_modern_graph.to_undirected()
    assert not g.is_directed()

    g2 = g.to_directed()
    assert g2.is_directed()


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

    ret = graphscope.pagerank(sub_graph_1)

    # the ret can add to the graph queried on
    g1 = sub_graph_1.add_column(ret, selector={"pr": "r"})
    assert g1.schema.get_vertex_properties("person")[0].id == 8
    assert g1.schema.get_vertex_properties("person")[0].name == "pr"
    # the ret can add to the origin graph
    g2 = ldbc.add_column(ret, selector={"pr": "r"})
    assert g2.schema.get_vertex_properties("person")[8].id == 8
    assert g2.schema.get_vertex_properties("person")[8].name == "pr"
    # the ret can add to the graph tha contain the same vertex label with sub_graph_1
    g3 = sub_graph_2.add_column(ret, selector={"pr": "r"})
    assert g3.schema.get_vertex_properties("person")[8].id == 8
    assert g3.schema.get_vertex_properties("person")[8].name == "pr"
    # the ret can not add to sub_graph_3
    with pytest.raises(AnalyticalEngineInternalError):
        g4 = sub_graph_3.add_column(ret, selector={"pr": "r"})
        print(g4.schema)
    # the ret can not add to sub_graph_4
    with pytest.raises(AnalyticalEngineInternalError):
        g5 = sub_graph_4.add_column(ret, selector={"pr": "r"})
        print(g4.schema)


def test_add_column_string_oid(
    p2p_property_graph_string, p2p_project_directed_graph_string
):
    g1 = p2p_property_graph_string
    g2 = p2p_project_directed_graph_string

    property_names = [p.name for p in g1.schema.get_vertex_properties("person")]
    assert "pagerank" not in property_names

    ctx = graphscope.pagerank(g2)
    g3 = g1.add_column(ctx, selector={"pagerank": "r"})

    property_names = [p.name for p in g3.schema.get_vertex_properties("person")]
    assert "pagerank" in property_names


def test_add_column_int32_oid(
    p2p_property_graph_int32, p2p_project_directed_graph_int32
):
    g1 = p2p_property_graph_int32
    g2 = p2p_project_directed_graph_int32

    property_names = [p.name for p in g1.schema.get_vertex_properties("person")]
    assert "pagerank" not in property_names

    ctx = graphscope.pagerank(g2)
    g3 = g1.add_column(ctx, selector={"pagerank": "r"})

    property_names = [p.name for p in g3.schema.get_vertex_properties("person")]
    assert "pagerank" in property_names


def test_graph_lifecycle(graphscope_session):
    graph = load_p2p_network(graphscope_session)
    c = graphscope.wcc(graph)
    del graph
    assert c.to_numpy("v.id") is not None
    del c  # should delete c and graph in c++
