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
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.graph import Graph
from graphscope.framework.loader import Loader
from graphscope.proto import types_pb2

logger = logging.getLogger("graphscope")


def test_graph_schema(arrow_property_graph):
    schema = arrow_property_graph.schema
    assert schema.vertex_labels == ["v0", "v1"]
    assert schema.edge_labels == ["e0", "e1"]


def test_load_graph_copy(graphscope_session, arrow_property_graph):
    g = arrow_property_graph
    g2 = graphscope_session.load_from(g)
    assert g.key != g2.key
    assert g.vineyard_id != g2.vineyard_id
    assert str(g.schema) == str(g2.schema)
    assert np.all(g.to_numpy("v:v0.id") == g2.to_numpy("v:v0.id"))
    g2.unload()
    assert not g2.loaded()
    # test load from vineyard's graph
    g3 = graphscope_session.load_from(vineyard.ObjectID(g.vineyard_id))
    assert g3.loaded()


def test_project_to_simple_with_name(p2p_property_graph, sssp_result):
    pg = p2p_property_graph.project_to_simple(
        v_label="person", v_prop="weight", e_label="knows", e_prop="dist"
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
    pg = p2p_property_graph.project_to_simple(0, 0, 0, 2)
    ctx = sssp(pg, src=6)
    r = (
        ctx.to_dataframe({"node": "v.id", "r": "r"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r[r == 1.7976931348623157e308] = float("inf")  # replace limit::max with inf
    assert np.allclose(r, sssp_result["directed"])


def test_error_on_project_to_simple_id_out_of_range(arrow_property_graph):
    g = arrow_property_graph
    # g has 4 vertex labels and 2 edge labels, each label has 1 property
    with pytest.raises(IndexError, match="id 5 is out of range"):
        g.project_to_simple(v_label=5, e_label=0)

    with pytest.raises(IndexError, match="id 3 is out of range"):
        g.project_to_simple(v_label=0, e_label=3)

    with pytest.raises(IndexError, match="id -1 is out of range"):
        g.project_to_simple(v_label=-1, e_label=3)

    with pytest.raises(IndexError, match="id 2 is out of range"):
        g.project_to_simple(v_label=0, e_label=0, v_prop=2, e_prop=0)

    with pytest.raises(IndexError, match="id 1 is out of range"):
        g.project_to_simple(v_label=0, e_label=0, v_prop=0, e_prop=1)


def test_error_label_on_project_to_simple(arrow_property_graph):
    g = arrow_property_graph
    # g has vertex labels: v0, v1, v2, v3, each label has a property: weight
    # g has edge label: e0, e1, each label has a property: dist
    with pytest.raises(ValueError, match="Label does not exists"):
        g.project_to_simple(v_label="v4", e_label="e0")

    with pytest.raises(ValueError, match="Label does not exists"):
        g.project_to_simple(v_label="v0", e_label="e2")

    with pytest.raises(ValueError, match="Property does not exists."):
        g.project_to_simple(v_label="v0", e_label="e0", v_prop="foo")

    with pytest.raises(ValueError, match="Property does not exists."):
        g.project_to_simple(v_label="v0", e_label="e0", e_prop="foo")


def test_error_relationship_on_project_to_simple(arrow_modern_graph):
    with pytest.raises(
        ValueError,
        match="Graph doesn't contain such relationship: person -> created <- person",
    ):
        g.project_to_simple(v_label="person", e_label="created")
    with pytest.raises(
        ValueError,
        match="Graph doesn't contain such relationship: software -> knows <- software",
    ):
        g.project_to_simple(v_label="software", e_label="knows")


def test_unload(graphscope_session):
    prefix = os.path.expandvars("${GS_TEST_DIR}/property")
    g = graphscope_session.load_from(
        edges={
            "knows": (
                Loader("{}/p2p-31_property_e_0".format(prefix)),
                ["src_label_id", "dst_label_id", "dist"],
                ("src_id", "person"),
                ("dst_id", "person"),
            ),
        },
        vertices={
            "person": Loader("{}/p2p-31_property_v_0".format(prefix)),
        },
    )
    assert g.vineyard_id is not None
    g.unload()

    assert not g.loaded()

    with pytest.raises(RuntimeError, match="The graph is not registered in remote."):
        g.unload()

    with pytest.raises(RuntimeError, match="The graph is not registered in remote"):
        g.project_to_simple(v_label="person", e_label="knows")
    with pytest.raises(AssertionError):
        g2 = graphscope_session.load_from(g)
    with pytest.raises(RuntimeError, match="graph should be registered in remote."):
        property_sssp(g, src=6)


def test_error_on_project_to_simple_wrong_graph_type(arrow_property_graph):
    sg = arrow_property_graph.project_to_simple(
        v_label=0, v_prop=0, e_label=0, e_prop=0
    )
    assert sg._graph_type == types_pb2.ARROW_PROJECTED
    with pytest.raises(AssertionError):
        sg.project_to_simple(v_label=0, v_prop=0, e_label=0, e_prop=0)


@pytest.mark.skipif(
    os.environ.get("EXPERIMENTAL_ON") != "ON", reason="dynamic graph is in experimental"
)
def test_error_on_project_to_simple_wrong_graph_type_2(dynamic_property_graph):
    sdg = dynamic_property_graph.project_to_simple()
    assert sdg._graph_type == types_pb2.DYNAMIC_PROJECTED
    with pytest.raises(AssertionError):
        sdg.project_to_simple()


def test_error_on_graph_init(graphscope_session):
    with pytest.raises(ValueError, match="Failed to create a graph"):
        g = Graph(graphscope_session.session_id)


@pytest.mark.skip("Why such strange behaviour? Shouldn't we raise in __init__?")
def test_error_on_operation_on_graph(graphscope_session):
    g = Graph(graphscope_session.session_id)
    with pytest.raises(RuntimeError, match="The graph is not registered in remote"):
        g.project_to_simple(v_label=0, v_prop=0, e_label=0, e_prop=0)

    with pytest.raises(RuntimeError, match="The graph is not registered in remote"):
        g.unload()

    with pytest.raises(AssertionError):
        property_sssp(g, src=6)


def test_error_on_app_query_non_compatible_graph(arrow_property_graph):
    g = arrow_property_graph
    # return a arrow property graph
    with pytest.raises(
        InvalidArgumentError,
        match="Not compatible for arrow_property dynamic_property type",
    ):
        sssp(g, src=6)

    sg = g.project_to_simple(
        v_label=0, e_label=0
    )  # edata is empty, not compatible with sssp
    with pytest.raises(graphscope.framework.errors.CompilationError):
        sssp(sg, 4)


@pytest.mark.skip(reason="appendonly graph not ready.")
def test_append_only_graph():
    g = load_from("xxx")
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
    g = load_from("xxx")
    ag = gs.to_appendable(g)
    with pytest.raises(AssertionError, match="expect source graph is immutable"):
        agg = gs.to_appendable(ag)

    g2 = gs.to_immutable(ag)
    with pytest.raises(AssertionError, match="expect source graph is appendable"):
        g3 = gs.to_immutable(g2)


def test_load_only_from_efile(
    arrow_property_graph, arrow_property_graph_only_from_efile
):
    assert (
        arrow_property_graph.schema.edge_properties
        == arrow_property_graph_only_from_efile.schema.edge_properties
    )
    assert (
        arrow_property_graph.schema.edge_relationships
        == arrow_property_graph_only_from_efile.schema.edge_relationships
    )
    assert (
        arrow_property_graph.schema.vertex_label_num
        == arrow_property_graph_only_from_efile.schema.vertex_label_num
    )
    assert (
        arrow_property_graph.schema.oid_type
        == arrow_property_graph_only_from_efile.schema.oid_type
    )
    assert (
        arrow_property_graph.schema.vid_type
        == arrow_property_graph_only_from_efile.schema.vid_type
    )
    assert (
        arrow_property_graph_only_from_efile.schema.vertex_labels
        == arrow_property_graph.schema.vertex_labels
    )
    assert arrow_property_graph_only_from_efile.schema.vertex_properties == [
        {"id": types_pb2.LONG},
        {"id": types_pb2.LONG},
    ]

    sg1 = arrow_property_graph.project_to_simple(0, 0, 0, 0)
    sg2 = arrow_property_graph_only_from_efile.project_to_simple(0, 0, 0, 0)
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
    with pytest.raises(ValueError, match="'non_exist_label' is not in list"):
        out = arrow_property_graph.add_column(
            property_context,
            {"id": "v:non_exist_label.id", "result": "r:non_exist_label.age"},
        )

    with pytest.raises(ValueError, match="'non_exist_prop' is not in list"):
        out = arrow_property_graph.add_column(
            property_context, {"id": "v:v0.non_exist_prop"}
        )

    with pytest.raises(AssertionError, match="selector of add column must be a dict"):
        out = arrow_property_graph.add_column(property_context, selector=None)

    with pytest.raises(SyntaxError, match="Invalid selector"):
        out = arrow_property_graph.add_column(property_context, {"id": "xxx:a.b"})


@pytest.mark.skip(reason="Issue 366")
def test_project_to_simple_string_vprop(arrow_modern_graph):
    sg = arrow_modern_graph.project_to_simple(
        v_label="person", e_label="knows", v_prop="name", e_prop="weight"
    )
    assert sg


@pytest.mark.skip(reason="String edata is not supported")
def test_project_to_simple_string_eprop(graphscope_session):
    data_dir = os.path.expandvars("${GS_TEST_DIR}/load_ldbc")
    g = load_ldbc(graphscope_session, data_dir)
    sg = g.project_to_simple(
        v_label="person", e_label="knows", v_prop="firstName", e_prop="creationDate"
    )
