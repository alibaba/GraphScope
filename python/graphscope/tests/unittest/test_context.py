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

import pandas as pd
import pytest
import vineyard.io

from graphscope import lpa_u2i
from graphscope import sssp
from graphscope.framework.app import AppAssets
from graphscope.framework.errors import InvalidArgumentError


def test_simple_context_to_numpy(simple_context):
    out = simple_context.to_numpy("v.id")
    assert out.shape == (40521,)
    out = simple_context.to_numpy("v.data")
    assert out.shape == (40521,)
    # selector of `e` is not done yet.
    # out = simple_context.to_numpy('e.src')
    # out = simple_context.to_numpy('e.dst')
    # out = simple_context.to_numpy('e.data')
    out = simple_context.to_numpy("r")
    assert out.shape == (40521,)


def test_simple_context_to_dataframe(simple_context):
    out = simple_context.to_dataframe({"id": "v.id", "data": "v.data", "result": "r"})
    assert out.shape == (40521, 3)


def test_simple_context_to_vineyard_tensor(simple_context, p2p_project_directed_graph):
    out = simple_context.to_vineyard_tensor("v.id")
    assert out is not None
    out = simple_context.to_vineyard_tensor("r")
    assert out is not None

    has_path = AppAssets(algo="sssp_has_path", context="tensor")
    ctx = has_path(
        p2p_project_directed_graph._project_to_simple(), source=6, target=3728
    )
    assert ctx.to_vineyard_tensor(axis=0) is not None


def test_simple_context_to_vineyard_dataframe(
    simple_context, p2p_project_directed_graph
):
    out = simple_context.to_vineyard_dataframe(
        {"id": "v.id", "data": "v.data", "result": "r"}
    )
    assert out is not None


def test_context_output_to_client(simple_context, tmp_path):
    rlt = os.path.join(tmp_path, "r0")
    simple_context.output_to_client(fd=rlt, selector={"id": "v.id", "result": "r"})
    out = pd.read_csv(rlt)
    assert out.shape == (40521, 2)


def test_context_output(simple_context):
    simple_context.output(
        fd="file:///tmp/rlt.csv",
        selector={"id": "v.id", "data": "v.data", "result": "r"},
    )


@pytest.mark.parametrize(
    "v_label,e_label",
    [
        ("v0", "e0"),
        ("v0", "e1"),
        ("v1", "e0"),
        ("v1", "e1"),
    ],
)
def test_add_column_after_computation(arrow_property_graph, v_label, e_label):
    sg = arrow_property_graph.project(
        vertices={v_label: ["id"]}, edges={e_label: ["weight"]}
    )
    ret = sssp(sg, 20)
    g2 = arrow_property_graph.add_column(
        ret, {"id_col": "v.id", "data_col": "v.data", "result_col": "r"}
    )
    assert "id_col" in [p.name for p in g2.schema.get_vertex_properties(v_label)]
    assert "data_col" in [p.name for p in g2.schema.get_vertex_properties(v_label)]
    assert "result_col" in [p.name for p in g2.schema.get_vertex_properties(v_label)]


def test_lpa_u2i(arrow_property_graph_lpa_u2i):
    ret = (
        lpa_u2i(arrow_property_graph_lpa_u2i, max_round=20)
        .to_dataframe(
            {"node": "v:v0.id", "label0": "r:v0.label_0", "label1": "r:v0.label_1"}
        )
        .sort_values(by=["node"])
    )


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_error_on_selector(arrow_property_graph_lpa_u2i):
    property_context = lpa_u2i(arrow_property_graph_lpa_u2i, max_round=20)
    with pytest.raises(KeyError, match="non_exist_label"):
        out = property_context.to_numpy("v:non_exist_label.id")
    with pytest.raises(KeyError, match="non_exist_prop"):
        out = property_context.to_numpy("v:v0.non_exist_prop")
    with pytest.raises(
        InvalidArgumentError,
        match="Selector in labeled vertex property context cannot be None",
    ):
        out = property_context.to_numpy(selector=None)
    with pytest.raises(
        SyntaxError,
        match="Invalid selector: `xxx`. Please inspect the result with `ret.schema` and choose a valid selector",
    ):
        out = property_context.to_numpy("xxx")
    with pytest.raises(
        SyntaxError,
        match="Invalid selector: `xxx:a.b`. Please inspect the result with `ret.schema` and choose a valid selector",
    ):
        out = property_context.to_numpy("xxx:a.b")
