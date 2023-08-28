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

import datetime
import io
import logging
import os
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import graphscope
from graphscope.analytical.udf.decorators import pie
from graphscope.analytical.udf.decorators import pregel
from graphscope.analytical.udf.utils import PregelAggregatorType
from graphscope.framework.app import AppAssets
from graphscope.framework.app import load_app
from graphscope.framework.errors import InvalidArgumentError

DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"

logger = logging.getLogger("graphscope")


@pytest.fixture(scope="function")
def random_gar():
    path = os.path.join(
        "/",
        tempfile.gettempprefix(),
        "{}.gar".format(str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f"))),
    )
    yield path
    os.remove(path)


@pytest.fixture(scope="module")
def not_exist_gar():
    path = os.path.join("not_exist_dir", "not_exist.gar")
    return path


@pytest.fixture(scope="module")
def non_zipfile_gar():
    path = os.path.join("/", tempfile.gettempprefix(), "test.txt")
    Path(path).touch()
    yield path
    os.remove(path)


@pytest.fixture(scope="module")
def empty_gar():
    path = os.path.join(
        "/",
        tempfile.gettempprefix(),
        "{}.gar".format(str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f"))),
    )
    empty_zip_data = b"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    with open(path, "wb") as f:
        f.write(empty_zip_data)
    yield path
    os.remove(path)


def invalid_configfile_gar():
    path = os.path.join(
        "/",
        tempfile.gettempprefix(),
        "{}.gar".format(str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f"))),
    )
    config = {"a": 10}
    in_memory_buffer = BytesIO()
    zip_file = zipfile.ZipFile(in_memory_buffer, "a", zipfile.ZIP_DEFLATED, False)
    zip_file.writestr(DEFAULT_GS_CONFIG_FILE, yaml.dump(config))
    zip_file.close()
    with open(path, "wb") as f:
        f.write(in_memory_buffer.getvalue())
    yield path
    os.remove(path)


# Example of pregel sssp
@pregel(vd_type="double", md_type="double")
class SSSP_Pregel(AppAssets):
    @staticmethod
    def Init(v, context):
        v.set_value(1000000000.0)

    @staticmethod
    def Compute(messages, v, context):
        src_id = context.get_config(b"src")
        cur_dist = v.value()
        new_dist = 1000000000.0
        if v.id() == src_id:
            new_dist = 0
        for message in messages:
            new_dist = min(message, new_dist)
        if new_dist < cur_dist:
            v.set_value(new_dist)
            for e_label_id in range(context.edge_label_num()):
                edges = v.outgoing_edges(e_label_id)
                for e in edges:
                    v.send(e.vertex(), new_dist + e.get_int(2))
        v.vote_to_halt()


# Example of pie sssp
@pie(vd_type="string", md_type="string")
class PIE_API_Test(AppAssets):
    """PIE API Test on ldbc sample graph."""

    @staticmethod
    def Init(frag, context):
        pass

    @staticmethod
    def PEval(frag, context):
        graphscope.declare(graphscope.Vertex, node)
        fid = frag.fid()
        if fid == 0:
            # This is not stable, as it depend on worker's number
            # assert frag.fnum() == 4
            assert frag.vertex_label_num() == 8
            assert frag.edge_label_num() == 15
            assert frag.get_total_nodes_num() == 190376

            v_label_num = frag.vertex_label_num()
            for v_label_id in range(v_label_num):
                assert frag.get_nodes_num(v_label_id) == (
                    frag.get_inner_nodes_num(v_label_id)
                    + frag.get_outer_nodes_num(v_label_id)
                )

                nodes = frag.nodes(v_label_id)
                assert nodes.size() == frag.get_nodes_num(v_label_id)

                inner_nodes = frag.inner_nodes(v_label_id)
                assert inner_nodes.size() == frag.get_inner_nodes_num(v_label_id)

                outer_nodes = frag.outer_nodes(v_label_id)
                assert outer_nodes.size() == frag.get_outer_nodes_num(v_label_id)

                for iv in inner_nodes:
                    assert frag.get_node_fid(iv) == 0
                    assert frag.is_inner_node(iv) == True
                    vid = frag.get_node_id(iv)
                    assert frag.get_node(v_label_id, vid, node) == True
                    assert frag.get_inner_node(v_label_id, vid, node) == True

                    e_label_num = frag.edge_label_num()
                    for e_label_id in range(e_label_num):
                        edges = frag.get_outgoing_edges(iv, e_label_id)
                        if edges.size() > 0:
                            assert frag.has_child(iv, e_label_id) == True
                        assert frag.get_outdegree(iv, e_label_id) == int(edges.size())

                        edges = frag.get_incoming_edges(iv, e_label_id)
                        if edges.size() > 0:
                            assert frag.has_parent(iv, e_label_id) == True
                        assert frag.get_indegree(iv, e_label_id) == int(edges.size())

                for ov in outer_nodes:
                    assert frag.is_outer_node(ov) == True
                    vid = frag.get_node_id(ov)
                    assert frag.get_node(v_label_id, vid, node) == True
                    assert frag.get_outer_node(v_label_id, vid, node) == True

            for v_label in frag.vertex_labels():
                label_id = frag.get_vertex_label_id_by_name(v_label)
                assert frag.get_vertex_label_by_id(label_id) == v_label

                for prop in frag.vertex_properties(v_label):
                    prop_id = frag.get_vertex_property_id_by_name(v_label, prop.first)
                    assert (
                        frag.get_vertex_property_by_id(v_label, prop_id) == prop.first
                    )
                    prop_id = frag.get_vertex_property_id_by_name(label_id, prop.first)
                    assert (
                        frag.get_vertex_property_by_id(label_id, prop_id) == prop.first
                    )

                for prop in frag.vertex_properties(label_id):
                    pass

            for e_label in frag.edge_labels():
                e_label_id = frag.get_edge_label_id_by_name(e_label)
                assert frag.get_edge_label_by_id(e_label_id) == e_label

                for prop in frag.edge_properties(e_label):
                    prop_id = frag.get_edge_property_id_by_name(e_label, prop.first)
                    assert frag.get_edge_property_by_id(e_label, prop_id) == prop.first
                    prop_id = frag.get_edge_property_id_by_name(e_label_id, prop.first)
                    assert (
                        frag.get_edge_property_by_id(e_label_id, prop_id) == prop.first
                    )

                for prop in frag.edge_properties(e_label_id):
                    pass

    @staticmethod
    def IncEval(frag, context):
        pass


# Pregel API Test
@pregel(vd_type="string", md_type="string")
class Pregel_API_Test(AppAssets):
    """Pregel API Test on ldbc_sample graph"""

    @staticmethod
    def Init(v, context):
        v.set_value(b"")

    @staticmethod
    def Compute(messages, v, context):
        """
        Test on vertex with id = 933, label = person
        """
        v.vote_to_halt()

        # v.id()
        vid = v.id()

        # v.label()
        label = v.label()

        if vid == b"933" and label == b"person":
            # v.label_id()
            label_id = v.label_id()

            assert context.get_vertex_label_by_id(label_id) == label

            assert context.get_vertex_label_id_by_name(label) == label_id

            v.set_value(b"graphscope")
            assert v.value() == b"graphscope"

            for prop in v.properties():
                prop_id = context.get_vertex_property_id_by_name(v.label(), prop.first)
                assert (
                    context.get_vertex_property_by_id(v.label(), prop_id) == prop.first
                )
                prop_id = context.get_vertex_property_id_by_name(
                    v.label_id(), prop.first
                )
                assert (
                    context.get_vertex_property_by_id(v.label_id(), prop_id)
                    == prop.first
                )
                if prop.second == b"DOUBLE":
                    # test v.get_double(v_property_name) / v.get_double(v_property_id)
                    assert v.get_double(prop.first) == v.get_double(prop_id)
                elif prop.second == b"LONG":
                    # test v.get_int(v_property_name) / test v.get_int(v_property_id)
                    assert v.get_int(prop.first) == v.get_int(prop_id)
                elif prop.second == b"STRING":
                    # test v.get_str(v_property_name) / test v.get_str(v_property_id)
                    assert v.get_str(prop.first) == v.get_str(prop_id)

            assert context.superstep() == 0

            assert context.get_config(b"param1") == b"graphscope"
            assert context.get_config(b"param2") == b"graphscope2"

            assert context.get_total_vertices_num() == 190376

            assert context.vertex_label_num() == 8

            assert context.edge_label_num() == 15

            assert context.vertex_property_num(v.label()) == 8
            assert context.vertex_property_num(v.label_id()) == 8

            for e_label in context.edge_labels():
                e_label_id = context.get_edge_label_id_by_name(e_label)
                assert context.get_edge_label_by_id(e_label_id) == e_label

                edges = v.incoming_edges(e_label_id)
                for edge in edges:
                    edge.vertex().id()

                if e_label == b"knows":
                    assert context.edge_property_num(e_label_id) == 2
                    assert context.edge_property_num(e_label) == 2

                for prop in context.edge_properties(e_label):
                    prop_id = context.get_edge_property_id_by_name(e_label, prop.first)
                    assert (
                        context.get_edge_property_by_id(e_label, prop_id) == prop.first
                    )
                    prop_id = context.get_edge_property_id_by_name(
                        e_label_id, prop.first
                    )
                    assert (
                        context.get_edge_property_by_id(e_label_id, prop_id)
                        == prop.first
                    )

                for prop in context.edge_properties(e_label_id):
                    pass

            for v_label in context.vertex_labels():
                v_label_id = context.get_vertex_label_id_by_name(v_label)
                assert context.get_vertex_label_by_id(v_label_id) == v_label

                if v_label == b"person":
                    assert context.vertex_property_num(v_label_id) == 8
                    assert context.vertex_property_num(v_label) == 8

                for prop in context.vertex_properties(v_label):
                    prop_id = context.get_vertex_property_id_by_name(
                        v_label, prop.first
                    )
                    assert (
                        context.get_vertex_property_by_id(v_label, prop_id)
                        == prop.first
                    )

                for prop in context.vertex_properties(v_label_id):
                    pass


# Example of pregel sssp (with combine)
@pregel(vd_type="double", md_type="double")
class SSSP_Pregel_Combine(AppAssets):
    @staticmethod
    def Init(v, context):
        v.set_value(1000000000.0)

    @staticmethod
    def Compute(messages, v, context):
        src_id = context.get_config(b"src")
        cur_dist = v.value()
        new_dist = 1000000000.0
        if v.id() == src_id:
            new_dist = 0
        for message in messages:
            new_dist = min(message, new_dist)
        if new_dist < cur_dist:
            v.set_value(new_dist)
            for e_label_id in range(context.edge_label_num()):
                edges = v.outgoing_edges(e_label_id)
                for e in edges:
                    v.send(e.vertex(), new_dist + e.get_int(2))
        v.vote_to_halt()

    @staticmethod
    def Combine(messages):
        ret = 1000000000.0
        for m in messages:
            ret = min(ret, m)
        return ret


# Example of pregel aggregator test
@pregel(vd_type="double", md_type="double")
class Aggregators_Pregel_Test(AppAssets):
    @staticmethod
    def Init(v, context):
        # int
        context.register_aggregator(
            b"int_sum_aggregator", PregelAggregatorType.kInt64SumAggregator
        )
        context.register_aggregator(
            b"int_max_aggregator", PregelAggregatorType.kInt64MaxAggregator
        )
        context.register_aggregator(
            b"int_min_aggregator", PregelAggregatorType.kInt64MinAggregator
        )
        context.register_aggregator(
            b"int_product_aggregator", PregelAggregatorType.kInt64ProductAggregator
        )
        context.register_aggregator(
            b"int_overwrite_aggregator", PregelAggregatorType.kInt64OverwriteAggregator
        )
        # double
        context.register_aggregator(
            b"double_sum_aggregator", PregelAggregatorType.kDoubleSumAggregator
        )
        context.register_aggregator(
            b"double_max_aggregator", PregelAggregatorType.kDoubleMaxAggregator
        )
        context.register_aggregator(
            b"double_min_aggregator", PregelAggregatorType.kDoubleMinAggregator
        )
        context.register_aggregator(
            b"double_product_aggregator", PregelAggregatorType.kDoubleProductAggregator
        )
        context.register_aggregator(
            b"double_overwrite_aggregator",
            PregelAggregatorType.kDoubleOverwriteAggregator,
        )
        # bool
        context.register_aggregator(
            b"bool_and_aggregator", PregelAggregatorType.kBoolAndAggregator
        )
        context.register_aggregator(
            b"bool_or_aggregator", PregelAggregatorType.kBoolOrAggregator
        )
        context.register_aggregator(
            b"bool_overwrite_aggregator", PregelAggregatorType.kBoolOverwriteAggregator
        )
        # text
        context.register_aggregator(
            b"text_append_aggregator", PregelAggregatorType.kTextAppendAggregator
        )

    @staticmethod
    def Compute(messages, v, context):
        if context.superstep() == 0:
            context.aggregate(b"int_sum_aggregator", 1)
            context.aggregate(b"int_max_aggregator", int(v.id()))
            context.aggregate(b"int_min_aggregator", int(v.id()))
            context.aggregate(b"int_product_aggregator", 1)
            context.aggregate(b"int_overwrite_aggregator", 1)
            context.aggregate(b"double_sum_aggregator", 1.0)
            context.aggregate(b"double_max_aggregator", float(v.id()))
            context.aggregate(b"double_min_aggregator", float(v.id()))
            context.aggregate(b"double_product_aggregator", 1.0)
            context.aggregate(b"double_overwrite_aggregator", 1.0)
            context.aggregate(b"bool_and_aggregator", True)
            context.aggregate(b"bool_or_aggregator", False)
            context.aggregate(b"bool_overwrite_aggregator", True)
            context.aggregate(b"text_append_aggregator", v.id() + b",")
        else:
            if v.id() == b"1":
                assert context.get_aggregated_value(b"int_sum_aggregator") == 62586
                assert context.get_aggregated_value(b"int_max_aggregator") == 62586
                assert context.get_aggregated_value(b"int_min_aggregator") == 1
                assert context.get_aggregated_value(b"int_product_aggregator") == 1
                assert context.get_aggregated_value(b"int_overwrite_aggregator") == 1
                assert context.get_aggregated_value(b"double_sum_aggregator") == 62586.0
                assert context.get_aggregated_value(b"double_max_aggregator") == 62586.0
                assert context.get_aggregated_value(b"double_min_aggregator") == 1.0
                assert context.get_aggregated_value(b"double_product_aggregator") == 1.0
                assert (
                    context.get_aggregated_value(b"double_overwrite_aggregator") == 1.0
                )
                assert context.get_aggregated_value(b"bool_and_aggregator") == True
                assert context.get_aggregated_value(b"bool_or_aggregator") == False
                assert (
                    context.get_aggregated_value(b"bool_overwrite_aggregator") == True
                )
                context.get_aggregated_value(b"text_append_aggregator")
            v.vote_to_halt()


@pregel(vd_type="string", md_type="string")
class PregelVertexTraversal(AppAssets):
    """Write Vertex properties.
    Formats: prop1,prop2,...,propN,id
    """

    @staticmethod
    def Init(v, context):
        v.set_value(b"")

    @staticmethod
    def Compute(messages, v, context):
        rlt = string(b"")
        first = True
        for prop in v.properties():
            if not first:
                rlt.append(b",")
            first = False
            if prop.second == b"DOUBLE":
                rlt.append(to_string(v.get_double(prop.first)))
            elif prop.second == b"LONG":
                rlt.append(to_string(v.get_int(prop.first)))
            elif prop.second == b"STRING":
                rlt.append(v.get_str(prop.first))
        v.set_value(rlt)
        v.vote_to_halt()


@pregel(vd_type="string", md_type="string")
class PregelEdgeTraversal(AppAssets):
    """Write Edge properties, together with src/dst id.
    Formats: e_label,src_id,dst_id,prop1,...,propN
    """

    @staticmethod
    def Init(v, context):
        v.set_value(b"")

    @staticmethod
    def Compute(messages, v, context):
        rlt = string(b"")
        e_labels = context.edge_labels()
        first = True
        for e_label in e_labels:
            edges = v.outgoing_edges(e_label)
            for e in edges:
                if not first:
                    rlt.append(b"|")
                first = False
                rlt.append(e_label)
                rlt.append(b",")
                rlt.append(v.id())
                rlt.append(b",")
                rlt.append(e.vertex().id())
                for prop in context.edge_properties(e_label):
                    rlt.append(b",")
                    e_prop_id = context.get_edge_property_id_by_name(
                        e_label, prop.first
                    )
                    if prop.second == b"DOUBLE":
                        rlt.append(to_string(e.get_double(e_prop_id)))
                    elif prop.second == b"LONG":
                        rlt.append(to_string(e.get_int(e_prop_id)))
                    elif prop.second == b"STRING":
                        rlt.append(e.get_str(e_prop_id))
        v.set_value(rlt)
        v.vote_to_halt()


# Example of get schema in pregel model
@pregel(vd_type="string", md_type="string")
class Pregel_GetSchema(AppAssets):
    @staticmethod
    def Init(v, context):
        v.set_value(string(b""))

    @staticmethod
    def Compute(messages, v, context):
        rlt = v.value()
        for v_label in context.vertex_labels():
            rlt.append(v_label)
            rlt.append(b",")
        for v_property in context.vertex_properties(v.label()):
            rlt.append(v_property.first)
            rlt.append(b",")
            rlt.append(v_property.second)
            rlt.append(b",")
        e_labels = context.edge_labels()
        for e_label in e_labels:
            rlt.append(e_label)
            rlt.append(b",")
            for e_property in context.edge_properties(e_label):
                rlt.append(e_property.first)
                rlt.append(b",")
                rlt.append(e_property.second)
                rlt.append(b",")
        v.set_value(rlt)
        v.vote_to_halt()


# Example of pie sssp
@pie(vd_type="double", md_type="double")
class SSSP_PIE(AppAssets):
    @staticmethod
    def Init(frag, context):
        v_label_num = frag.vertex_label_num()
        for v_label_id in range(v_label_num):
            nodes = frag.nodes(v_label_id)
            context.init_value(
                nodes, v_label_id, 1000000000.0, PIEAggregateType.kMinAggregate
            )
            context.register_sync_buffer(v_label_id, MessageStrategy.kSyncOnOuterVertex)

    @staticmethod
    def PEval(frag, context):
        src = context.get_config(b"src")
        graphscope.declare(graphscope.Vertex, source)
        native_source = False
        v_label_num = frag.vertex_label_num()
        for v_label_id in range(v_label_num):
            if frag.get_inner_node(v_label_id, src, source):
                native_source = True
                break
        if native_source:
            context.set_node_value(source, 0)
        else:
            return
        e_label_num = frag.edge_label_num()
        for e_label_id in range(e_label_num):
            edges = frag.get_outgoing_edges(source, e_label_id)
            for e in edges:
                dst = e.neighbor()
                distv = e.get_int(2)
                if context.get_node_value(dst) > distv:
                    context.set_node_value(dst, distv)

    @staticmethod
    def IncEval(frag, context):
        v_label_num = frag.vertex_label_num()
        e_label_num = frag.edge_label_num()
        for v_label_id in range(v_label_num):
            iv = frag.inner_nodes(v_label_id)
            for v in iv:
                v_dist = context.get_node_value(v)
                for e_label_id in range(e_label_num):
                    es = frag.get_outgoing_edges(v, e_label_id)
                    for e in es:
                        u = e.neighbor()
                        u_dist = v_dist + e.get_int(2)
                        if context.get_node_value(u) > u_dist:
                            context.set_node_value(u, u_dist)


# Example of get schema in pie model
@pie(vd_type="string", md_type="string")
class PIE_GetSchema(AppAssets):
    @staticmethod
    def Init(frag, context):
        v_label_num = frag.vertex_label_num()
        for i in range(0, v_label_num):
            nodes = frag.nodes(i)
            context.init_value(
                nodes, i, string(b""), PIEAggregateType.kTextAppendAggregate
            )

    @staticmethod
    def PEval(frag, context):
        v_label_num = frag.vertex_label_num()
        for v_label_id in range(0, v_label_num):
            iv = frag.inner_nodes(v_label_id)
            for v in iv:
                rlt = context.get_node_value(v)
                for v_label in frag.vertex_labels():
                    rlt.append(v_label)
                    rlt.append(b",")
                for v_property in frag.vertex_properties(v_label_id):
                    rlt.append(v_property.first)
                    rlt.append(b",")
                    rlt.append(v_property.second)
                    rlt.append(b",")
                e_labels = frag.edge_labels()
                for e_label in e_labels:
                    rlt.append(e_label)
                    rlt.append(b",")
                    for e_property in frag.edge_properties(e_label):
                        rlt.append(e_property.first)
                        rlt.append(b",")
                        rlt.append(e_property.second)
                        rlt.append(b",")
                context.set_node_value(v, rlt)

    @staticmethod
    def IncEval(frag, context):
        pass


# Example of pregel sssp
@pregel(vd_type="double", md_type="double")
class MathInAlgorithm(AppAssets):
    @staticmethod
    def Init(v, context):
        v.set_value(context.math.log2(1000000000.0 * context.math.M_PI))

    @staticmethod
    def Compute(messages, v, context):
        v.vote_to_halt()


def test_error_with_missing_necessary_method():
    with pytest.raises(ValueError, match="Can't find method definition"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_1:
            @staticmethod
            def Init(v, context):
                pass

    with pytest.raises(ValueError, match="Can't find method definition"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_2:
            @staticmethod
            def Compute(message, v, context):
                pass

    with pytest.raises(ValueError, match="Can't find method definition"):

        @pie(vd_type="double", md_type="double")
        class PIE_1:
            @staticmethod
            def Init(frag, context):
                pass

            @staticmethod
            def PEval(frag, context):
                pass

    with pytest.raises(ValueError, match="Can't find method definition"):

        @pie(vd_type="double", md_type="double")
        class PIE_2:
            @staticmethod
            def Init(v, context):
                pass

            @staticmethod
            def IncEval(frag, context):
                pass

    with pytest.raises(ValueError, match="Can't find method definition"):

        @pie(vd_type="double", md_type="double")
        class PIE_3:
            @staticmethod
            def PEval(frag, context):
                pass

            @staticmethod
            def IncEval(frag, context):
                pass


def test_error_with_missing_staticmethod_keyword():
    with pytest.raises(ValueError, match="Missing staticmethod decorator"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_1:
            @staticmethod
            def Init(v, context):
                pass

            def Compute(message, v, context):
                pass

    with pytest.raises(ValueError, match="Missing staticmethod decorator"):

        @pie(vd_type="double", md_type="double")
        class PIE_1:
            def Init(frag, context):
                pass

            def PEval(frag, context):
                pass

            def IncEval(frag, context):
                pass


def test_error_with_method_signature():
    with pytest.raises(AssertionError, match="The number of parameters does not match"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_1:
            @staticmethod
            def Init(v):  # missing context
                pass

            @staticmethod
            def Compute(message, v, context):
                pass

    with pytest.raises(AssertionError, match="The number of parameters does not match"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_2:
            @staticmethod
            def Init(v, context):
                pass

            @staticmethod
            def Compute(v, context):  # misssing message
                pass

    with pytest.raises(AssertionError, match="The number of parameters does not match"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_3:
            @staticmethod
            def Init(v, context, other):  # more args
                pass

            @staticmethod
            def Compute(message, v, context):
                pass

    with pytest.raises(AssertionError, match="The number of parameters does not match"):

        @pie(vd_type="double", md_type="double")
        class PIE_1:
            @staticmethod
            def Init(frag):  # missing context
                pass

            @staticmethod
            def PEval(frag, context):
                pass

            @staticmethod
            def IncEval(frag, context):
                pass

    with pytest.raises(AssertionError, match="The number of parameters does not match"):

        @pie(vd_type="double", md_type="double")
        class PIE_2:
            @staticmethod
            def Init(frag, context):
                pass

            @staticmethod
            def PEval(frag):  # missing context
                pass

            @staticmethod
            def IncEval(frag, context):
                pass

    with pytest.raises(AssertionError, match="The number of parameters does not match"):

        @pie(vd_type="double", md_type="double")
        class PIE_3:
            @staticmethod
            def Init(frag, context):
                pass

            @staticmethod
            def PEval(frag, context):
                pass

            @staticmethod
            def IncEval(frag):  # missing context
                pass

    with pytest.raises(AssertionError, match="The number of parameters does not match"):

        @pie(vd_type="double", md_type="double")
        class PIE_4:
            @staticmethod
            def Init(frag, context, message):  # more args
                pass

            @staticmethod
            def PEval(frag, context):
                pass

            @staticmethod
            def IncEval(frag, context):
                pass


def test_extra_method_definition():
    with pytest.raises(RuntimeError, match="Not recognized method"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_1:
            @staticmethod
            def Init(v, context):
                pass

            @staticmethod
            def Compute(message, v, context):
                pass

            @staticmethod
            def util(self):  # extra staticmethod
                pass

    with pytest.raises(RuntimeError, match="Not recognized method"):

        @pie(vd_type="double", md_type="double")
        class PIE_1:
            @staticmethod
            def Init(frag, context):
                pass

            @staticmethod
            def PEval(frag, context):
                pass

            @staticmethod
            def IncEval(frag, context):
                pass

            @staticmethod  # extra staticmethod
            def util():
                pass


def test_error_with_import_module():
    with pytest.raises(RuntimeError, match="Import is not supported yet"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_1:
            @staticmethod
            def Init(v, context):
                import random

                pass

            @staticmethod
            def Compute(message, v, context):
                pass

    with pytest.raises(RuntimeError, match="ImportFrom is not supported yet"):

        @pregel(vd_type="double", md_type="double")
        class Pregel_1:
            @staticmethod
            def Init(v, context):
                from os import path

                pass

            @staticmethod
            def Compute(message, v, context):
                pass


def test_dump_gar(random_gar, not_exist_gar):
    SSSP_Pregel.to_gar(random_gar)
    # gar file already exist
    with pytest.raises(RuntimeError, match="Path exist"):
        SSSP_Pregel.to_gar(random_gar)
    # not exist dir, also works with permission denied
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        SSSP_Pregel.to_gar(not_exist_gar)


def test_load_app_from_gar(random_gar, not_exist_gar, non_zipfile_gar):
    # file not exist, also works with permission denied
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        ast1 = load_app(not_exist_gar)
    # not a zip file
    with pytest.raises(ValueError, match="not a zip file"):
        ast2 = load_app(non_zipfile_gar)
    # type error
    with pytest.raises(ValueError, match="Wrong type"):
        ast3 = load_app([1, 2, 3, 4])
    with pytest.raises(ValueError, match="Wrong type"):
        ast4 = load_app(gar=None)
    SSSP_Pregel.to_gar(random_gar)
    ast1 = load_app(random_gar)
    assert isinstance(ast1, AppAssets)


def test_error_on_create_cython_app(
    graphscope_session,
    p2p_property_graph,
    dynamic_property_graph,
    random_gar,
    empty_gar,
):
    SSSP_Pregel.to_gar(random_gar)
    with pytest.raises(InvalidArgumentError, match="App is uncompatible with graph"):
        a1 = load_app(random_gar)
        a1(dynamic_property_graph, src=4)
    # algo not found in gar resource
    with pytest.raises(InvalidArgumentError, match="App not found in gar: sssp"):
        a2 = load_app(gar=random_gar, algo="sssp")
        a2(p2p_property_graph, src=6)
    # no `.gs_conf.yaml` in empty gar, raise KeyError exception
    with pytest.raises(KeyError):
        a3 = load_app(gar=empty_gar, algo="SSSP_Pregel")
        a3(p2p_property_graph, src=6)


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_get_schema(graphscope_session, arrow_property_graph):
    # pregel
    a1 = Pregel_GetSchema()
    ctx1 = a1(arrow_property_graph)
    r1 = ctx1.to_numpy("r:v0", vertex_range={"begin": 0, "end": 7})
    assert r1.tolist() == [
        "v0,v1,dist,DOUBLE,id,LONG,e0,weight,LONG,e1,weight,LONG,",
        "v0,v1,dist,DOUBLE,id,LONG,e0,weight,LONG,e1,weight,LONG,",
        "v0,v1,dist,DOUBLE,id,LONG,e0,weight,LONG,e1,weight,LONG,",
    ]
    # pie
    a2 = PIE_GetSchema()
    ctx2 = a2(arrow_property_graph)
    r2 = ctx2.to_numpy("r:v0", vertex_range={"begin": 0, "end": 7})
    assert r2.tolist() == [
        "v0,v1,dist,DOUBLE,id,LONG,e0,weight,LONG,e1,weight,LONG,",
        "v0,v1,dist,DOUBLE,id,LONG,e0,weight,LONG,e1,weight,LONG,",
        "v0,v1,dist,DOUBLE,id,LONG,e0,weight,LONG,e1,weight,LONG,",
    ]


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_property_context(graphscope_session, p2p_property_graph):
    a1 = SSSP_Pregel()
    ctx = a1(p2p_property_graph, src=6)
    # property context to numpy
    np_out = ctx.to_numpy("r:person")
    # property context to tensor
    df_out = ctx.to_dataframe({"result": "r:person"})
    # property context to vineyard tensor
    vt_out = ctx.to_vineyard_tensor("r:person")
    assert vt_out is not None
    # property context to vineyard dataframe
    vdf_out = ctx.to_vineyard_dataframe({"node": "v:person.id", "r": "r:person"})
    assert vdf_out is not None
    # add column
    g = p2p_property_graph.add_column(ctx, {"result0": "r:person"})
    g_out_df = g.to_dataframe({"result": "v:person.result0"})
    assert g_out_df.equals(df_out)


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_run_cython_pregel_app(
    graphscope_session, p2p_property_graph, sssp_result, random_gar
):
    SSSP_Pregel.to_gar(random_gar)
    a1 = SSSP_Pregel()
    ctx1 = a1(p2p_property_graph, src=6)
    r1 = (
        ctx1.to_dataframe({"node": "v:person.id", "r": "r:person"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r1[r1 == 1000000000.0] = float("inf")
    assert np.allclose(r1, sssp_result["directed"])
    # redundant params is ok
    ctx2 = a1(p2p_property_graph, src=6, other="a", yet_other=[1, 2, 3])
    r2 = (
        ctx2.to_dataframe({"node": "v:person.id", "r": "r:person"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r2[r2 == 1000000000.0] = float("inf")
    assert np.allclose(r2, sssp_result["directed"])
    # load from gar
    a2 = load_app(random_gar)
    ctx3 = a2(p2p_property_graph, src=6)
    r3 = (
        ctx3.to_dataframe({"node": "v:person.id", "r": "r:person"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r3[r3 == 1000000000.0] = float("inf")
    assert np.allclose(r3, sssp_result["directed"])
    # args is not supported
    with pytest.raises(
        InvalidArgumentError, match="Only support using keyword arguments in cython app"
    ):
        a3 = load_app(random_gar)
        ctx4 = a3(p2p_property_graph, 6, src=6)
    # combine
    a5 = SSSP_Pregel_Combine()
    ctx5 = a5(p2p_property_graph, src=6)
    r5 = (
        ctx5.to_dataframe({"node": "v:person.id", "r": "r:person"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r5[r5 == 1000000000.0] = float("inf")
    assert np.allclose(r5, sssp_result["directed"])
    # aggregator test
    a6 = Aggregators_Pregel_Test()
    a6(p2p_property_graph)

    # math.h function test
    a7 = MathInAlgorithm()
    a7(p2p_property_graph)


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_run_cython_pie_app(
    graphscope_session, p2p_property_graph, sssp_result, random_gar
):
    SSSP_PIE.to_gar(random_gar)
    a1 = SSSP_PIE()
    ctx1 = a1(p2p_property_graph, src=6)
    r1 = (
        ctx1.to_dataframe({"node": "v:person.id", "r": "r:person"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r1[r1 == 1000000000.0] = float("inf")
    assert np.allclose(r1, sssp_result["directed"])
    ctx2 = a1(p2p_property_graph, src=6, other="a", yet_other=[1, 2, 3])
    r2 = (
        ctx2.to_dataframe({"node": "v:person.id", "r": "r:person"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r2[r2 == 1000000000.0] = float("inf")
    assert np.allclose(r2, sssp_result["directed"])
    # load from gar
    a2 = load_app(random_gar)
    ctx3 = a2(p2p_property_graph, src=6)
    r3 = (
        ctx3.to_dataframe({"node": "v:person.id", "r": "r:person"})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    r3[r3 == 1000000000.0] = float("inf")
    assert np.allclose(r3, sssp_result["directed"])
    # args is not supported
    with pytest.raises(
        InvalidArgumentError, match="Only support using keyword arguments in cython app"
    ):
        a3 = load_app(random_gar)
        ctx4 = a3(p2p_property_graph, 6, src=6)


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_vertex_traversal(arrow_property_graph, twitter_v_0, twitter_v_1):
    traversal = PregelVertexTraversal()
    ctx = traversal(arrow_property_graph)
    # to dataframe
    r0 = ctx.to_dataframe({"node": "v:v0.id", "r": "r:v0"})

    r1 = ctx.to_dataframe({"node": "v:v1.id", "r": "r:v1"})

    def compare_result(df, result_file):
        id_col = df["node"].astype("int64")
        df = (
            pd.DataFrame(df.r.str.split(",").tolist(), columns=["dist", "id"])
            .reindex(columns=["id", "dist"])
            .astype({"id": "int64", "dist": "float64"})
        )
        assert id_col.equals(df["id"])

        df = df.sort_values(by=["id"]).reset_index(drop=True)

        result_df = pd.read_csv(result_file, sep=",")
        assert df.equals(result_df)

    compare_result(r0, twitter_v_0)
    compare_result(r1, twitter_v_1)


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_modern_graph_vertex_traversal(arrow_modern_graph):
    traversal = PregelVertexTraversal()
    ctx = traversal(arrow_modern_graph)
    r0 = ctx.to_dataframe({"node": "v:person.id", "r": "r:person"})
    r1 = ctx.to_dataframe({"node": "v:software.id", "r": "r:software"})

    def compare_id(df, names):
        id_col = df["node"].astype("int64")
        df = (
            pd.DataFrame(df.r.str.split(",").tolist(), columns=names + ["id"])
            .reindex(columns=["id"] + names)
            .astype({"id": "int64"})
        )
        assert id_col.equals(df["id"])

    compare_id(r0, ["name", "age"])
    compare_id(r1, ["name", "lang"])


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_edge_traversal(
    arrow_property_graph,
    twitter_e_0_0_0,
    twitter_e_0_1_0,
    twitter_e_1_0_0,
    twitter_e_1_1_0,
    twitter_e_0_0_1,
    twitter_e_0_1_1,
    twitter_e_1_0_1,
    twitter_e_1_1_1,
):
    traversal = PregelEdgeTraversal()
    ctx = traversal(arrow_property_graph)
    r0 = ctx.to_dataframe({"node": "v:v0.id", "r": "r:v0"})
    r1 = ctx.to_dataframe({"node": "v:v1.id", "r": "r:v1"})
    edges = []
    edges.extend(r0.r.str.split("|").tolist())
    edges.extend(r1.r.str.split("|").tolist())
    df = pd.read_csv(
        io.StringIO("\n".join([item for sublist in edges for item in sublist])),
        sep=",",
        names=["label", "src", "dst", "weight"],
    )

    def compare_result(df, *args):
        df = df[["src", "dst", "weight"]].astype(
            {"src": "int64", "dst": "int64", "weight": "float64"}
        )
        df = df.sort_values(by=["src", "dst"]).reset_index(drop=True)
        result_df = pd.concat(
            [pd.read_csv(arg) for arg in args], ignore_index=True, copy=False
        )
        result_df = result_df.astype(
            {"src": "int64", "dst": "int64", "weight": "float64"}
        )
        result_df = result_df.sort_values(by=["src", "dst"]).reset_index(drop=True)
        assert df.equals(result_df)

    compare_result(
        df[df["label"] == "e0"],
        twitter_e_0_0_0,
        twitter_e_0_1_0,
        twitter_e_1_0_0,
        twitter_e_1_1_0,
    )
    compare_result(
        df[df["label"] == "e1"],
        twitter_e_0_0_1,
        twitter_e_0_1_1,
        twitter_e_1_0_1,
        twitter_e_1_1_1,
    )


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_run_on_string_oid_graph(
    graphscope_session, p2p_property_graph_string, sssp_result
):
    # pregel
    a1 = SSSP_Pregel()
    ctx1 = a1(p2p_property_graph_string, src="6")
    r1 = ctx1.to_dataframe({"node": "v:person.id", "r": "r:person"})
    r1["node"] = r1["node"].astype(int)
    r1 = r1.sort_values(by=["node"]).to_numpy(dtype=float)
    r1[r1 == 1000000000.0] = float("inf")
    assert np.allclose(r1, sssp_result["directed"])
    # pie
    a2 = SSSP_PIE()
    ctx2 = a2(p2p_property_graph_string, src="6")
    r2 = ctx2.to_dataframe({"node": "v:person.id", "r": "r:person"})
    r2["node"] = r2["node"].astype(int)
    r2 = r2.sort_values(by=["node"]).to_numpy(dtype=float)
    r2[r2 == 1000000000.0] = float("inf")
    assert np.allclose(r2, sssp_result["directed"])


def test_pregel_api(graphscope_session, ldbc_graph):
    a1 = Pregel_API_Test()
    a1(ldbc_graph, param1="graphscope", param2="graphscope2")


def test_pie_api(graphscope_session, ldbc_graph_undirected):
    a1 = PIE_API_Test()
    a1(ldbc_graph_undirected, param1="graphscope", param2="graphscope2")


def test_app_on_local_vm_graph(
    p2p_property_graph_undirected_local_vm_string,
    p2p_property_graph_undirected_local_vm_int32,
):
    a1 = Pregel_GetSchema()
    a1(p2p_property_graph_undirected_local_vm_string)
    a2 = PIE_GetSchema()
    a2(p2p_property_graph_undirected_local_vm_int32)
    # Comment out some base case to reduce the compile time.
    # a1(p2p_property_graph_undirected_local_vm_string)
    # a1(p2p_property_graph_undirected_local_vm_int32)
