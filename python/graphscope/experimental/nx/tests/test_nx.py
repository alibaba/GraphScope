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

import pytest

import graphscope
import graphscope.experimental.nx as nx
from graphscope.client.session import g
from graphscope.framework.errors import AnalyticalEngineInternalError
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.loader import Loader
from graphscope.proto import types_pb2


@pytest.fixture(scope="session")
def graphscope_session():
    graphscope.set_option(show_log=True)
    graphscope.set_option(initializing_interactive_engine=False)

    sess = graphscope.session(cluster_type="hosts", num_workers=1)
    sess.as_default()
    yield sess
    sess.close()


def ldbc_sample_single_label(prefix, directed):
    graph = graphscope.g(directed=directed)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "comment_0_0.csv"), delimiter="|"), "comment"
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), delimiter="|"),
        "replyOf",
    )
    return graph


def ldbc_sample_single_label_with_sess(sess, prefix, directed):
    graph = sess.g(directed=directed)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "comment_0_0.csv"), delimiter="|"), "comment"
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), delimiter="|"),
        "replyOf",
    )
    return graph


def ldbc_sample_multi_labels(prefix, directed):
    graph = graphscope.g(directed=directed)
    graph = (
        graph.add_vertices(
            Loader(os.path.join(prefix, "comment_0_0.csv"), delimiter="|"), "comment"
        )
        .add_vertices(
            Loader(os.path.join(prefix, "person_0_0.csv"), delimiter="|"), "person"
        )
        .add_vertices(
            Loader(os.path.join(prefix, "post_0_0.csv"), delimiter="|"),
            "post",
        )
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), delimiter="|"),
        "replyOf",
        src_label="comment",
        dst_label="comment",
    ).add_edges(
        Loader(os.path.join(prefix, "person_knows_person_0_0.csv"), delimiter="|"),
        "knows",
        ["creationDate"],
        src_label="person",
        dst_label="person",
    )
    return graph


def ldbc_sample_with_duplicated_oid(prefix, directed):
    graph = graphscope.g(directed=directed)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "place_0_0.csv"), delimiter="|"), "place"
    ).add_vertices(
        Loader(os.path.join(prefix, "person_0_0.csv"), delimiter="|"), "person"
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "place_isPartOf_place_0_0.csv"), delimiter="|"),
        "isPartOf",
        src_label="place",
        dst_label="place",
    ).add_edges(
        Loader(os.path.join(prefix, "person_knows_person_0_0.csv"), delimiter="|"),
        "knows",
        ["creationDate"],
        src_label="person",
        dst_label="person",
    )
    return graph


@pytest.mark.usefixtures("graphscope_session")
class TestGraphTransformation(object):
    @classmethod
    def setup_class(cls):
        cls.NXGraph = nx.Graph

        cls.data_dir = os.path.expandvars("${GS_TEST_DIR}/ldbc_sample")
        cls.single_label_g = ldbc_sample_single_label(cls.data_dir, False)
        cls.multi_label_g = ldbc_sample_multi_labels(cls.data_dir, False)
        cls.duplicated_oid_g = ldbc_sample_with_duplicated_oid(cls.data_dir, False)

        # FIXME: this is tricky way to create a str gs graph
        les_g = nx.les_miserables_graph()
        cls.str_oid_g = g(les_g)

    @classmethod
    def teardown_class(cls):
        cls.single_label_g.unload()
        cls.multi_label_g.unload()
        cls.duplicated_oid_g.unload()

    def assert_convert_success(self, gs_g, nx_g):
        assert gs_g.is_directed() == nx_g.is_directed()
        assert self._schema_equal(gs_g.schema, nx_g.schema)

    def _schema_equal(self, gs_schema, nx_schema):
        v_props = {}
        for entry in gs_schema._valid_vertex_entries():
            for prop in entry.properties:
                v_props[prop.name] = prop.type
        e_props = {}
        for entry in gs_schema._valid_edge_entries():
            for prop in entry.properties:
                e_props[prop.name] = prop.type
        gs_v_props = {
            prop.name: prop.type
            for prop in list(nx_schema._valid_vertex_entries())[0].properties
        }
        gs_e_props = {
            prop.name: prop.type
            for prop in list(nx_schema._valid_edge_entries())[0].properties
        }
        return v_props == gs_v_props and e_props == gs_e_props

    # nx to gs
    def test_empty_nx_to_gs(self):
        empty_nx_g = self.NXGraph()
        gs_g = g(empty_nx_g)
        self.assert_convert_success(gs_g, empty_nx_g)

    def test_only_contains_nodes_nx_to_gs(self):
        nx_g = self.NXGraph()
        nx_g.add_nodes_from(range(100), type="node")
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

    def test_simple_nx_to_gs(self):
        nx_g = nx.complete_graph(10, create_using=self.NXGraph)
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

    def test_int_node_nx_to_gs(self):
        nx_g = self.NXGraph()
        nx_g.add_nodes_from(range(10), foo="star")
        nx_g.add_edges_from(
            [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9)],
            weight=3.14,
        )
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

    def test_str_node_nx_to_gs(self):
        nx_g = nx.les_miserables_graph()
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

    def test_complete_nx_to_gs(self):
        # multi-propery, node propery and edge propty both aligned
        nodes = [
            (0, {"vp1": 1, "vp2": "v", "vp3": 3.14}),
            (1, {"vp1": 1, "vp2": "v", "vp3": 3.14}),
            (2, {"vp1": 1, "vp2": "v", "vp3": 3.14}),
        ]
        edges = [
            (0, 1, {"ep1": 1, "ep2": "e", "ep3": 3.14}),
            (0, 2, {"ep1": 1, "ep2": "e", "ep3": 3.14}),
            (1, 2, {"ep1": 1, "ep2": "e", "ep3": 3.14}),
        ]
        nx_g = self.NXGraph()
        nx_g.update(edges, nodes)
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

        # node property aliged, edge not aliged
        nx_g2 = nx_g.copy()
        nx_g2.add_edge(0, 1, ep4="new propery")
        gs_g2 = g(nx_g2)
        self.assert_convert_success(gs_g2, nx_g2)

        # edge property aliged, node not aliged
        nx_g3 = nx_g.copy()
        nx_g3.add_node(2, vp4="new propery")
        gs_g3 = g(nx_g3)
        self.assert_convert_success(gs_g3, nx_g3)

        # both not aliged
        nx_g4 = nx_g.copy()
        nx_g4.add_edge(0, 1, ep4="new propery")
        nx_g4.add_node(2, vp4="new propery")
        gs_g4 = g(nx_g4)
        self.assert_convert_success(gs_g4, nx_g4)

    def test_nx_to_gs_after_modify(self):
        nx_g = self.NXGraph()
        nodes = [
            (0, {"vp1": 1, "vp2": "v", "vp3": 3.14}),
            (1, {"vp1": 1, "vp2": "v", "vp3": 3.14}),
            (2, {"vp1": 1, "vp2": "v", "vp3": 3.14}),
        ]
        # add nodes
        nx_g.add_nodes_from(nodes)
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

        # add_edges
        edges = [
            (0, 1, {"ep1": 1, "ep2": "e", "ep3": 3.14}),
            (0, 2, {"ep1": 1, "ep2": "e", "ep3": 3.14}),
            (1, 2, {"ep1": 1, "ep2": "e", "ep3": 3.14}),
        ]
        nx_g.add_edges_from(edges)
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

        # remove edge
        nx_g.remove_edge(0, 1)
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

        # remove node
        nx_g.remove_node(0)
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

        # clear
        nx_g.clear()
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

    def test_nx_to_gs_remove_nodes(self):
        nx_g = self.NXGraph()
        nx_g.add_nodes_from(range(10))  # all nodes are int
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)  # success

        nx_g.add_node("str_node")  # add a str node
        with pytest.raises(
            RuntimeError,
            match="The vertex type is not consistent <class 'int'> vs <class 'str'>, can not convert it to arrow graph",
        ):
            gs_g = g(nx_g)  # mixing oid type, failed

        nx_g.remove_node("str_node")  # remove str node, all nodes are int again
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)  # success

    def test_error_on_view_to_gs(self):
        nx_g = self.NXGraph()
        nx_g._graph = None  # graph view always has a _graph attribute
        with pytest.raises(TypeError, match="graph view can not convert to gs graph"):
            gs_g = g(nx_g)

    def test_error_on_mixing_node_nx_to_gs(self):
        nx_g = self.NXGraph()
        nx_g.add_node(0, weight=1.23)
        nx_g.add_node("zakky", foo="node")
        with pytest.raises(
            RuntimeError,
            match="The vertex type is not consistent <class 'int'> vs <class 'str'>, can not convert it to arrow graph",
        ):
            gs_g = g(nx_g)

    # gs to nx
    def test_empty_gs_to_nx(self):
        empty_nx = self.NXGraph()
        empty_gs_graph = g(empty_nx)
        nx_g = self.NXGraph(empty_gs_graph)
        self.assert_convert_success(empty_gs_graph, nx_g)

    def test_single_label_gs_to_nx(self):
        g = self.single_label_g
        nx_g = self.NXGraph(g)
        self.assert_convert_success(g, nx_g)

    def test_multi_label_gs_to_nx(self):
        g = self.multi_label_g
        nx_g = self.NXGraph(g)
        self.assert_convert_success(g, nx_g)

    def test_str_oid_gs_to_nx(self):
        g = self.str_oid_g
        nx_g = self.NXGraph(g)
        self.assert_convert_success(g, nx_g)

    def test_error_on_wrong_nx_type(self):
        g = self.single_label_g
        with pytest.raises(TypeError):
            nx_g = nx.DiGraph(g)

    def test_error_on_duplicate_oid(self):
        g = self.duplicated_oid_g
        with pytest.raises(AnalyticalEngineInternalError):
            nx_g = self.NXGraph(g)

    @pytest.mark.skip(reason="TODO: open it")
    def test_multiple_sessions(self):
        g = self.single_label_g
        sess2 = graphscope.session(cluster_type="hosts", num_workers=1)
        g2 = ldbc_sample_single_label_with_sess(sess2, self.data_dir, False)
        assert g.session_id != g2.session_id

        nx_g = self.NXGraph(g)
        nx_g2 = self.NXGraph(g2)
        self.assert_convert_success(g2, nx_g2)
        assert nx_g.session_id == g.session_id
        assert nx_g2.session_id == g2.session_id

        # copies
        cg1 = nx_g2.copy()
        assert cg1.session_id == nx_g2.session_id
        dg1 = nx_g2.to_directed()
        assert dg1.session_id == nx_g2.session_id
        dg2 = nx_g2.to_directed(as_view=True)
        assert dg2.session_id == nx_g2.session_id

        # subgraph
        sg1 = nx_g2.subgraph([274877907301, 274877907299])
        assert sg1.session_id == nx_g2.session_id
        sg2 = nx_g2.edge_subgraph([(274877907301, 274877907299)])
        assert sg2.session_id == nx_g2.session_id

        sess2.close()


@pytest.mark.usefixtures("graphscope_session")
class TestGraphProjectTest(object):
    @classmethod
    def setup_class(cls):
        cls.NXGraph = nx.Graph
        edgelist = os.path.expandvars("${GS_TEST_DIR}/dynamic/p2p-31_dynamic.edgelist")
        cls.g = nx.read_edgelist(
            edgelist, nodetype=int, data=True, create_using=cls.NXGraph
        )
        cls.g.add_node(1, vdata_str="kdjfao")
        cls.g.add_node(1, vdata_int=123)

    def test_project_to_simple(self):
        # default, e_prop='', v_prop=''
        sg1 = self.g._project_to_simple()
        assert (
            sg1.schema.vdata_type == types_pb2.NULLVALUE
            and sg1.schema.edata_type == types_pb2.NULLVALUE
        )

        # to_simple with e_prop
        sg2 = self.g._project_to_simple(e_prop="edata_float")
        assert (
            sg2.schema.vdata_type == types_pb2.NULLVALUE
            and sg2.schema.edata_type == types_pb2.DOUBLE
        )

        # to_simple with v_prop
        sg3 = self.g._project_to_simple(v_prop="vdata_str")
        assert (
            sg3.schema.vdata_type == types_pb2.STRING
            and sg3.schema.edata_type == types_pb2.NULLVALUE
        )

        # to_simple with e_prop and v_prop
        sg4 = self.g._project_to_simple(v_prop="vdata_int", e_prop="edata_str")
        assert (
            sg4.schema.vdata_type == types_pb2.INT64
            and sg4.schema.edata_type == types_pb2.STRING
        )

        # empty graph to simple
        empty_g = self.NXGraph()
        sg5 = empty_g._project_to_simple()
        assert (
            sg5.schema.vdata_type == types_pb2.NULLVALUE
            and sg5.schema.edata_type == types_pb2.NULLVALUE
        )
        with pytest.raises(
            InvalidArgumentError, match="graph not contains the vertex property foo"
        ):
            sg6 = empty_g._project_to_simple(v_prop="foo")

    @pytest.mark.skip(reason="It use much memory, exceeds the limit of Github runner")
    def test_implicit_project_to_simple(self):
        g = self.g
        nx.builtin.degree_centrality(g)
        nx.builtin.single_source_dijkstra_path_length(g, source=6, weight="weight")

    def test_error_on_not_exist_vertex_property(self):
        g = self.NXGraph()
        g.add_node(0, foo="node")
        with pytest.raises(
            InvalidArgumentError, match="graph not contains the vertex property weight"
        ):
            sg = g._project_to_simple(v_prop="weight")

    def test_error_on_not_exist_edge_property(self):
        g = self.NXGraph()
        g.add_edge(0, 1, weight=3)
        with pytest.raises(
            InvalidArgumentError, match="graph not contains the edge property type"
        ):
            sg = g._project_to_simple(e_prop="type")

    @pytest.mark.skip(reason="FIXME: engine can not catch the app throw error now")
    def test_error_on_some_edges_not_contain_property(self):
        g = self.g
        # some edges not contain the property
        with pytest.raises(RuntimeError):
            nx.builtin.single_source_dijkstra_path_length(
                g, source=6, weight="edata_random_int_0"
            )

    @pytest.mark.skip(reason="FIXME: engine can not catch the app throw error now")
    def test_error_on_some_edges_has_wrong_type(self):
        g = self.g.copy()
        # set edge a wrong type
        g[6][42]["weight"] = "a str"
        with pytest.raises(RuntimeError):
            nx.builtin.single_source_dijkstra_path_length(g, source=6, weight="weight")

    @pytest.mark.skip(reason="find a algorithm that use vertex data")
    def test_error_on_some_nodes_not_contain_property(self):
        g = self.g
        with pytest.raises(RuntimeError):
            nx.builtin.sssp(weight="vdata_random_int_0")

    @pytest.mark.skip(reason="find a algorithm that use vertex data")
    def test_error_on_some_nodes_has_wrong_type(self):
        g = self.g.copy()
        g[0]["weight"] = "a str"
        with pytest.raises(RuntimeError):
            nx.builtin.sssp(weight="weight")


@pytest.mark.usefixtures("graphscope_session")
class TestDigraphTransformation(TestGraphTransformation):
    @classmethod
    def setup_class(cls):
        cls.NXGraph = nx.DiGraph
        data_dir = os.path.expandvars("${GS_TEST_DIR}/ldbc_sample")
        cls.single_label_g = ldbc_sample_single_label(data_dir, True)
        cls.multi_label_g = ldbc_sample_multi_labels(data_dir, True)
        cls.duplicated_oid_g = ldbc_sample_with_duplicated_oid(data_dir, True)

        # FIXME: this is tricky way to create a str gs graph
        les_g = nx.les_miserables_graph()
        di_les_g = nx.DiGraph()
        di_les_g.add_edges_from(di_les_g.edges.data())
        cls.str_oid_g = g(di_les_g)

    @classmethod
    def teardown_class(cls):
        cls.single_label_g.unload()
        cls.multi_label_g.unload()
        cls.duplicated_oid_g.unload()

    def test_error_on_wrong_nx_type(self):
        g = self.single_label_g
        with pytest.raises(TypeError):
            nx_g = nx.Graph(g)


@pytest.mark.usefixtures("graphscope_session")
class TestDiGraphProjectTest(TestGraphProjectTest):
    @classmethod
    def setup_class(cls):
        cls.NXGraph = nx.DiGraph
        edgelist = os.path.expandvars("${GS_TEST_DIR}/dynamic/p2p-31_dynamic.edgelist")
        cls.g = nx.read_edgelist(
            edgelist, nodetype=int, data=True, create_using=cls.NXGraph
        )
        cls.g.add_node(0, vdata_str="kdjfao")
        cls.g.add_node(1, vdata_int=123)
