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
from networkx.exception import NetworkXError
from networkx.testing.utils import assert_graphs_equal
from networkx.utils.misc import default_opener

import graphscope
import graphscope.nx as nx
from graphscope.client.session import g
from graphscope.client.session import get_default_session
from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.loader import Loader
from graphscope.proto import graph_def_pb2


def ldbc_sample_single_label(prefix, directed):
    graph = graphscope.g(directed=directed, generate_eid=False)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "comment_0_0.csv"), delimiter="|"), "comment"
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), delimiter="|"),
        "replyOf",
    )
    return graph


def ldbc_sample_string_oid(prefix, directed):
    graph = graphscope.g(directed=directed, oid_type="string", generate_eid=False)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "comment_0_0.csv"), delimiter="|"), "comment"
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), delimiter="|"),
        "replyOf",
    )
    return graph


def ldbc_sample_single_label_with_sess(sess, prefix, directed):
    graph = sess.g(directed=directed, generate_eid=False)
    graph = graph.add_vertices(
        Loader(os.path.join(prefix, "comment_0_0.csv"), delimiter="|"), "comment"
    )
    graph = graph.add_edges(
        Loader(os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), delimiter="|"),
        "replyOf",
    )
    return graph


def ldbc_sample_multi_labels(prefix, directed):
    graph = graphscope.g(directed=directed, generate_eid=False)
    graph = (
        graph.add_vertices(
            Loader(os.path.join(prefix, "comment_0_0.csv"), delimiter="|"),
            "comment",
            ["creationDate", "locationIP", "browserUsed", "content", "length"],
        )
        .add_vertices(
            Loader(os.path.join(prefix, "person_0_0.csv"), delimiter="|"),
            "person",
            [
                "firstName",
                "lastName",
                "gender",
                ("birthday", str),
                "creationDate",
                "locationIP",
                "browserUsed",
            ],
        )
        .add_vertices(
            Loader(os.path.join(prefix, "post_0_0.csv"), delimiter="|"),
            "post",
            [
                "imageFile",
                "creationDate",
                "locationIP",
                "browserUsed",
                "language",
                "content",
                "length",
            ],
        )
    )
    graph = (
        graph.add_edges(
            Loader(
                os.path.join(prefix, "comment_replyOf_comment_0_0.csv"), delimiter="|"
            ),
            "replyOf",
            src_label="comment",
            dst_label="comment",
        )
        .add_edges(
            Loader(os.path.join(prefix, "person_knows_person_0_0.csv"), delimiter="|"),
            "knows",
            ["creationDate"],
            src_label="person",
            dst_label="person",
        )
        .add_edges(
            Loader(os.path.join(prefix, "comment_replyOf_post_0_0.csv"), delimiter="|"),
            "replyOf2",
            src_label="comment",
            dst_label="post",
        )
    )
    return graph


def load_p2p(prefix, directed):
    graph = graphscope.load_from(
        edges={
            "group": {
                "loader": Loader(
                    os.path.join(prefix, "p2p-31.e"), header_row=False, delimiter=" "
                )
            }
        },
        directed=directed,
        generate_eid=False,
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
        cls.p2p = load_p2p(os.path.expandvars("${GS_TEST_DIR}"), False)
        cls.p2p_nx = nx.read_edgelist(
            os.path.expandvars("${GS_TEST_DIR}/dynamic/p2p-31_dynamic.edgelist"),
            nodetype=int,
            data=True,
        )
        cls.str_oid_g = ldbc_sample_string_oid(cls.data_dir, False)

    @classmethod
    def teardown_class(cls):
        cls.single_label_g.unload()
        cls.multi_label_g.unload()
        cls.str_oid_g.unload()

    def assert_convert_success(self, gs_g, nx_g):
        assert gs_g.is_directed() == nx_g.is_directed()
        assert self._schema_equal(gs_g.schema, nx_g.schema)

    def _schema_equal(self, gs_schema, nx_schema):
        v_props = {}
        for entry in gs_schema._valid_vertex_labels():
            for prop in entry.properties:
                v_props[prop.name] = prop.type
        e_props = {}
        for entry in gs_schema._valid_edge_labels():
            for prop in entry.properties:
                e_props[prop.name] = prop.type
        gs_v_props = {
            prop.name: prop.type
            for prop in list(nx_schema._valid_vertex_labels())[0].properties
        }
        gs_e_props = {
            prop.name: prop.type
            for prop in list(nx_schema._valid_edge_labels())[0].properties
        }
        return v_props == gs_v_props and e_props == gs_e_props

    # nx to gs
    def test_empty_nx_to_gs(self):
        empty_nx_g = self.NXGraph(dist=True)
        gs_g = g(empty_nx_g)
        self.assert_convert_success(gs_g, empty_nx_g)

    def test_only_contains_nodes_nx_to_gs(self):
        nx_g = self.NXGraph(dist=True)
        nx_g.add_nodes_from(range(100), type="node")
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

    def test_simple_nx_to_gs(self):
        nx_g = nx.complete_graph(10, create_using=self.NXGraph)
        gs_g = g(nx_g)
        self.assert_convert_success(gs_g, nx_g)

    def test_int_node_nx_to_gs(self):
        nx_g = self.NXGraph(dist=True)
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
        nx_g = self.NXGraph(dist=True)
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
        nx_g = self.NXGraph(dist=True)
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
        nx_g = self.NXGraph(dist=True)
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
        nx_g = self.NXGraph(dist=True)
        nx_g._graph = None  # graph view always has a _graph attribute
        nx_g._is_client_view = False
        with pytest.raises(TypeError, match="graph view can not convert to gs graph"):
            gs_g = g(nx_g)

    def test_error_on_mixing_node_nx_to_gs(self):
        nx_g = self.NXGraph(dist=True)
        nx_g.add_node(0, weight=1.23)
        nx_g.add_node("zakky", foo="node")
        with pytest.raises(
            RuntimeError,
            match="The vertex type is not consistent <class 'int'> vs <class 'str'>, can not convert it to arrow graph",
        ):
            gs_g = g(nx_g)

    # gs to nx
    def test_empty_gs_to_nx(self):
        empty_nx = self.NXGraph(dist=True)
        empty_gs_graph = g(empty_nx)
        G = self.NXGraph(empty_gs_graph)
        self.assert_convert_success(empty_gs_graph, G)

    def test_single_label_gs_to_nx(self):
        G = self.NXGraph(self.single_label_g)
        assert G.number_of_nodes() == 76830
        assert G.number_of_edges() == 38786
        assert 618475290625 not in G
        assert ("comment", 618475290625) in G
        G2 = self.NXGraph(self.single_label_g, default_label="comment")
        assert G2.number_of_nodes() == 76830
        assert G2.number_of_edges() == 38786
        assert 618475290625 in G2
        assert ("comment", 618475290625) not in G2

    def test_multi_label_gs_to_nx(self):
        G = self.NXGraph(self.multi_label_g)
        assert G.number_of_nodes() == (76830 + 903 + 78976)
        assert G.number_of_edges() == (38786 + 6626 + 38044)
        assert 618475290625 not in G  # comment node is (label, id) format
        assert ("comment", 618475290625) in G
        assert 933 not in G  # person node is (label, id) format
        assert ("person", 933) in G
        assert 618475290624 not in G  # post node is (label, id) format
        assert ("post", 618475290624) in G
        G2 = self.NXGraph(self.multi_label_g, default_label="comment")
        assert G2.number_of_nodes() == (76830 + 903 + 78976)
        assert G2.number_of_edges() == (38786 + 6626 + 38044)
        assert 618475290625 in G2  # comment node is default label node
        assert ("comment", 618475290625) not in G2
        assert 933 not in G2  # person node is (label, id) format
        assert ("person", 933) in G2
        assert 618475290624 not in G2  # post node is (label, id) format
        assert ("post", 618475290624) in G

    def test_report_methods_on_copy_on_write_strategy(self):
        G = self.NXGraph(self.multi_label_g, default_label="person")
        assert G.graph_type == graph_def_pb2.ARROW_PROPERTY
        # test NODE_NUM and EDGE_NUM
        assert G.number_of_nodes() == (76830 + 903 + 78976)
        assert G.number_of_edges() == (38786 + 6626 + 38044)
        # test HAS_NODE and HAS_EDGE
        assert 0 not in G
        assert 933 in G
        assert ("person", 933) not in G  # deault node must be non-tuple format
        assert ("random", 933) not in G
        assert G.has_edge(933, 4398046511628)
        assert G.has_edge(("comment", 618475290625), ("post", 618475290624))
        assert not G.has_edge(933, ("post", 618475290624))
        # test GET_NODE_DATA and GET_EDGE_DATA
        assert G.get_node_data(933) == {
            "browserUsed": "Firefox",
            "locationIP": "119.235.7.103",
            "creationDate": "2010-02-14T15:32:10.447+0000",
            "birthday": "1989-12-03",
            "gender": "male",
            "lastName": "Perera",
            "firstName": "Mahinda",
        }
        assert G.get_edge_data(933, 4398046511628) == {
            "creationDate": "2010-07-30T15:19:53.298+0000",
        }
        assert sorted(list(G.neighbors(933))) == [
            4398046511628,
            8796093023017,
            28587302322537,
        ]
        if G.is_directed():
            assert sorted(list(G.predecessors(4398046511628))) == [
                318,
                933,
                987,
                2199023256530,
            ]

        G.add_node(0)  # modify graph to make copy on write
        assert G.graph_type == graph_def_pb2.DYNAMIC_PROPERTY
        assert G.number_of_nodes() == (76831 + 903 + 78976)
        assert G.number_of_edges() == (38786 + 6626 + 38044)
        # test HAS_NODE and HAS_EDGE
        assert 0 in G
        assert 933 in G
        assert ("person", 933) not in G
        assert ("random", 933) not in G
        assert G.has_edge(933, 4398046511628)
        assert G.has_edge(("comment", 618475290625), ("post", 618475290624))
        assert not G.has_edge(618475290625, ("post", 618475290624))
        # test GET_NODE_DATA and GET_EDGE_DATA
        assert G.get_node_data(933) == {
            "browserUsed": "Firefox",
            "locationIP": "119.235.7.103",
            "creationDate": "2010-02-14T15:32:10.447+0000",
            "birthday": "1989-12-03",
            "gender": "male",
            "lastName": "Perera",
            "firstName": "Mahinda",
        }
        assert G.get_edge_data(933, 4398046511628) == {
            "creationDate": "2010-07-30T15:19:53.298+0000",
        }
        assert sorted(list(G.neighbors(933))) == [
            4398046511628,
            8796093023017,
            28587302322537,
        ]
        if G.is_directed():
            assert sorted(list(G.predecessors(4398046511628))) == [
                318,
                933,
                987,
                2199023256530,
            ]

    def test_str_oid_gs_to_nx(self):
        g = self.str_oid_g
        nx_g = self.NXGraph(g)
        self.assert_convert_success(g, nx_g)

    @pytest.mark.skip(reason="TODO: open after supporting run app on arrow_property")
    def test_gs_to_nx_with_sssp(self):
        nx_g = self.NXGraph(self.p2p)
        ret = nx.builtin.single_source_dijkstra_path_length(nx_g, 6, weight="f2")
        ret2 = nx.builtin.single_source_dijkstra_path_length(
            self.p2p_nx, 6, weight="weight"
        )
        assert ret == ret2

    def test_error_on_wrong_nx_type(self):
        g = self.single_label_g
        with pytest.raises(NetworkXError):
            nx_g = nx.DiGraph(g)

    @pytest.mark.skip(reason="FIXME: multiple session crash in ci.")
    def test_multiple_sessions(self):
        sess2 = graphscope.session(cluster_type="hosts", num_workers=1)
        nx2 = sess2.nx()
        gs_g = self.single_label_g

        if self.NXGraph is nx.Graph:
            gs_g2 = ldbc_sample_single_label_with_sess(sess2, self.data_dir, False)
        else:
            gs_g2 = ldbc_sample_single_label_with_sess(sess2, self.data_dir, True)
        assert gs_g.session_id != gs_g2.session_id

        nx_g = self.NXGraph(gs_g, dist=True)
        if nx_g.is_directed():
            nx_g2 = nx2.DiGraph(gs_g2, dist=True)
        else:
            nx_g2 = nx2.Graph(gs_g2, dist=True)
        self.assert_convert_success(gs_g2, nx_g2)
        assert nx_g.session_id == gs_g.session_id
        assert nx_g2.session_id == gs_g2.session_id

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

        # error raise if gs graph and nx graph not in the same session.
        with pytest.raises(
            RuntimeError,
            match="graphscope graph and networkx graph not in the same session.",
        ):
            tmp = self.NXGraph(gs_g2)
        with pytest.raises(
            RuntimeError,
            match="networkx graph and graphscope graph not in the same session.",
        ):
            tmp = g(nx_g2)
            print(tmp.session_id, nx_g2.session_id)

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
            sg1.schema.vdata_type == graph_def_pb2.NULLVALUE
            and sg1.schema.edata_type == graph_def_pb2.NULLVALUE
        )

        # to_simple with e_prop
        sg2 = self.g._project_to_simple(e_prop="edata_float")
        assert (
            sg2.schema.vdata_type == graph_def_pb2.NULLVALUE
            and sg2.schema.edata_type == graph_def_pb2.DOUBLE
        )

        # to_simple with v_prop
        sg3 = self.g._project_to_simple(v_prop="vdata_str")
        assert (
            sg3.schema.vdata_type == graph_def_pb2.STRING
            and sg3.schema.edata_type == graph_def_pb2.NULLVALUE
        )

        # to_simple with e_prop and v_prop
        sg4 = self.g._project_to_simple(v_prop="vdata_int", e_prop="edata_str")
        assert (
            sg4.schema.vdata_type == graph_def_pb2.LONG
            and sg4.schema.edata_type == graph_def_pb2.STRING
        )

        # empty graph to simple
        empty_g = self.NXGraph()
        sg5 = empty_g._project_to_simple()
        assert (
            sg5.schema.vdata_type == graph_def_pb2.NULLVALUE
            and sg5.schema.edata_type == graph_def_pb2.NULLVALUE
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
        cls.p2p = load_p2p(os.path.expandvars("${GS_TEST_DIR}"), True)
        cls.p2p_nx = nx.read_edgelist(
            os.path.expandvars("${GS_TEST_DIR}/dynamic/p2p-31_dynamic.edgelist"),
            nodetype=int,
            data=True,
            create_using=nx.DiGraph,
        )
        cls.str_oid_g = ldbc_sample_string_oid(data_dir, True)

    @classmethod
    def teardown_class(cls):
        cls.single_label_g.unload()
        cls.multi_label_g.unload()
        cls.str_oid_g.unload()

    def test_error_on_wrong_nx_type(self):
        g = self.single_label_g
        with pytest.raises(NetworkXError):
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


@pytest.mark.usefixtures("graphscope_session")
class TestImportNetworkxModuleWithSession(object):
    @classmethod
    def setup_class(cls):
        cls.session1 = graphscope.session(cluster_type="hosts", num_workers=1)
        cls.session2 = graphscope.session(cluster_type="hosts", num_workers=1)
        cls.session_lazy = graphscope.session(
            cluster_type="hosts", num_workers=1, mode="lazy"
        )

    def test_import(self):
        import graphscope.nx as nx_default

        nx1 = self.session1.nx()
        nx2 = self.session2.nx()
        G = nx_default.Graph()
        G1 = nx1.Graph()
        G2 = nx2.Graph()
        assert G.session_id == get_default_session().session_id
        assert G1.session_id == self.session1.session_id
        assert G2.session_id == self.session2.session_id

        self.session1.close()
        self.session2.close()

    def test_error_import_with_wrong_session(self):
        with pytest.raises(
            RuntimeError,
            match="Networkx module need the session to be eager mode. Current session is lazy mode.",
        ):
            nx = self.session_lazy.nx()
        self.session_lazy.close()
