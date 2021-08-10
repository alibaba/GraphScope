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

import json
import os

import numpy as np
import pytest
from gremlin_python import statics

import graphscope
from graphscope.analytical.udf.decorators import step
from graphscope.framework.loader import Loader
from graphscope.learning.extra import ___

statics.load_statics(globals())

graphscope.set_option(show_log=True)
graphscope.set_option(initializing_interactive_engine=False)

test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")
property_dir = os.path.join(test_repo_dir, "property")


@pytest.fixture
def ogbn_mag_small():
    return "{}/ogbn_mag_small".format(test_repo_dir)


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
    g.unload()


QUERY_1 = (
    "g.V().process("
    "V().property('$pr', expr('1.0/TOTAL_V'))"
    ".repeat("
    "V().property('$tmp', expr('$pr/OUT_DEGREE'))"
    ".scatter('$tmp').by(out())"
    ".gather('$tmp', sum)"
    ".property('$new', expr('0.15/TOTAL_V+0.85*$tmp'))"
    ".where(expr('abs($new-$pr)>1e-10'))"
    ".property('$pr', expr('$new')))"
    ".until(count().is(0))"
    ").withProperty('$pr', 'pr')"
    ".order().by('pr', desc).limit(10).valueMap('id', 'pr')"
)


@graphscope.step()
class SSSP(graphscope.PIE):
    def Init(g, context):
        v_label_num = g.vertex_label_num()
        for v_label_id in range(v_label_num):
            nodes = g.nodes(v_label_id)
            context.init_value(
                nodes, v_label_id, 1000000000.0, PIEAggregateType.kMinAggregate
            )
            context.register_sync_buffer(v_label_id, MessageStrategy.kSyncOnOuterVertex)

    def PEval(g, context):
        graphscope.declare(graphscope.Vertex, source)
        graphscope.declare(graphscope.VertexVector, updates)
        self.d = context.get_param(b"distProperty")
        self.p = context.get_param(b"edgeProperty")
        src = int(context.get_param(b"srcID"))
        g[context.get_param(b"srcID")][self.d] = 0
        if g.get_inner_node(src, source):
            updates.push_back(source)
            dijkstra(g, updates)

    def IncEval(g, updates):
        dijkstra(g, updates)

    def dijkstra(g, updates):
        heap = VertexHeap(g, self.p)
        for i in updates:
            val = g[i][self.d]
            heap.push(i, -val)
        while not heap.empty():
            u = heap.top().second
            distu = -heap.top().first
            heap.pop()
            for e in g.get_outgoing_edges(u):
                v = e.get_neighbor()
                distv = distu + e.data(self.p)
                if g[v][self.d] > distv:
                    g[v][self.d] = distv
                    if g.is_inner_node(v):
                        heap.push(v, -distv)


def demo(sess, graph):
    # Interactive engine
    interactive = sess.gremlin(graph)
    # case1: PageRank Orderby with String
    ret = interactive.execute(
        QUERY_1,
        request_options={"engine": "gae"},
    ).one()
    assert len(ret) == 10
    for item in ret:
        assert "id" in item
        assert "pr" in item
    # case2: PageRank Orderby with traversal source
    g = interactive.traversal_source()
    ret = (
        g.V()
        .process(
            V()
            .property("$pr", expr("1.0/TOTAL_V"))
            .repeat(
                V()
                .property("$tmp", expr("$pr/OUT_DEGREE"))
                .scatter("$tmp")
                .by(out())
                .gather("$tmp", sum)
                .property("$new", expr("0.15/TOTAL_V+0.85*$tmp"))
                .where(expr("abs($new-$pr)>1e-10"))
                .property("$pr", expr("$new"))
            )
            .until(count().is_(0))
        )
        .withProperty("$pr", "pr")
        .order()
        .by("pr", desc)
        .limit(10)
        .valueMap("id", "pr")
        .toList()
    )
    assert len(ret) == 10
    for item in ret:
        assert "id" in item
        assert "pr" in item
    # case3: GraphLearn Sample
    ret = (
        g.V()
        .process(
            V()
            .property("$pr", expr("1.0/TOTAL_V"))
            .repeat(
                V()
                .property("$tmp", expr("$pr/OUT_DEGREE"))
                .scatter("$tmp")
                .by(out())
                .gather("$tmp", sum)
                .property("$new", expr("0.15/TOTAL_V+0.85*$tmp"))
                .where(expr("abs($new-$pr)>1e-10"))
                .property("$pr", expr("$new"))
            )
            .until(count().is_(0))
        )
        .withProperty("$pr", "pr")
        .sample(
            ___.V("person").batch(64).outV("knows").sample(10).by("random").values()
        )
        .toTensorFlowDataset()
        .toList()
    )
    print("[Ret]: ", type(ret))
    # case4: UDF SSSP
    sess.registerUDF("SSSP", SSSP)
    ret = (
        g.V()
        .process("SSSP")
        .with_("edgeProperty", "weight")
        .with_("distProperty", "$dist")
        .with_("srcID", 6)
        .withProperty("$dist", "dist2")
        .toList()
    )
    print(ret[0])


def test_query_1(graphscope_session, p2p_property_graph):
    demo(graphscope_session, p2p_property_graph)
