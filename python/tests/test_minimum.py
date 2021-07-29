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

import pytest
from gremlin_python import statics

import graphscope
from graphscope.framework.loader import Loader

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


def demo(sess, graph):
    # Interactive engine
    interactive = sess.gremlin(graph)
    papers = interactive.execute(
        QUERY_1,
        request_options={"engine": "gae"},
    ).one()
    print(papers)
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
    print(ret)


def test_query_1(graphscope_session, p2p_property_graph):
    demo(graphscope_session, p2p_property_graph)
