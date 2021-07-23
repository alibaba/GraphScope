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
from graphscope.dataset.ogbn_mag import load_ogbn_mag

statics.load_statics(globals())

graphscope.set_option(show_log=True)
graphscope.set_option(initializing_interactive_engine=False)

test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")


@pytest.fixture(scope="module")
def sess():
    s = graphscope.session(cluster_type="hosts", num_workers=2)
    yield s
    s.close()


@pytest.fixture
def ogbn_mag_small():
    return "{}/ogbn_mag_small".format(test_repo_dir)


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
    ").with('$pr', 'pr')"
    ".order().by('pr', desc).limit(10).valueMap('name', 'pr')"
)


def demo(sess, ogbn_mag_small):
    graph = load_ogbn_mag(sess, ogbn_mag_small)

    # Interactive engine
    interactive = sess.gremlin(graph)
    papers = interactive.execute(
        "g.V().has('author', 'id', 2).out('writes').where(__.in('writes').has('id', 4307)).count()"
    ).one()
    print(papers)

    source_to_source_json = interactive.execute(
        QUERY_1, request_options={"engine": "gae"}
    ).one()
    assert len(source_to_source_json) == 1
    print(json.loads(source_to_source_json[0]))

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
        .with_("$pr", "pr")
        .order()
        .by("pr", desc)
        .limit(10)
        .valueMap("name", "pr")
        .toList()
    )
    print(ret)


def test_demo(sess, ogbn_mag_small):
    demo(sess, ogbn_mag_small)
