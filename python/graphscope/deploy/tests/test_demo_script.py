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
import random
import string
import subprocess
import sys

import pytest

import graphscope
from graphscope.config import GSConfig as gs_config
from graphscope.dataset.ldbc import load_ldbc
from graphscope.dataset.modern_graph import load_modern_graph
from graphscope.framework.loader import Loader

graphscope.set_option(show_log=True)
logger = logging.getLogger("graphscope")


@pytest.fixture
def data_dir():
    return "/testingdata/ldbc_sample"


@pytest.fixture
def modern_graph_data_dir():
    return "/testingdata/modern_graph"


def get_gs_image_on_ci_env():
    if "GS_IMAGE" in os.environ:
        return os.environ["GS_IMAGE"]
    else:
        return gs_config.GS_IMAGE


def test_demo(data_dir):
    image = get_gs_image_on_ci_env()
    sess = graphscope.session(
        num_workers=1,
        k8s_gs_image=image,
        k8s_coordinator_cpu=0.5,
        k8s_coordinator_mem="2500Mi",
        k8s_vineyard_cpu=0.1,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=0.1,
        k8s_engine_mem="1500Mi",
        k8s_vineyard_shared_mem="2Gi",
    )
    graph = load_ldbc(sess, data_dir)

    # Interactive engine
    interactive = sess.gremlin(graph)
    sub_graph = interactive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )

    # Analytical engine
    # project the projected graph to simple graph.
    simple_g = sub_graph.project_to_simple(v_label="person", e_label="knows")

    pr_result = graphscope.pagerank(simple_g, delta=0.8)
    tc_result = graphscope.triangles(simple_g)

    # add the PageRank and triangle-counting results as new columns to the property graph
    # FIXME: Add column to sub_graph
    sub_graph.add_column(pr_result, {"Ranking": "r"})
    sub_graph.add_column(tc_result, {"TC": "r"})

    # GNN engine
    sess.close()


def test_demo_distribute(data_dir, modern_graph_data_dir):
    image = get_gs_image_on_ci_env()
    sess = graphscope.session(
        num_workers=1,
        k8s_gs_image=image,
        k8s_coordinator_cpu=0.5,
        k8s_coordinator_mem="2500Mi",
        k8s_vineyard_cpu=0.1,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=0.1,
        k8s_engine_mem="1500Mi",
        k8s_vineyard_shared_mem="2Gi",
    )
    graph = load_ldbc(sess, data_dir)

    # Interactive engine
    interactive = sess.gremlin(graph)
    sub_graph = interactive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )
    person_count = (
        interactive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    knows_count = (
        interactive.execute('g.V().hasLabel("person").outE("knows").count()')
        .all()
        .result()[0]
    )
    interactive2 = sess.gremlin(sub_graph)
    sub_person_count = interactive2.execute("g.V().count()").all().result()[0]
    sub_knows_count = interactive2.execute("g.E().count()").all().result()[0]
    assert person_count == sub_person_count
    assert knows_count == sub_knows_count

    # Analytical engine
    # project the projected graph to simple graph.
    simple_g = sub_graph.project_to_simple(v_label="person", e_label="knows")

    pr_result = graphscope.pagerank(simple_g, delta=0.8)
    tc_result = graphscope.triangles(simple_g)

    # add the PageRank and triangle-counting results as new columns to the property graph
    # FIXME: Add column to sub_graph
    sub_graph.add_column(pr_result, {"Ranking": "r"})
    sub_graph.add_column(tc_result, {"TC": "r"})

    # test subgraph on modern graph
    mgraph = load_modern_graph(sess, modern_graph_data_dir)

    # Interactive engine
    minteractive = sess.gremlin(mgraph)
    msub_graph = minteractive.subgraph(  # noqa: F841
        'g.V().hasLabel("person").outE("knows")'
    )
    person_count = (
        minteractive.execute(
            'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
        )
        .all()
        .result()[0]
    )
    msub_interactive = sess.gremlin(msub_graph)
    sub_person_count = msub_interactive.execute("g.V().count()").all().result()[0]
    assert person_count == sub_person_count

    # GNN engine
    sess.close()


def test_multiple_session(data_dir):
    namespace = "gs-multi-" + "".join(
        [random.choice(string.ascii_lowercase) for _ in range(6)]
    )

    image = get_gs_image_on_ci_env()
    sess = graphscope.session(
        num_workers=1,
        k8s_gs_image=image,
        k8s_coordinator_cpu=0.5,
        k8s_coordinator_mem="2500Mi",
        k8s_vineyard_cpu=0.1,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=0.1,
        k8s_engine_mem="1500Mi",
        k8s_vineyard_shared_mem="2Gi",
    )
    info = sess.info
    assert info["status"] == "active"
    assert info["type"] == "k8s"
    assert len(info["engine_hosts"].split(",")) == 1

    sess2 = graphscope.session(
        k8s_namespace=namespace,
        num_workers=2,
        k8s_gs_image=image,
        k8s_coordinator_cpu=0.5,
        k8s_coordinator_mem="2500Mi",
        k8s_vineyard_cpu=0.1,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=0.1,
        k8s_engine_mem="1500Mi",
        k8s_vineyard_shared_mem="2Gi",
    )

    info = sess2.info
    assert info["status"] == "active"
    assert info["type"] == "k8s"
    assert len(info["engine_hosts"].split(",")) == 2

    sess2.close()
    sess.close()


def test_query_modern_graph(modern_graph_data_dir):
    image = get_gs_image_on_ci_env()
    sess = graphscope.session(
        num_workers=1,
        k8s_gs_image=image,
        k8s_coordinator_cpu=0.5,
        k8s_coordinator_mem="2500Mi",
        k8s_vineyard_cpu=0.1,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=0.1,
        k8s_engine_mem="1500Mi",
        k8s_vineyard_shared_mem="2Gi",
    )
    graph = load_modern_graph(sess, modern_graph_data_dir)
    interactive = sess.gremlin(graph)
    queries = [
        "g.V().has('name','marko').count()",
        "g.V().has('person','name','marko').count()",
        "g.V().has('person','name','marko').outE('created').count()",
        "g.V().has('person','name','marko').outE('created').inV().count()",
        "g.V().has('person','name','marko').out('created').count()",
        "g.V().has('person','name','marko').out('created').values('name').count()",
    ]
    for q in queries:
        result = interactive.execute(q).all().result()[0]
        assert result == 1


def test_traversal_modern_graph(modern_graph_data_dir):
    from gremlin_python.process.traversal import Order
    from gremlin_python.process.traversal import P

    image = get_gs_image_on_ci_env()
    sess = graphscope.session(
        show_log=True,
        num_workers=1,
        k8s_gs_image=image,
        k8s_coordinator_cpu=0.5,
        k8s_coordinator_mem="2500Mi",
        k8s_vineyard_cpu=0.1,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=0.1,
        k8s_engine_mem="1500Mi",
        k8s_vineyard_shared_mem="2Gi",
    )
    graph = load_modern_graph(sess, modern_graph_data_dir)
    interactive = sess.gremlin(graph)
    g = interactive.traversal_source()
    assert g.V().has("name", "marko").count().toList()[0] == 1
    assert g.V().has("person", "name", "marko").count().toList()[0] == 1
    assert g.V().has("person", "name", "marko").outE("created").count().toList()[0] == 1
    assert (
        g.V().has("person", "name", "marko").outE("created").inV().count().toList()[0]
        == 1
    )
    assert g.V().has("person", "name", "marko").out("created").count().toList()[0] == 1
    assert (
        g.V()
        .has("person", "name", "marko")
        .out("created")
        .values("name")
        .count()
        .toList()[0]
        == 1
    )
    assert (
        g.V()
        .hasLabel("person")
        .has("age", P.gt(30))
        .order()
        .by("age", Order.desc)
        .count()
        .toList()[0]
        == 2
    )
