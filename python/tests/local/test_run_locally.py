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
import sys

import pytest

import graphscope
from graphscope.dataset.ogbn_mag import load_ogbn_mag

if sys.platform == "linux":
    from graphscope.learning.examples import GCN
    from graphscope.learning.graphlearn.python.model.tf import optimizer
    from graphscope.learning.graphlearn.python.model.tf.trainer import LocalTFTrainer

graphscope.set_option(show_log=True)
graphscope.set_option(initializing_interactive_engine=False)

test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")


def train(config, graph):
    def model_fn():
        return GCN(
            graph,
            config["class_num"],
            config["features_num"],
            config["batch_size"],
            val_batch_size=config["val_batch_size"],
            test_batch_size=config["test_batch_size"],
            categorical_attrs_desc=config["categorical_attrs_desc"],
            hidden_dim=config["hidden_dim"],
            in_drop_rate=config["in_drop_rate"],
            neighs_num=config["neighs_num"],
            hops_num=config["hops_num"],
            node_type=config["node_type"],
            edge_type=config["edge_type"],
            full_graph_mode=config["full_graph_mode"],
        )

    trainer = LocalTFTrainer(
        model_fn,
        epoch=config["epoch"],
        optimizer=optimizer.get_tf_optimizer(
            config["learning_algo"], config["learning_rate"], config["weight_decay"]
        ),
    )
    trainer.train_and_evaluate()


@pytest.fixture(scope="function")
def sess():
    s = graphscope.session(cluster_type="hosts", num_workers=2)
    yield s
    s.close()


@pytest.fixture(scope="function")
def sess_lazy():
    s = graphscope.session(cluster_type="hosts", num_workers=2, mode="lazy")
    yield s
    s.close()


@pytest.fixture(scope="function")
def sess_enable_gaia():
    s = graphscope.session(cluster_type="hosts", num_workers=2, enable_gaia=True)
    yield s
    s.close()


@pytest.fixture
def ogbn_mag_small():
    return "{}/ogbn_mag_small".format(test_repo_dir)


@pytest.fixture(scope="module")
def ogbn_small_script():
    script = "g.V().has('author', 'id', 2).out('writes').where(__.in('writes').has('id', 4307)).count()"
    return script


@pytest.fixture(scope="module")
def ogbn_small_bytecode():
    from gremlin_python.process.graph_traversal import __

    def func(g):
        assert (
            g.V()
            .has("author", "id", 2)
            .out("writes")
            .where(__.in_("writes").has("id", 4307))
            .count()
            .toList()[0]
            == 1
        )

    return func


def demo(sess, ogbn_mag_small, ogbn_small_script):
    graph = load_ogbn_mag(sess, ogbn_mag_small)

    # Interactive engine
    interactive = sess.gremlin(graph)
    papers = interactive.execute(ogbn_small_script).one()

    sub_graph = interactive.subgraph(
        "g.timeout(1000000).V().has('year', inside(2014, 2020)).outE('cites')"
    )

    simple_g = sub_graph.project(vertices={"paper": []}, edges={"cites": []})

    ret1 = graphscope.k_core(simple_g, k=5)
    ret2 = graphscope.triangles(simple_g)

    sub_graph = sub_graph.add_column(ret1, {"kcore": "r"})
    sub_graph = sub_graph.add_column(ret2, {"tc": "r"})

    # MacOS skip the GLE test
    if sys.platform == "darwin":
        return

    # GLE on ogbn_mag_small graph
    paper_features = []
    for i in range(128):
        paper_features.append("feat_" + str(i))
    paper_features.append("kcore")
    paper_features.append("tc")
    lg = sess.learning(
        sub_graph,
        nodes=[("paper", paper_features)],
        edges=[("paper", "cites", "paper")],
        gen_labels=[
            ("train", "paper", 100, (0, 75)),
            ("val", "paper", 100, (75, 85)),
            ("test", "paper", 100, (85, 100)),
        ],
    )

    # hyperparameters config.
    config = {
        "class_num": 349,  # output dimension
        "features_num": 130,  # 128 dimension + kcore + triangle count
        "batch_size": 500,
        "val_batch_size": 100,
        "test_batch_size": 100,
        "categorical_attrs_desc": "",
        "hidden_dim": 256,
        "in_drop_rate": 0.5,
        "hops_num": 2,
        "neighs_num": [5, 10],
        "full_graph_mode": False,
        "agg_type": "gcn",  # mean, sum
        "learning_algo": "adam",
        "learning_rate": 0.01,
        "weight_decay": 0.0005,
        "epoch": 5,
        "node_type": "paper",
        "edge_type": "cites",
    }

    train(config, lg)


def simple_flow(sess, ogbn_mag_small, ogbn_small_script):
    graph = load_ogbn_mag(sess, ogbn_mag_small)

    # Interactive engine
    interactive = sess.gremlin(graph)
    papers = interactive.execute(ogbn_small_script).one()

    # GLE on ogbn_mag_small graph
    paper_features = []
    for i in range(128):
        paper_features.append("feat_" + str(i))
    lg = sess.learning(
        graph,
        nodes=[("paper", paper_features)],
        edges=[("paper", "cites", "paper")],
        gen_labels=[
            ("train", "paper", 100, (0, 75)),
            ("val", "paper", 100, (75, 85)),
            ("test", "paper", 100, (85, 100)),
        ],
    )

    # hyperparameters config.
    config = {
        "class_num": 349,  # output dimension
        "features_num": 128,
        "batch_size": 500,
        "val_batch_size": 100,
        "test_batch_size": 100,
        "categorical_attrs_desc": "",
        "hidden_dim": 256,
        "in_drop_rate": 0.5,
        "hops_num": 2,
        "neighs_num": [5, 10],
        "full_graph_mode": False,
        "agg_type": "gcn",  # mean, sum
        "learning_algo": "adam",
        "learning_rate": 0.01,
        "weight_decay": 0.0005,
        "epoch": 5,
        "node_type": "paper",
        "edge_type": "cites",
    }

    train(config, lg)


def test_demo(sess, ogbn_mag_small, ogbn_small_script):
    demo(sess, ogbn_mag_small, ogbn_small_script)


def test_demo_lazy_mode(sess_lazy, ogbn_mag_small, ogbn_small_script):
    graph_node = load_ogbn_mag(sess_lazy, ogbn_mag_small)
    # Interactive query
    interactive_node = sess_lazy.gremlin(graph_node)
    paper_result_node = interactive_node.execute(
        ogbn_small_script,
        request_options={"engine": "gae"},
    ).one()
    sub_graph_node = interactive_node.subgraph(
        "g.timeout(1000000).V().has('year', inside(2014, 2020)).outE('cites')",
        request_options={"engine": "gae"},
    )
    simple_graph_node = sub_graph_node.project(
        vertices={"paper": []}, edges={"cites": []}
    )
    ret1 = graphscope.k_core(simple_graph_node, k=5)
    ret2 = graphscope.triangles(simple_graph_node)
    sub_graph_node = sub_graph_node.add_column(ret1, {"kcore": "r"})
    sub_graph_node = sub_graph_node.add_column(ret2, {"tc": "r"})
    # GLE on ogbn_mag_small graph
    paper_features = []
    for i in range(128):
        paper_features.append("feat_" + str(i))
    paper_features.append("kcore")
    paper_features.append("tc")
    learning_graph_node = sess_lazy.learning(
        sub_graph_node,
        nodes=[("paper", paper_features)],
        edges=[("paper", "cites", "paper")],
        gen_labels=[
            ("train", "paper", 100, (0, 75)),
            ("val", "paper", 100, (75, 85)),
            ("test", "paper", 100, (85, 100)),
        ],
    )
    # sess run
    # r[0]: gremlin result
    # r[1]: learning graph instance
    r = sess_lazy.run([paper_result_node, learning_graph_node])
    # hyperparameters config.
    config = {
        "class_num": 349,  # output dimension
        "features_num": 130,  # 128 dimension + kcore + triangle count
        "batch_size": 500,
        "val_batch_size": 100,
        "test_batch_size": 100,
        "categorical_attrs_desc": "",
        "hidden_dim": 256,
        "in_drop_rate": 0.5,
        "hops_num": 2,
        "neighs_num": [5, 10],
        "full_graph_mode": False,
        "agg_type": "gcn",  # mean, sum
        "learning_algo": "adam",
        "learning_rate": 0.01,
        "weight_decay": 0.0005,
        "epoch": 5,
        "node_type": "paper",
        "edge_type": "cites",
    }
    train(config, r[1])


def test_enable_gaia(
    sess_enable_gaia, ogbn_mag_small, ogbn_small_script, ogbn_small_bytecode
):
    graph = load_ogbn_mag(sess_enable_gaia, ogbn_mag_small)

    # Interactive engine
    interactive = sess_enable_gaia.gremlin(graph)
    papers = interactive.gaia().execute(ogbn_small_script).one()[0]
    assert papers == 1

    g = interactive.traversal_source()
    ogbn_small_bytecode(g.gaia())


@pytest.mark.skipif(sys.platform == "darwin", reason="Mac no need to run this test.")
def test_multiple_session(ogbn_mag_small, ogbn_small_script):
    sess1 = graphscope.session(cluster_type="hosts", num_workers=1)
    assert sess1.info["status"] == "active"

    sess2 = graphscope.session(cluster_type="hosts", num_workers=1)
    assert sess2.info["status"] == "active"

    simple_flow(sess1, ogbn_mag_small, ogbn_small_script)
    simple_flow(sess2, ogbn_mag_small, ogbn_small_script)

    sess1.close()
    sess2.close()
