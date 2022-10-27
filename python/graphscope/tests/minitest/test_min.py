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
import sys

import pytest

try:
    # https://www.tensorflow.org/guide/migrate
    import tensorflow.compat.v1 as tf

    tf.disable_v2_behavior()
except ImportError:
    import tensorflow as tf

import graphscope
import graphscope.learning
import graphscope.nx as nx
from graphscope.analytical.udf.decorators import pregel
from graphscope.dataset import load_modern_graph
from graphscope.dataset import load_ogbn_mag
from graphscope.framework.app import AppAssets

logger = logging.getLogger("graphscope")

graphscope.set_option(show_log=True)


@pytest.fixture(scope="module")
def ogbn_small_script():
    script = "g.V().has('author', 'id', 2).out('writes').where(__.in('writes').has('id', 4307)).count()"
    return script


def train(config, graph):
    from graphscope.learning.examples import GCN
    from graphscope.learning.graphlearn.python.model.tf import optimizer
    from graphscope.learning.graphlearn.python.model.tf.trainer import LocalTFTrainer

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

    graphscope.learning.reset_default_tf_graph()
    trainer = LocalTFTrainer(
        model_fn,
        epoch=config["epoch"],
        optimizer=optimizer.get_tf_optimizer(
            config["learning_algo"], config["learning_rate"], config["weight_decay"]
        ),
    )
    trainer.train_and_evaluate()


def simple_flow(sess, ogbn_small_script):
    graph = load_ogbn_mag(sess)

    # Interactive engine
    interactive = sess.gremlin(graph)
    papers = interactive.execute(ogbn_small_script).one()

    # GLE on ogbn_mag_small graph
    paper_features = []
    for i in range(128):
        paper_features.append("feat_" + str(i))
    lg = sess.graphlearn(
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
        "epoch": 2,
        "node_type": "paper",
        "edge_type": "cites",
    }

    train(config, lg)


def test_minimum_udf_app():
    @pregel(vd_type="string", md_type="string")
    class DummyClass(AppAssets):
        @staticmethod
        def Init(v, context):
            pass

        @staticmethod
        def Compute(messages, v, context):
            v.vote_to_halt()

    s = graphscope.session(cluster_type="hosts", num_workers=1)
    g = load_ogbn_mag(s)
    a = DummyClass()
    r = a(g)
    s.close()


def test_minimum_networkx():
    s = graphscope.session(cluster_type="hosts", num_workers=2)
    s.as_default()
    # case-1 run app
    G = nx.path_graph(10)
    nx.builtin.pagerank(G)
    # case-2 transfer nx graph to gs graph
    nx_g = nx.Graph(dist=True)
    nx_g.add_nodes_from(range(100), type="node")
    gs_g = s.g(nx_g)
    s.close()


def test_multiple_session(ogbn_small_script):
    s1 = graphscope.session(cluster_type="hosts", num_workers=1)
    assert s1.info["status"] == "active"

    s2 = graphscope.session(cluster_type="hosts", num_workers=2)
    assert s2.info["status"] == "active"

    simple_flow(s1, ogbn_small_script)
    simple_flow(s2, ogbn_small_script)

    s1.close()
    s2.close()


def test_demo_with_default_session(ogbn_small_script):
    graph = load_ogbn_mag()

    # Interactive engine
    interactive = graphscope.gremlin(graph)
    papers = interactive.execute(ogbn_small_script).one()

    sub_graph = interactive.subgraph(
        "g.V().has('year', gt(2014).and(lt(2020))).outE('cites')"
    )

    simple_g = sub_graph.project(vertices={"paper": []}, edges={"cites": []})

    ret1 = graphscope.k_core(simple_g, k=5)
    ret2 = graphscope.triangles(simple_g)

    sub_graph = sub_graph.add_column(ret1, {"kcore": "r"})
    sub_graph = sub_graph.add_column(ret2, {"tc": "r"})

    # GLE on ogbn_mag_small graph
    paper_features = []
    for i in range(128):
        paper_features.append("feat_" + str(i))
    paper_features.append("kcore")
    paper_features.append("tc")
    lg = graphscope.graphlearn(
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
        "epoch": 2,
        "node_type": "paper",
        "edge_type": "cites",
    }

    train(config, lg)


def test_modern_graph():
    import vineyard

    def make_nodes_set(nodes):
        return {
            item.get("id", [None])[0]: {k: v for k, v in item.items() if k}
            for item in nodes
        }

    def make_edges_set(edges):
        return {item.get("eid", [None])[0]: item for item in edges}

    def subgraph_roundtrip(num_workers, threads_per_worker):
        logger.info(
            "testing subgraph with %d workers and %d threads per worker",
            num_workers,
            threads_per_worker,
        )

        vquery = "g.V().valueMap()"
        equery = "g.E().valueMap()"
        session = graphscope.session(cluster_type="hosts", num_workers=num_workers)

        g0 = load_modern_graph(session)
        interactive0 = session.gremlin(g0)
        nodes = interactive0.execute(vquery).all()
        edges = interactive0.execute(equery).all()

        logger.info("nodes = %s", nodes)
        logger.info("edges = %s", edges)

        g1 = interactive0.subgraph("g.E()")
        interactive0.close()
        interactive1 = session.gremlin(g1)
        subgraph_nodes = interactive1.execute(vquery).all()
        subgraph_edges = interactive1.execute(equery).all()
        logger.info("subgraph nodes = %s", subgraph_nodes)
        logger.info("subgraph edges = %s", subgraph_edges)
        interactive1.close()

        assert make_nodes_set(nodes) == make_nodes_set(subgraph_nodes)
        assert make_edges_set(edges) == make_edges_set(subgraph_edges)
        session.close()

    num_workers_options = (
        1,
        2,
    )
    threads_per_worker_options = (
        1,
        2,
    )

    for num_workers in num_workers_options:
        for threads_per_worker in threads_per_worker_options:
            with vineyard.envvars(
                {
                    "RUST_LOG": "debug",
                    "THREADS_PER_WORKER": "%d" % (threads_per_worker,),
                }
            ):
                subgraph_roundtrip(num_workers, threads_per_worker)
