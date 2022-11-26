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

import graphscope
from graphscope.analytical.udf.decorators import pregel
from graphscope.dataset import load_modern_graph
from graphscope.dataset import load_ogbn_mag
from graphscope.framework.app import AppAssets

logger = logging.getLogger("graphscope")

graphscope.set_option(show_log=True)
graphscope.set_option(log_level="DEBUG")


@pytest.fixture(scope="module")
def ogbn_small_script():
    script = "g.V().has('author', 'id', 2).out('writes').where(__.in('writes').has('id', 4307)).count()"
    return script


def train_gcn(
    graph,
    node_type,
    edge_type,
    class_num,
    features_num,
    hops_num=2,
    nbrs_num=[25, 10],
    epochs=2,
    train_batch_size=128,
    test_batch_size=128,
    hidden_dim=256,
    in_drop_rate=0.5,
    learning_rate=0.01,
):
    try:
        # https://www.tensorflow.org/guide/migrate
        import tensorflow.compat.v1 as tf

        tf.disable_v2_behavior()
    except ImportError:
        import tensorflow as tf

    import graphscope.learning
    from graphscope.learning.examples import EgoGraphSAGE
    from graphscope.learning.examples import EgoSAGESupervisedDataLoader
    from graphscope.learning.examples.tf.trainer import LocalTrainer

    graphscope.learning.reset_default_tf_graph()

    dimensions = [features_num] + [hidden_dim] * (hops_num - 1) + [class_num]
    model = EgoGraphSAGE(dimensions, act_func=tf.nn.relu, dropout=in_drop_rate)

    # prepare train dataset
    train_data = EgoSAGESupervisedDataLoader(
        graph,
        graphscope.learning.Mask.TRAIN,
        "random",
        train_batch_size,
        node_type=node_type,
        edge_type=edge_type,
        nbrs_num=nbrs_num,
        hops_num=hops_num,
    )
    train_embedding = model.forward(train_data.src_ego)
    train_labels = train_data.src_ego.src.labels
    loss = tf.reduce_mean(
        tf.nn.sparse_softmax_cross_entropy_with_logits(
            labels=train_labels,
            logits=train_embedding,
        )
    )
    optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)

    # prepare test dataset
    test_data = EgoSAGESupervisedDataLoader(
        graph,
        graphscope.learning.Mask.TEST,
        "random",
        test_batch_size,
        node_type=node_type,
        edge_type=edge_type,
        nbrs_num=nbrs_num,
        hops_num=hops_num,
    )
    test_embedding = model.forward(test_data.src_ego)
    test_labels = test_data.src_ego.src.labels
    test_indices = tf.math.argmax(test_embedding, 1, output_type=tf.int32)
    test_acc = tf.div(
        tf.reduce_sum(tf.cast(tf.math.equal(test_indices, test_labels), tf.float32)),
        tf.cast(tf.shape(test_labels)[0], tf.float32),
    )

    # train and test
    trainer = LocalTrainer()
    trainer.train(train_data.iterator, loss, optimizer, epochs=epochs)
    trainer.test(test_data.iterator, test_acc)


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

    # config hyperparameters and train.
    train_gcn(
        lg,
        node_type="paper",
        edge_type="cites",
        class_num=349,  # output dimension
        features_num=128,  # input dimension
    )


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
    import graphscope.nx as nx

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
    train_gcn(
        lg,
        node_type="paper",
        edge_type="cites",
        class_num=349,  # output dimension
        features_num=130,  # input dimension, 128 + kcore + triangle count
    )


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
