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

import importlib
import logging
import os
import random
import string
import sys

import numpy as np
import pytest

import graphscope
from graphscope.config import GSConfig as gs_config
from graphscope.dataset.ldbc import load_ldbc
from graphscope.dataset.modern_graph import load_modern_graph
from graphscope.dataset.ogbn_mag import load_ogbn_mag
from graphscope.framework.graph import Graph
from graphscope.framework.loader import Loader

test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")


def train(config, graph):
    from graphscope.learning.examples import GCN
    from graphscope.learning.graphlearn.python.model.tf.optimizer import (
        get_tf_optimizer,
    )
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

    trainer = LocalTFTrainer(
        model_fn,
        epoch=config["epoch"],
        optimizer=get_tf_optimizer(
            config["learning_algo"], config["learning_rate"], config["weight_decay"]
        ),
    )
    trainer.train_and_evaluate()


@pytest.fixture
def sess():
    graphscope.set_option(show_log=True)
    graphscope.set_option(initializing_interactive_engine=False)
    s = graphscope.session(cluster_type="hosts", num_workers=1)
    yield s
    s.close()


@pytest.fixture
def ogbn_mag_small():
    return "{}/ogbn_mag_small".format(test_repo_dir)


def test_demo(sess, ogbn_mag_small):
    graph = load_ogbn_mag(sess, ogbn_mag_small)
    print(graph.vineyard_id)

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
        "features_num": 128,  # 128 dimension + kcore + triangle count
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


# def test_demo(gs_session, data_dir):
# graph = load_ldbc(gs_session, data_dir)

# Interactive engine
# interactive = gs_session.gremlin(graph)
# sub_graph = interactive.subgraph(  # noqa: F841
# 'g.V().hasLabel("person").outE("knows")'
# )

# # Analytical engine
# # project the projected graph to simple graph.
# simple_g = sub_graph.project_to_simple(v_label="person", e_label="knows")

# pr_result = graphscope.pagerank(simple_g, delta=0.8)
# tc_result = graphscope.triangles(simple_g)

# # add the PageRank and triangle-counting results as new columns to the property graph
# # FIXME: Add column to sub_graph
# sub_graph.add_column(pr_result, {"Ranking": "r"})
# sub_graph.add_column(tc_result, {"TC": "r"})

# GNN engine


# def test_demo_distribute(graphscope_session, ldbc_graph):
# print(graphscope_session.info)
# print(ldbc_graph.vineyard_id)
# print(ldbc_graph.schema_path)

#     # # Interactive engine
# interactive = gs_session_distributed.gremlin(graph)
# sub_graph = interactive.subgraph(  # noqa: F841
# 'g.V().hasLabel("person").outE("knows")'
# )
# person_count = (
# interactive.execute(
# 'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
# )
# .all()
# .result()[0]
# )
# knows_count = (
# interactive.execute('g.V().hasLabel("person").outE("knows").count()')
# .all()
# .result()[0]
# )
# interactive2 = gs_session_distributed.gremlin(sub_graph)
# sub_person_count = interactive2.execute("g.V().count()").all().result()[0]
# sub_knows_count = interactive2.execute("g.E().count()").all().result()[0]
# assert person_count == sub_person_count
# assert knows_count == sub_knows_count

# # Analytical engine
# # project the projected graph to simple graph.
# simple_g = sub_graph.project_to_simple(v_label="person", e_label="knows")

# pr_result = graphscope.pagerank(simple_g, delta=0.8)
# tc_result = graphscope.triangles(simple_g)

# # add the PageRank and triangle-counting results as new columns to the property graph
# # FIXME: Add column to sub_graph
# sub_graph.add_column(pr_result, {"Ranking": "r"})
# sub_graph.add_column(tc_result, {"TC": "r"})

# # test subgraph on modern graph
# mgraph = load_modern_graph(gs_session_distributed, modern_graph_data_dir)

# # Interactive engine
# minteractive = gs_session_distributed.gremlin(mgraph)
# msub_graph = minteractive.subgraph(  # noqa: F841
# 'g.V().hasLabel("person").outE("knows")'
# )
# person_count = (
# minteractive.execute(
# 'g.V().hasLabel("person").outE("knows").bothV().dedup().count()'
# )
# .all()
# .result()[0]
# )
# msub_interactive = gs_session_distributed.gremlin(msub_graph)
# sub_person_count = msub_interactive.execute("g.V().count()").all().result()[0]
# assert person_count == sub_person_count

# # GNN engine


# def test_multiple_session(data_dir):
# namespace = "gs-multi-" + "".join(
# [random.choice(string.ascii_lowercase) for _ in range(6)]
# )

# gs_image, gie_manager_image = get_gs_image_on_ci_env()
# sess = graphscope.session(
# num_workers=1,
# k8s_gs_image=gs_image,
# k8s_gie_graph_manager_image=gie_manager_image,
# k8s_volumes=get_k8s_volumes(),
# )
# info = sess.info
# assert info["status"] == "active"
# assert info["type"] == "k8s"
# assert len(info["engine_hosts"].split(",")) == 1

# sess2 = graphscope.session(
# k8s_namespace=namespace,
# num_workers=2,
# k8s_gs_image=gs_image,
# k8s_gie_graph_manager_image=gie_manager_image,
# k8s_volumes=get_k8s_volumes(),
# )

# info = sess2.info
# assert info["status"] == "active"
# assert info["type"] == "k8s"
# assert len(info["engine_hosts"].split(",")) == 2

# sess2.close()
# sess.close()


# def test_query_modern_graph(gs_session, modern_graph_data_dir):
# graph = load_modern_graph(gs_session, modern_graph_data_dir)
# interactive = gs_session.gremlin(graph)
# queries = [
# "g.V().has('name','marko').count()",
# "g.V().has('person','name','marko').count()",
# "g.V().has('person','name','marko').outE('created').count()",
# "g.V().has('person','name','marko').outE('created').inV().count()",
# "g.V().has('person','name','marko').out('created').count()",
# "g.V().has('person','name','marko').out('created').values('name').count()",
# ]
# for q in queries:
# result = interactive.execute(q).all().result()[0]
# assert result == 1


# def test_traversal_modern_graph(gs_session, modern_graph_data_dir):
# from gremlin_python.process.traversal import Order
# from gremlin_python.process.traversal import P

# gs_image, gie_manager_image = get_gs_image_on_ci_env()
# graph = load_modern_graph(gs_session, modern_graph_data_dir)
# interactive = gs_session.gremlin(graph)
# g = interactive.traversal_source()
# assert g.V().has("name", "marko").count().toList()[0] == 1
# assert g.V().has("person", "name", "marko").count().toList()[0] == 1
# assert g.V().has("person", "name", "marko").outE("created").count().toList()[0] == 1
# assert (
# g.V().has("person", "name", "marko").outE("created").inV().count().toList()[0]
# == 1
# )
# assert g.V().has("person", "name", "marko").out("created").count().toList()[0] == 1
# assert (
# g.V()
# .has("person", "name", "marko")
# .out("created")
# .values("name")
# .count()
# .toList()[0]
# == 1
# )
# assert (
# g.V()
# .hasLabel("person")
# .has("age", P.gt(30))
# .order()
# .by("age", Order.desc)
# .count()
# .toList()[0]
# == 2
# )


# def test_add_vertices_edges(gs_session_distributed, modern_graph_data_dir):
# graph = load_modern_graph(gs_session_distributed, modern_graph_data_dir)
# graph = graph.add_vertices(
# Loader(os.path.join(modern_graph_data_dir, "person.csv"), delimiter="|"),
# "person2",
# ["name", ("age", "int")],
# "id",
# )
# assert "person2" in graph.schema.vertex_labels

# graph = graph.add_edges(
# Loader(
# os.path.join(modern_graph_data_dir, "knows.csv"),
# delimiter="|",
# ),
# "knows2",
# ["weight"],
# src_label="person2",
# dst_label="person2",
# )

# assert "knows2" in graph.schema.edge_labels

# interactive = gs_session_distributed.gremlin(graph)
# g = interactive.traversal_source()
# assert g.V().count().toList()[0] == 10
# assert g.E().count().toList()[0] == 8


# def test_serialize_roundtrip(gs_session_distributed, p2p_property_dir):
# graph = Graph(gs_session_distributed, generate_eid=False)
# graph = graph.add_vertices(f"{p2p_property_dir}/p2p-31_property_v_0", "person")
# graph = graph.add_edges(
# f"{p2p_property_dir}/p2p-31_property_e_0",
# label="knows",
# src_label="person",
# dst_label="person",
# )

# graph.save_to("/tmp/serialize")
# new_graph = Graph.load_from("/tmp/serialize", gs_session_distributed)
# pg = new_graph.project_to_simple(0, 0, 0, 2)
# ctx = graphscope.sssp(pg, src=6)
# ret = (
# ctx.to_dataframe({"node": "v.id", "r": "r"}, vertex_range={"end": 6})
# .sort_values(by=["node"])
# .to_numpy(dtype=float)
# )
# expect = np.array(
# [[1.0, 260.0], [2.0, 229.0], [3.0, 310.0], [4.0, 256.0], [5.0, 303.0]]
# )
# assert np.all(ret == expect)
