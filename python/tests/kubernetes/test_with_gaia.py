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

import pytest

import graphscope
from graphscope.config import GSConfig as gs_config
from graphscope.dataset.modern_graph import load_modern_graph

graphscope.set_option(show_log=True)
logger = logging.getLogger("graphscope")


def get_k8s_volumes():
    k8s_volumes = {
        "data": {
            "type": "hostPath",
            "field": {"path": os.environ["GS_TEST_DIR"], "type": "Directory"},
            "mounts": {"mountPath": "/testingdata"},
        }
    }
    return k8s_volumes


def get_gs_image_on_ci_env():
    if "GS_IMAGE" in os.environ:
        return os.environ["GS_IMAGE"]
    else:
        return gs_config.k8s_gs_image


@pytest.fixture
def gs_session():
    gs_image = get_gs_image_on_ci_env()
    sess = graphscope.session(
        num_workers=2,
        enable_gaia=True,
        k8s_gs_image=gs_image,
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        k8s_etcd_cpu=2,
        k8s_etcd_mem="256Mi",
        k8s_etcd_num_pods=3,
        vineyard_shared_mem="4Gi",
        k8s_volumes=get_k8s_volumes(),
    )
    yield sess
    sess.close()


@pytest.fixture
def modern_graph_data_dir():
    return "/testingdata/modern_graph"


def test_query_modern_graph(gs_session, modern_graph_data_dir, modern_scripts):
    graph = load_modern_graph(gs_session, modern_graph_data_dir)
    interactive = gs_session.gremlin(graph)
    for q in modern_scripts:
        result = interactive.gaia().execute(q).all()[0]
        assert result == 1


def test_traversal_modern_graph(gs_session, modern_graph_data_dir, modern_bytecode):
    graph = load_modern_graph(gs_session, modern_graph_data_dir)
    interactive = gs_session.gremlin(graph)
    g = interactive.traversal_source()
    modern_bytecode(g.gaia())
