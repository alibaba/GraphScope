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

import numpy as np
import pytest

import graphscope
from graphscope.config import GSConfig as gs_config
from graphscope.dataset.ldbc import load_ldbc
from graphscope.dataset.modern_graph import load_modern_graph
from graphscope.framework.graph import Graph
from graphscope.framework.loader import Loader

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
    if "GS_IMAGE" in os.environ and "GIE_MANAGER_IMAGE" in os.environ:
        return os.environ["GS_IMAGE"], os.environ["GIE_MANAGER_IMAGE"]
    else:
        return gs_config.k8s_gs_image, gs_config.k8s_gie_graph_manager_image


@pytest.fixture
def gs_conn():
    node_ip = os.environ["NODE_IP"]
    grpc_port = os.environ["GRPC_PORT"]
    gremlin_port = os.environ["GREMLIN_PORT"]
    grpc_endpoint = f"{node_ip}:{grpc_port}"
    gremlin_endpoint = f"{node_ip}:{gremlin_port}"
    yield graphscope.conn(grpc_endpoint, gremlin_endpoint)


@pytest.fixture
def data_dir():
    return "/testingdata/ldbc_sample"


def test_demo(gs_conn, data_dir):
    graph = gs_conn.g()
    schema = graph.schema()
    schema.add_vertex_label("person").add_primary_key("id", "int").add_property(
        "name", "str"
    )
    schema.add_edge_label("knows").source("person").destination("person").add_property(
        "date", "int"
    )

    interactive = gs_conn.gremlin()
    # TODO: Adapt the queries to ldbc.
    assert interactive.V().count().toList()[0] == 0
    assert interactive.E().count().toList()[0] == 0
