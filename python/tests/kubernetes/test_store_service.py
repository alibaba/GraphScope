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

import numpy as np
import pytest

import graphscope

logger = logging.getLogger("graphscope")


@pytest.fixture
def gs_conn():
    node_ip = os.environ["NODE_IP"]
    grpc_port = os.environ["GRPC_PORT"]
    gremlin_port = os.environ["GREMLIN_PORT"]
    grpc_endpoint = f"{node_ip}:{grpc_port}"
    gremlin_endpoint = f"{node_ip}:{gremlin_port}"
    yield graphscope.conn(grpc_endpoint, gremlin_endpoint)


def test_demo(gs_conn):
    graph = gs_conn.g()
    schema = graph.schema()
    schema.add_vertex_label("person").add_primary_key("id", "long").add_property(
        "name", "str"
    )
    schema.add_edge_label("knows").source("person").destination("person").add_property(
        "date", "str"
    )
    schema.update()
    load_script = os.environ["LOAD_DATA_SCRIPT"]
    os.system(load_script)

    interactive = gs_conn.gremlin()
    # TODO(zsy, tianli): Use more compilcated queries
    assert interactive.V().count().toList()[0] == 903
    assert interactive.E().count().toList()[0] == 6626
