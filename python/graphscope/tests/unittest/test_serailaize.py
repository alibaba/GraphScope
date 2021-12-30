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
import shutil
import tempfile

import numpy as np
import pytest

import graphscope
from graphscope import Graph

graphscope.set_option(show_log=True)

logger = logging.getLogger("graphscope")


@pytest.fixture
def gs_session_local():
    sess = graphscope.session(
        cluster_type="hosts", num_workers=1, vineyard_shared_mem="4Gi"
    )
    yield sess
    sess.close()


def test_serialize_roundtrip(gs_session_local):
    graph = gs_session_local.g(generate_eid=False)
    p2p_property_dir = os.path.expandvars("${GS_TEST_DIR}/property")
    graph = graph.add_vertices(f"{p2p_property_dir}/p2p-31_property_v_0", "person")
    graph = graph.add_edges(
        f"{p2p_property_dir}/p2p-31_property_e_0",
        label="knows",
        src_label="person",
        dst_label="person",
    )
    logger.debug("finishing loading graphs")

    serialization_path = os.path.join("/", tempfile.gettempprefix(), "serialize")
    shutil.rmtree(serialization_path, ignore_errors=True)

    graph.save_to(serialization_path)
    logger.debug("finishing serializing graph to %s", serialization_path)

    new_graph = Graph.load_from(serialization_path, gs_session_local)
    logger.info("finishing loading new graph from serialization %s", serialization_path)

    pg = new_graph.project(vertices={"person": []}, edges={"knows": ["dist"]})
    ctx = graphscope.sssp(pg, src=6)
    ret = (
        ctx.to_dataframe({"node": "v.id", "r": "r"}, vertex_range={"end": 6})
        .sort_values(by=["node"])
        .to_numpy(dtype=float)
    )
    expect = np.array(
        [[1.0, 260.0], [2.0, 229.0], [3.0, 310.0], [4.0, 256.0], [5.0, 303.0]]
    )
    assert np.all(ret == expect)
