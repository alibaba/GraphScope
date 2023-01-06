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

import numpy as np

import graphscope
from graphscope import sssp
from graphscope.dataset import load_ldbc
from graphscope.framework.loader import Loader


def p2p_property_graph(num_workers, directed=True):
    data_dir = os.path.expandvars("${GS_TEST_DIR}/property")
    graphscope.set_option(show_log=True)
    graphscope.set_option(log_level="DEBUG")
    sess = graphscope.session(num_workers=num_workers, cluster_type="hosts")
    graph = sess.g(directed=directed)
    graph = graph.add_vertices("{}/p2p-31_property_v_0".format(data_dir), "person")
    graph = graph.add_edges("{}/p2p-31_property_e_0".format(data_dir), "knows")
    return sess, graph


def test_sssp():
    prev_result = None
    for num_workers in (1, 2, 3, 4):
        sess, g = p2p_property_graph(num_workers, True)
        sg = g.project(vertices={"person": ["id"]}, edges={"knows": ["dist"]})

        ctx = sssp(sg, 6)
        curr_result = (
            ctx.to_dataframe({"node": "v.id", "result": "r"})
            .sort_values(by=["node"])
            .to_numpy(dtype=int)
        )
        if prev_result is not None and not np.array_equal(prev_result, curr_result):
            raise RuntimeError(
                "Result is not consistent with different workers, current number of workers: %d",
                num_workers,
            )
        prev_result = curr_result
        sess.close()
