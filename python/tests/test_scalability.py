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
from graphscope.dataset.ldbc import load_ldbc
from graphscope.framework.loader import Loader


def p2p_property_graph(num_workers, directed=True):
    data_dir = os.path.expandvars("${GS_TEST_DIR}/property")
    graphscope.set_option(show_log=True)
    sess = graphscope.session(num_workers=num_workers, run_on_local=True)

    g = sess.load_from(
        edges={
            "knows": (
                Loader("{}/p2p-31_property_e_0".format(data_dir), header_row=True),
                ["src_label_id", "dst_label_id", "dist"],
                ("src_id", "person"),
                ("dst_id", "person"),
            ),
        },
        vertices={
            "person": Loader(
                "{}/p2p-31_property_v_0".format(data_dir), header_row=True
            ),
        },
        directed=directed,
    )
    return sess, g


def test_sssp():
    prev_result = None
    for num_workers in (1, 2, 3, 4):
        sess, g = p2p_property_graph(num_workers, True)
        sg = g.project_to_simple(0, 0, 0, 2)

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
