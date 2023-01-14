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

import base64
import json
import logging
import sys

import graphscope.learning.graphlearn as gl

logger = logging.getLogger("graphscope")


def decode_arg(arg):
    if isinstance(arg, dict):
        return arg
    return json.loads(
        base64.b64decode(arg.encode("utf-8", errors="ignore")).decode(
            "utf-8", errors="ignore"
        )
    )


def launch_server(handle, config, server_index):
    logger.info("server = %s", handle["server"])
    logger.info("handle = %s", handle)
    logger.info("config = %s", config)
    g = gl.Graph().vineyard(handle, config["nodes"], config["edges"])

    for label, node_attr in config["node_attributes"].items():
        n_ints, n_floats, n_strings = (
            node_attr[1][0],
            node_attr[1][1],
            node_attr[1][2],
        )
        g.node_attributes(label, node_attr[0], n_ints, n_floats, n_strings)
    for label, edge_attr in config["edge_attributes"].items():
        n_ints, n_floats, n_strings = (
            edge_attr[1][0],
            edge_attr[1][1],
            edge_attr[1][2],
        )
        g.edge_attributes(label, edge_attr[0], n_ints, n_floats, n_strings)

    for mask, node_label, nsplit, split_range in config["gen_labels"]:
        g.node_view(node_label, mask, nsplit=nsplit, split_range=split_range)

    # we guess the "worker_count" doesn't matter in the server side.
    g = g.init_vineyard(server_index=server_index, worker_count=0)
    g.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: ./learning.py <handle> <config> <server_index>", file=sys.stderr)
        sys.exit(-1)

    handle = decode_arg(sys.argv[1])
    config = decode_arg(sys.argv[2])
    server_index = int(sys.argv[3])
    launch_server(handle, config, server_index)
