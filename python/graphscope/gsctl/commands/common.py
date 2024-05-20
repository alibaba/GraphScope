#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited. All Rights Reserved.
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

"""Group of commands used by all products under the FLEX architecture"""

import click

from graphscope.gsctl.config import Context
from graphscope.gsctl.config import get_current_context
from graphscope.gsctl.config import load_gs_config
from graphscope.gsctl.impl import connect_coordinator
from graphscope.gsctl.impl import disconnect_coordinator
from graphscope.gsctl.utils import err
from graphscope.gsctl.utils import info
from graphscope.gsctl.utils import succ


@click.group()
def cli():
    # nothing happens
    pass


@cli.command()
@click.option(
    "-c",
    "--coordinator-endpoint",
    help="Coordinator endpoint, e.g. http://127.0.0.1:9527",
)
def connect(coordinator_endpoint):
    """Connect to a launched coordinator

    By default, it will read context from  ~/.gsctl. If '--coordinator-endpoint'
    is specified, use it as the current context and override the configuration file.
    """
    if coordinator_endpoint is None:
        context = get_current_context()
        if context is None:
            err(
                "No available context found, try to connect by `gsctl connect --coordinator-endpoint <addr>`."
            )
            return
        coordinator_endpoint = context.coordinator_endpoint
    # connect
    try:
        resp = connect_coordinator(coordinator_endpoint)
    except Exception as e:
        err(f"Unable to connect to server: {str(e)}")
    else:
        succ(
            f"Connected to {coordinator_endpoint}, coordinator is serving with {resp.solution} mode.\n"
        )
        info("Try 'gsctl --help' for help.")


@cli.command()
def close():
    """Disconnect from coordinator"""
    try:
        context = disconnect_coordinator()
    except Exception as e:
        err(f"Disconnect from coordinator failed: {str(e)}")
    else:
        if context is not None:
            info(f"Disconnecting from the context: {context.to_dict()}")
            succ("Coordinator service disconnected.\n")
            info("Try 'gsctl --help' for help.")


disclaimer = """
Disclaimer: The `estimate` command would do a rough estimation of the total memory usage of the GAE.
The actual memory usage may vary vastly due to the complexity of the graph algorithm and the data distribution.

The estimation is based on the following formulas:
let #V = number of vertices, #E = number of edges,
assuming load_factor of hashmap is 0.41, sizeof(inner_id) = sizeof(edge_id) = sizeof(vertex_id) = 8 bytes,
where vertex_id is the original representation of vertex, it could be string though, in this case, the memory usage will grow.
Graph:
    1. vertex
        - vertex map: #V * (sizeof(vertex_id) + sizeof(inner_id)) * (1 / load_factor)
        - vertex table: size of vertex files in uncompressed CSV format, unit is GB
    2. edge
        - CSR + CSC: 2 * #E * (sizeof(inner_id) + sizeof(edge_id))
        - offset array: #V * sizeof(size_t)
        - edge table: size of vertex files in uncompressed CSV format, unit is GB
    3. additional data structure when graph is partitioned:
        - outer vertex ID to local ID map:
        - local ID to outer vertex ID array:

This is the minimum usage of the GAE with 1 partition, the actual memory usage would be larger than this estimation.
User should enlarge it by a multiple it by some factor (e.g. 2) to simulate the memory usage of the graphscope engine.
"""


def estimate_gae_memory_usage(v_num, e_num, v_file_size, e_file_size):
    gb = 1024 * 1024 * 1024
    # vertex map
    vertex_map = v_num * (8 + 8) * (1 / 0.41) / gb
    # vertex table
    vertex_table = v_file_size
    # edge
    edge = 2 * e_num * (8 + 8) / gb
    # offset array
    offset_array = v_num * 8 / gb
    # edge table
    edge_table = e_file_size
    return vertex_map + vertex_table + edge + offset_array + edge_table


@cli.command()
@click.option(
    "-s",
    "--storage",
    type=click.Choice(["gae"], case_sensitive=False),
    help="Storage type",
    required=True,
)
@click.option("-v", "--v-num", type=int, help="Number of vertices")
@click.option("-e", "--e-num", type=int, help="Number of edges")
@click.option("-vf", "--v-file-size", type=float, help="Size of vertex files in GB")
@click.option("-ef", "--e-file-size", type=float, help="Size of edge files in GB")
def estimate(storage, v_num, e_num, v_file_size, e_file_size):
    """Estimate the memory usage of the graphscope engine"""
    if storage == "gae":
        if v_num is None or e_num is None or v_file_size is None or e_file_size is None:
            err("Please provide the required parameters.")
            return
        memory_usage = estimate_gae_memory_usage(v_num, e_num, v_file_size, e_file_size)
        info(disclaimer)
        print(f"The estimated memory usage is {memory_usage:.2f} GB.\n")
    else:
        err(f"Estimate usage of storage {storage} is not supported yet.")


if __name__ == "__main__":
    cli()
