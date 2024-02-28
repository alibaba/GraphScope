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
import os.path as osp
import sys

import graphscope
import graphscope.learning.graphlearn_torch as glt
import torch
from graphscope.learning.gl_torch_graph import GLTorchGraph

graphscope.set_option(show_log=True)
graphscope.set_option(log_level='DEBUG')

logger = logging.getLogger("graphscope")


def decode_arg(arg):
    if isinstance(arg, dict):
        return arg
    return json.loads(
        base64.b64decode(arg.encode("utf-8", errors="ignore")).decode(
            "utf-8", errors="ignore"
        )
    )


def run_server_proc(proc_rank, handle, config, server_rank, dataset):
    glt.distributed.init_server(
        num_servers=handle["num_servers"],
        server_rank=server_rank,
        dataset=dataset,
        master_addr=handle["master_addr"],
        master_port=handle["server_client_master_port"],
        num_rpc_threads=16,
        is_dynamic=True,
    )
    glt.distributed.wait_and_shutdown_server()


def launch_graphlearn_torch_server(handle, config, server_rank):
    logger.info(f"-- [Server {server_rank}] Initializing server ...")
    edge_dir = config.pop("edge_dir")

    random_node_split = config.pop("random_node_split")
    logger.info(f'random_node_split: {random_node_split}')
    dataset = glt.distributed.DistDataset(
        edge_dir=edge_dir,
        num_partitions=handle["num_servers"],
        partition_idx=server_rank,
        node_pb=glt.data.VineyardPartitionBook(
            str(handle["vineyard_socket"]),
            str(handle["fragments"][server_rank]),
            "paper",
            # {str(handle["fragments"][0]): 0, str(handle["fragments"][1]): 1}
        )
    )
    dataset.load_vineyard(
        # vineyard_id=str(handle["vineyard_id"]),
        vineyard_id=str(handle["fragments"][server_rank]),
        vineyard_socket=handle["vineyard_socket"],
        id2idx=glt.data.VineyardGid2Lid(
            sock=str(handle["vineyard_socket"]),
            fid=str(handle["fragments"][server_rank]),
            v_label_name="paper",
        ),
        **config,
    )
    if random_node_split is not None:
        dataset.random_node_split(**random_node_split, id_filter=glt.data.v6d_id_filter)
    logger.info(f"-- [Server {server_rank}] Running server ...")

    torch.multiprocessing.spawn(
        fn=run_server_proc, args=(handle, config, server_rank, dataset), nprocs=1
    )
    logger.info(f"-- [Server {server_rank}] Server exited.")
    

if __name__ == "__main__":
    if len(sys.argv) < 3:
        logger.info(
            "Usage: ./launch_graphlearn_torch.py <handle> <config> <server_index>",
        )
        sys.exit(-1)

    handle = decode_arg(sys.argv[1])
    config = decode_arg(sys.argv[2])
    server_index = int(sys.argv[3])
    config = GLTorchGraph.reverse_transform_config(config)

    launch_graphlearn_torch_server(handle, config, server_index)
