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
import logging
import os.path as osp
import pickle
import sys

import graphscope.learning.graphlearn_torch as glt
import torch

logger = logging.getLogger("graphscope")


def decode_arg(arg):
    if isinstance(arg, dict):
        return arg
    return pickle.loads(base64.b64decode(arg))


def run_server_proc(proc_rank, handle, config, server_rank, dataset):
    glt.distributed.init_server(
        num_servers=handle["num_servers"],
        num_clients=handle["num_clients"],
        server_rank=server_rank,
        dataset=dataset,
        master_addr=handle["master_addr"],
        master_port=handle["server_client_master_port"],
        num_rpc_threads=16,
        # server_group_name="dist_train_supervised_sage_server",
    )
    logger.info(f"-- [Server {server_rank}] Waiting for exit ...")
    glt.distributed.wait_and_shutdown_server()
    logger.info(f"-- [Server {server_rank}] Exited ...")


def launch_graphlearn_torch_server(handle, config, server_rank):
    logger.info(f"-- [Server {server_rank}] Initializing server ...")

    dataset = glt.distributed.DistDataset()
    dataset.load_vineyard(
        vineyard_id=str(handle["vineyard_id"]),
        vineyard_socket=handle["vineyard_socket"],
        **config,
    )
    logger.info(f"-- [Server {server_rank}] Initializing server ...")

    torch.multiprocessing.spawn(
        fn=run_server_proc, args=(handle, config, server_rank, dataset), nprocs=1
    )


if __name__ == "__main__":
    if len(sys.argv) < 3:
        logger.info(
            "Usage: ./launch_graphlearn_torch.py <handle> <config> <server_index>",
            file=sys.stderr,
        )
        sys.exit(-1)

    handle = decode_arg(sys.argv[1])
    config = decode_arg(sys.argv[2])
    server_index = int(sys.argv[3])

    logger.info(
        f"launch_graphlearn_torch_server handle: {handle} config: {config} server_index: {server_index}"
    )
    launch_graphlearn_torch_server(handle, config, server_index)
