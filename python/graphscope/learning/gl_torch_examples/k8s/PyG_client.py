# Copyright 2022 Alibaba Group Holding Limited. All Rights Reserved.
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
# ==============================================================================

import argparse
import time
from typing import List

import torch
import torch.nn.functional as F
from torch.distributed.algorithms.join import Join
from torch.nn.parallel import DistributedDataParallel
from torch_geometric.nn import GraphSAGE

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
from torch_geometric.loader import GLTNeighborLoader
from graphscope.learning.GSFeatureStore import GSFeatureStore
from graphscope.learning.GSGraphStore import GSGraphStore
from graphscope.learning.graphlearn_torch.typing import Split

gs.set_option(log_level="DEBUG")
gs.set_option(show_log=True)


@torch.no_grad()
def test(model, test_loader, dataset_name):
    model.eval()
    xs = []
    y_true = []
    for i, batch in enumerate(test_loader):
        if i == 0:
            device = batch.x.device
        batch.x = batch.x.to(torch.float32)  # TODO
        x = model.module(batch.x, batch.edge_index)[: batch.batch_size]
        xs.append(x.cpu())
        y_true.append(batch.y[: batch.batch_size].cpu())
        del batch

    xs = [t.to(device) for t in xs]
    y_true = [t.to(device) for t in y_true]
    y_pred = torch.cat(xs, dim=0).argmax(dim=-1, keepdim=True)
    y_true = torch.cat(y_true, dim=0)
    test_acc = sum((y_pred.T == y_true.T)[0]) / len(y_true.T)

    return test_acc.item()


def run_client_proc(
    feature_store: GSFeatureStore,
    graph_store: GSGraphStore,
    group_master: str,
    num_servers: int,
    num_clients: int,
    client_rank: int,
    server_rank_list: List[int],
    dataset_name: str,
    epochs: int,
    batch_size: int,
    training_pg_master_port: int,
):

    print("-- Initializing client ...")
    glt.distributed.init_client(
        num_servers=num_servers,
        num_clients=num_clients,
        client_rank=client_rank,
        master_addr=graph_store.master_addr,
        master_port=graph_store.server_client_master_port,
        num_rpc_threads=4,
        client_group_name="k8s_glt_client",
        is_dynamic=True,
    )

    # Initialize training process group of PyTorch.
    current_ctx = glt.distributed.get_context()

    torch.distributed.init_process_group(
        backend="gloo",
        rank=current_ctx.rank,
        world_size=current_ctx.world_size,
        init_method="tcp://{}:{}".format(group_master, training_pg_master_port),
    )

    device = torch.device("cpu")
    # Create distributed neighbor loader on remote server for training.
    print("-- Initializing PyG sampler ...")
    
    train_sampler = glt.distributed.PygNeighborSampler(
        data=(feature_store, graph_store),
        num_neighbors=[5, 3, 2],
        data_split=Split.train,
        shuffle=True,
        collect_features=True,
        to_device=device,
        batch_size=batch_size,
        server_rank=server_rank_list,
        num_workers=1,
        worker_devices=[torch.device("cpu")],
        worker_concurrency=1,
        buffer_size="256MB",
        prefetch_size=1,
        workload_type="train",
    )

    test_sampler = glt.distributed.PygNeighborSampler(
        data=(feature_store, graph_store),
        num_neighbors=[5, 3, 2],
        data_split=Split.test,
        shuffle=False,
        collect_features=True,
        to_device=device,
        batch_size=batch_size,
        server_rank=server_rank_list,
        num_workers=1,
        worker_devices=[torch.device("cpu")],
        worker_concurrency=1,
        buffer_size="256MB",
        prefetch_size=1,
        workload_type="test",
    )

    print("-- Initializing PyG loader ...")
    train_loader = GLTNeighborLoader(
        data=(feature_store, graph_store),
        neighbor_sampler=train_sampler,
        input_nodes='paper',
    )

    test_loader = GLTNeighborLoader(
        data=(feature_store, graph_store),
        neighbor_sampler=test_sampler,
        input_nodes='paper',
    )

    # Define model and optimizer.
    print("-- Initializing model and optimizer ...")
    model = GraphSAGE(
        in_channels=128,
        hidden_channels=128,
        num_layers=3,
        out_channels=47,
    ).to(device)
    model = DistributedDataParallel(model, device_ids=None)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Train and test.
    print("-- Start training and testing ...")
    epochs = 1
    dataset_name = "ogbn-arxiv"
    for epoch in range(0, epochs):
        model.train()
        start = time.time()
        with Join([model]):
            for batch in train_loader:
                optimizer.zero_grad()
                batch.x = batch.x.to(torch.float32)  # TODO
                out = model(batch.x, batch.edge_index)[: batch.batch_size].log_softmax(
                    dim=-1
                )
                loss = F.nll_loss(out, torch.flatten(batch.y[: batch.batch_size]))
                loss.backward()
                optimizer.step()

        end = time.time()
        print(f"-- Epoch: {epoch:03d}, Loss: {loss:04f} Epoch Time: {end - start}")
        torch.distributed.barrier()
        # Test accuracy.
        if epoch == 0 or epoch > (epochs // 2):
            test_acc = test(model, test_loader, dataset_name)
            print(f"-- Test Accuracy: {test_acc:.4f}")
            torch.distributed.barrier()

    print("-- Shutdowning ...")
    glt.distributed.shutdown_client()

    print("-- Exited ...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Arguments for distributed training of supervised SAGE with servers."
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="ogbn-arxiv",
        help="The name of ogbn arxiv.",
    )
    parser.add_argument(
        "--num_server_nodes",
        type=int,
        default=2,
        help="Number of server nodes for remote sampling.",
    )
    parser.add_argument(
        "--num_client_nodes",
        type=int,
        default=1,
        help="Number of client nodes for training.",
    )
    parser.add_argument(
        "--node_rank",
        type=int,
        default=0,
        help="The node rank of the current role.",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=10,
        help="The number of training epochs. (client option)",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=256,
        help="Batch size for the training and testing dataloader.",
    )
    parser.add_argument(
        "--training_pg_master_port",
        type=int,
        default=9997,
        help="The port used for PyTorch's process group initialization across all training processes.",
    )
    parser.add_argument(
        "--train_loader_master_port",
        type=int,
        default=9998,
        help="The port used for RPC initialization across all sampling workers of training loader.",
    )
    parser.add_argument(
        "--test_loader_master_port",
        type=int,
        default=9999,
        help="The port used for RPC initialization across all sampling workers of testing loader.",
    )
    parser.add_argument(
        "--master_addr",
        type=str,
        default="localhost",
        help="The master address of the graphlearn server.",
    )
    parser.add_argument(
        "--group_master",
        type=str,
        default="localhost",
        help="The master address of the training process group.",
    )
    args = parser.parse_args()

    print(
        f"--- Distributed training example of supervised SAGE with server-client mode. Client {args.node_rank} ---"
    )
    print(f"* dataset: {args.dataset}")
    print(f"* total server nodes: {args.num_server_nodes}")
    print(f"* total client nodes: {args.num_client_nodes}")
    print(f"* node rank: {args.node_rank}")

    num_servers = args.num_server_nodes
    num_clients = args.num_client_nodes

    print(f"* epochs: {args.epochs}")
    print(f"* batch size: {args.batch_size}")
    print(f"* training process group master port: {args.training_pg_master_port}")
    print(f"* training loader master port: {args.train_loader_master_port}")
    print(f"* testing loader master port: {args.test_loader_master_port}")

    client_rank = args.node_rank
    print("--- Loading graph info ...")
    endpoints = [
            args.master_addr + ":9001",
            args.master_addr + ":9002",
            args.master_addr + ":9003",
            args.master_addr + ":9004",
        ]
    feature_store = GSFeatureStore(endpoints=endpoints)
    graph_store = GSGraphStore(endpoints=endpoints)
    print("--- Launching client processes ...")
    run_client_proc(
        feature_store,
        graph_store,
        args.group_master,
        num_servers,
        num_clients,
        client_rank,
        [server_rank for server_rank in range(num_servers)],
        args.dataset,
        args.epochs,
        args.batch_size,
        args.training_pg_master_port,
    )
