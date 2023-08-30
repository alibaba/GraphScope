# python3 local_glt.py --dataset=ogbn-arxiv --epochs=10
import argparse
import os.path as osp
import time
from typing import List

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
import torch
import torch.nn.functional as F
from graphscope.dataset import load_ogbn_arxiv
from ogb.nodeproppred import Evaluator
from torch_geometric.nn import GraphSAGE


@torch.no_grad()
def test(model, test_loader, dataset_name):
    evaluator = Evaluator(name=dataset_name)
    model.eval()
    xs = []
    y_true = []
    for i, batch in enumerate(test_loader):
        if i == 0:
            device = batch.x.device
        batch.x = batch.x.to(torch.float32)  # TODO
        x = model(batch.x, batch.edge_index)[: batch.batch_size]
        xs.append(x.cpu())
        y_true.append(batch.y[: batch.batch_size].cpu())
        del batch

    xs = [t.to(device) for t in xs]
    y_true = [t.to(device) for t in y_true]
    y_pred = torch.cat(xs, dim=0).argmax(dim=-1, keepdim=True)
    y_true = torch.cat(y_true, dim=0).unsqueeze(-1)
    test_acc = evaluator.eval(
        {
            "y_true": y_true,
            "y_pred": y_pred,
        }
    )["acc"]
    return test_acc


def run(
    glt_graph,
    dataset_name: str,
    train_path_list: List[str],
    test_path_list: List[str],
    epochs: int,
    batch_size: int,
    train_loader_master_port: int,
    test_loader_master_port: int,
):
    print(f"-- Initializing client ...")
    glt.distributed.init_client(
        num_servers=1,
        num_clients=1,
        client_rank=0,
        master_addr=glt_graph.master_addr,
        master_port=glt_graph.server_client_master_port,
        num_rpc_threads=4,
    )

    device = torch.device("cpu")

    # Create distributed neighbor loader on remote server for training.
    print(f"-- Creating training dataloader ...")
    train_loader = glt.distributed.DistNeighborLoader(
        data=None,
        num_neighbors=[15, 10, 5],
        input_nodes=train_path_list,
        batch_size=batch_size,
        shuffle=True,
        collect_features=True,
        to_device=device,
        worker_options=glt.distributed.RemoteDistSamplingWorkerOptions(
            server_rank=[0],
            num_workers=1,
            worker_devices=[torch.device("cpu")],
            worker_concurrency=1,
            master_addr=glt_graph.master_addr,
            master_port=train_loader_master_port,
            buffer_size="1GB",
            prefetch_size=1,
            worker_key="train",
        ),
    )

    # Create distributed neighbor loader on remote server for testing.
    print(f"-- Creating testing dataloader ...")
    test_loader = glt.distributed.DistNeighborLoader(
        data=None,
        num_neighbors=[15, 10, 5],
        input_nodes=test_path_list,
        batch_size=batch_size,
        shuffle=False,
        collect_features=True,
        to_device=device,
        worker_options=glt.distributed.RemoteDistSamplingWorkerOptions(
            server_rank=[0],
            num_workers=1,
            worker_devices=[torch.device("cpu")],
            worker_concurrency=1,
            master_addr=glt_graph.master_addr,
            master_port=test_loader_master_port,
            buffer_size="1GB",
            prefetch_size=1,
            worker_key="test",
        ),
    )

    # Define model and optimizer.
    print(f"-- Initializing model and optimizer ...")
    model = GraphSAGE(
        in_channels=128,
        hidden_channels=256,
        num_layers=3,
        out_channels=47,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    # Train and test.
    print(f"-- Start training and testing ...")
    for epoch in range(0, epochs):
        model.train()
        start = time.time()
        for batch in train_loader:
            optimizer.zero_grad()
            batch.x = batch.x.to(torch.float32)  # TODO
            out = model(batch.x, batch.edge_index)[: batch.batch_size].log_softmax(
                dim=-1
            )
            loss = F.nll_loss(out, batch.y[: batch.batch_size])
            loss.backward()
            optimizer.step()

        end = time.time()
        print(f"-- Epoch: {epoch:03d}, Loss: {loss:.4f}, Epoch Time: {end - start}")
        # Test accuracy.
        if epoch == 0 or epoch > (epochs // 2):
            test_acc = test(model, test_loader, dataset_name)
            print(f"-- Test Accuracy: {test_acc:.4f}")

    print(f"-- Shutdowning ...")
    glt.distributed.shutdown_client()

    print(f"-- Exited ...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Arguments for distributed training of supervised SAGE with servers."
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="ogbn-products",
        help="The name of ogbn dataset.",
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
        default=512,
        help="Batch size for the training and testing dataloader.",
    )
    parser.add_argument(
        "--train_loader_master_port",
        type=int,
        default=11112,
        help="The port used for RPC initialization across all sampling workers of training loader.",
    )
    parser.add_argument(
        "--test_loader_master_port",
        type=int,
        default=11113,
        help="The port used for RPC initialization across all sampling workers of testing loader.",
    )
    args = parser.parse_args()

    print(f"* dataset: {args.dataset}")
    print(f"* epochs: {args.epochs}")
    print(f"* batch size: {args.batch_size}")
    print(f"* training loader master port: {args.train_loader_master_port}")
    print(f"* testing loader master port: {args.test_loader_master_port}")

    print("-- Loading training and testing seeds ...")

    train_path = "/root/GraphScope/examples/train_seeds.pt"
    val_path = "/root/GraphScope/examples/val_seeds.pt"
    torch.save(torch.arange(10000), train_path)
    torch.save(torch.arange(10000, 169343), val_path)

    gs.set_option(show_log=True)

    # load the ogbn_arxiv graph as example.
    g = load_ogbn_arxiv()
    glt_graph = gs.graphlearn_torch(
        g,
        edges=[
            ("paper", "citation", "paper"),
        ],
        node_features={
            "paper": [f"feat_{i}" for i in range(128)],
        },
        node_labels={
            "paper": "label",
        },
    )

    run(
        glt_graph,
        args.dataset,
        [train_path],
        [val_path],
        args.epochs,
        args.batch_size,
        args.train_loader_master_port,
        args.test_loader_master_port,
    )
