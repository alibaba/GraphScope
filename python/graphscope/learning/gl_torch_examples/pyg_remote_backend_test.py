import multiprocessing as mp
import time

import torch
import torch.nn.functional as F
from ogb.nodeproppred import Evaluator
from torch_geometric.data import TensorAttr
from torch_geometric.nn import GraphSAGE

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
from graphscope.dataset import load_ogbn_arxiv
from graphscope.learning.glt_neighbor_loader import GltNeighborLoader
from graphscope.learning.graphlearn_torch.typing import Split
from graphscope.learning.pyg_neighbor_sampler import PygNeighborSampler

NUM_EPOCHS = 10
LOADER_BATCH_SIZE = 512

print("Batch size:", LOADER_BATCH_SIZE)
print("Number of epochs:", NUM_EPOCHS)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print("Using device:", device)


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


# Get remote backend for PyG:
gs.set_option(show_log=True)

# load the ogbn_arxiv graph as an example.
g = load_ogbn_arxiv()

print("-- Initializing store ...")
feature_store, graph_store = gs.pyg_remote_backend(
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
    edge_dir="out",
    random_node_split={
        "num_val": 0.1,
        "num_test": 0.1,
    },
)

# from torch_geometric.data import EdgeAttr
# edge_index = graph_store._get_edge_index(EdgeAttr(('paper', 'citation', 'paper'), 'csr'))
# print(edge_index)
# graph_store.get_all_edge_attrs()

# from torch_geometric.data import TensorAttr
# feature_tensor = feature_store._get_tensor(TensorAttr('paper', 'feat_0'))
# print(feature_tensor)
# print(feature_tensor.shape)

# tensor_attr_list = feature_store.get_all_tensor_attrs()
# print(tensor_attr_list[0].index.shape)

# tensor_list = feature_store._multi_get_tensor([TensorAttr('paper', 'feat_0'), TensorAttr('paper', 'feat_10')])
# print(tensor_list)

print("-- Initializing client ...")
glt.distributed.init_client(
    num_servers=1,
    num_clients=1,
    client_rank=0,
    master_addr=graph_store.master_addr,
    master_port=graph_store.server_client_master_port,
    num_rpc_threads=4,
    is_dynamic=True,
)

train_sampler = PygNeighborSampler(
    data=(feature_store, graph_store),
    num_neighbors=[15, 10, 5],
    data_split=Split.train,
    shuffle=True,
    collect_features=True,
    to_device=device,
    batch_size=LOADER_BATCH_SIZE,
    num_workers=1,
    worker_devices=[torch.device("cpu")],
    worker_concurrency=1,
    buffer_size="1GB",
    prefetch_size=1,
    workload_type="train",
)

test_sampler = PygNeighborSampler(
    data=(feature_store, graph_store),
    num_neighbors=[15, 10, 5],
    data_split=Split.test,
    shuffle=False,
    collect_features=True,
    to_device=device,
    batch_size=LOADER_BATCH_SIZE,
    num_workers=1,
    worker_devices=[torch.device("cpu")],
    worker_concurrency=1,
    buffer_size="1GB",
    prefetch_size=1,
    workload_type="test",
)

print("-- Initializing loader ...")
train_loader = GltNeighborLoader(
    data=(feature_store, graph_store),
    neighbor_sampler=train_sampler,
    input_nodes="paper",
)

test_loader = GltNeighborLoader(
    data=(feature_store, graph_store),
    neighbor_sampler=test_sampler,
    input_nodes="paper",
)

model = GraphSAGE(
    in_channels=128,
    hidden_channels=256,
    num_layers=3,
    out_channels=47,
).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)


print("-- Start training ...")
dataset_name = "ogbn-arxiv"
for epoch in range(0, NUM_EPOCHS):
    model.train()
    start = time.time()
    for batch in train_loader:
        # print(f"-- Epoch: {epoch:03d}, batch: {batch}, batch size: {batch.batch_size}")
        optimizer.zero_grad()
        batch.x = batch.x.to(torch.float32)
        out = model(batch.x, batch.edge_index)[: batch.batch_size].log_softmax(dim=-1)
        loss = F.nll_loss(out, batch.y[: batch.batch_size])
        loss.backward()
        optimizer.step()

    end = time.time()
    print(f"-- Epoch: {epoch:03d}, Loss: {loss:.4f}, Epoch Time: {end - start}")
    # Test accuracy.
    if epoch == 0 or epoch > (NUM_EPOCHS // 2):
        test_acc = test(model, test_loader, dataset_name)
        print(f"-- Test Accuracy: {test_acc:.4f}")

print("-- Shutdowning ...")
glt.distributed.shutdown_client()

print("-- Exited ...")
