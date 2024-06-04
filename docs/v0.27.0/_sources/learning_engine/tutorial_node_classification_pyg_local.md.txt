# Tutorial: Training a Node Classification Model (PyG) on Your Local Machine

This tutorial presents an end-to-end example that illustrates how GraphScope 
trains the GraphSAGE model (implemented in PyG) for a node classification task. 

## Load Graph
```python
import time

import torch
import torch.nn.functional as F
from ogb.nodeproppred import Evaluator
from torch_geometric.nn import GraphSAGE

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
from graphscope.dataset import load_ogbn_arxiv
from graphscope.learning.graphlearn_torch.typing import Split

gs.set_option(show_log=True)

# load the ogbn_arxiv graph as an example.
g = load_ogbn_arxiv()
```
## Define the evaluation function
```python
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
```

## Launch the Learning Engine
```python
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
    edge_dir="out",
    random_node_split={
        "num_val": 0.1,
        "num_test": 0.1,
    },
)

print("-- Initializing client ...")
glt.distributed.init_client(
    num_servers=1,
    num_clients=1,
    client_rank=0,
    master_addr=glt_graph.master_addr,
    master_port=glt_graph.server_client_master_port,
    num_rpc_threads=4,
    is_dynamic=True,
)
```

## Create neighbor loaderfor training, testing and validation
```python
device = torch.device("cpu")
# Create distributed neighbor loader on remote server for training.
print("-- Creating training dataloader ...")
train_loader = glt.distributed.DistNeighborLoader(
    data=None,
    num_neighbors=[15, 10, 5],
    input_nodes=Split.train,
    batch_size=512,
    shuffle=True,
    collect_features=True,
    to_device=device,
    worker_options=glt.distributed.RemoteDistSamplingWorkerOptions(
        num_workers=1,
        worker_devices=[torch.device("cpu")],
        worker_concurrency=1,
        buffer_size="1GB",
        prefetch_size=1,
        glt_graph=glt_graph,
        workload_type="train",
    ),
)

# Create distributed neighbor loader on remote server for testing.
print("-- Creating testing dataloader ...")
test_loader = glt.distributed.DistNeighborLoader(
    data=None,
    num_neighbors=[15, 10, 5],
    input_nodes=Split.test,
    batch_size=512,
    shuffle=False,
    collect_features=True,
    to_device=device,
    worker_options=glt.distributed.RemoteDistSamplingWorkerOptions(
        num_workers=1,
        worker_devices=[torch.device("cpu")],
        worker_concurrency=1,
        buffer_size="1GB",
        prefetch_size=1,
        glt_graph=glt_graph,
        workload_type="test",
    ),
)
```

## Define the PyG GraphSage Model and optimizer
```python
print("-- Initializing model and optimizer ...")
model = GraphSAGE(
    in_channels=128,
    hidden_channels=256,
    num_layers=3,
    out_channels=47,
).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
```

## Train and test
```python
print("-- Start training and testing ...")
epochs = 10
dataset_name = "ogbn-arxiv"
for epoch in range(0, epochs):
    model.train()
    start = time.time()
    for batch in train_loader:
        optimizer.zero_grad()
        batch.x = batch.x.to(torch.float32)  # TODO
        out = model(batch.x, batch.edge_index)[: batch.batch_size].log_softmax(dim=-1)
        loss = F.nll_loss(out, batch.y[: batch.batch_size])
        loss.backward()
        optimizer.step()

    end = time.time()
    print(f"-- Epoch: {epoch:03d}, Loss: {loss:.4f}, Epoch Time: {end - start}")
    # Test accuracy.
    if epoch == 0 or epoch > (epochs // 2):
        test_acc = test(model, test_loader, dataset_name)
        print(f"-- Test Accuracy: {test_acc:.4f}")

print("-- Shutting down ...")
glt.distributed.shutdown_client()
```