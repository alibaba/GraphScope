import torch
import torch.nn.functional as F
from ogb.nodeproppred import Evaluator
from torch_geometric.data.feature_store import TensorAttr
from torch_geometric.loader import NeighborLoader
from torch_geometric.nn import GraphSAGE
from tqdm import tqdm

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
from graphscope.dataset import load_ogbn_arxiv

NUM_EPOCHS = 10
BATCH_SIZE = 4096
NUM_SERVERS = 1
NUM_NEIGHBORS = [2, 2, 2]

print("Batch size:", BATCH_SIZE)
print("Number of epochs:", NUM_EPOCHS)
print("Number of neighbors:", NUM_NEIGHBORS)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print("Using device:", device)

gs.set_option(show_log=True)

# load the ogbn_arxiv graph.
sess = gs.session(cluster_type="hosts", num_workers=NUM_SERVERS)
g = load_ogbn_arxiv(sess=sess)

print("-- Initializing store ...")
glt_graph, feature_store, graph_store = gs.graphlearn_torch(
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
    return_pyg_remote_backend=True,
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


print("-- Initializing loader ...")
# get train & test mask
num_nodes = feature_store.get_tensor_size(TensorAttr(group_name="paper"))[0]
print("Node num:", num_nodes)
shuffle_id = torch.randperm(num_nodes)
train_indices = shuffle_id[: int(0.8 * num_nodes)]
test_indices = shuffle_id[int(0.2 * num_nodes) :]
train_mask = torch.zeros(num_nodes, dtype=torch.bool)
test_mask = torch.zeros(num_nodes, dtype=torch.bool)
train_mask[train_indices] = True
test_mask[test_indices] = True

train_loader = NeighborLoader(
    data=(feature_store, graph_store),
    batch_size=BATCH_SIZE,
    num_neighbors=NUM_NEIGHBORS,
    shuffle=False,
    input_nodes=("paper", train_mask),
)

test_loader = NeighborLoader(
    data=(feature_store, graph_store),
    batch_size=BATCH_SIZE,
    num_neighbors=NUM_NEIGHBORS,
    shuffle=False,
    input_nodes=("paper", test_mask),
)

model = GraphSAGE(
    in_channels=128,
    hidden_channels=256,
    num_layers=3,
    out_channels=47,
).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)


@torch.no_grad()
def test(model, test_loader, dataset_name):
    evaluator = Evaluator(name=dataset_name)
    model.eval()
    xs = []
    y_true = []
    for i, batch in enumerate(test_loader):
        if i == 0:
            device = batch["paper"].x.device
        batch["paper"].x = batch["paper"].x.to(torch.float32)  # TODO
        x = model(batch["paper"].x, batch[("paper", "citation", "paper")].edge_index)[
            : batch["paper"].batch_size
        ]
        xs.append(x.cpu())
        y_true.append(batch["paper"].label[: batch["paper"].batch_size].cpu())
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


dataset_name = "ogbn-arxiv"
for epoch in range(NUM_EPOCHS):
    model.train()
    with tqdm(
        total=len(train_loader), desc=f"Epoch {epoch+1}/{NUM_EPOCHS}", unit="batch"
    ) as pbar:
        for batch in train_loader:
            optimizer.zero_grad()
            batch["paper"].x = batch["paper"].x.to(torch.float32)  # TODO
            out = model(
                batch["paper"].x, batch[("paper", "citation", "paper")].edge_index
            )[: batch["paper"].batch_size].log_softmax(dim=-1)
            label = batch["paper"].label[: batch["paper"].batch_size].long()
            loss = F.nll_loss(out, label)
            loss.backward()
            optimizer.step()
            pbar.set_postfix({"Loss": f"{loss:.4f}"})
            pbar.update(1)

    # Test accuracy.
    if epoch % 2 == 0:
        test_acc = test(model, test_loader, dataset_name)
        print(f"-- Test Accuracy: {test_acc:.4f}", flush=True)

print("-- Shutdowning ...")
glt.distributed.shutdown_client()

print("-- Exited ...")
