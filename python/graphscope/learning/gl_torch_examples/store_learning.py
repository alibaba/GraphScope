import time

import torch
import torch.nn.functional as F
from ogb.nodeproppred import Evaluator
from torch_geometric.data import EdgeAttr
from torch_geometric.nn import GraphSAGE

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
from graphscope.dataset import load_ogbn_arxiv
from graphscope.learning.gs_feature_store import GsTensorAttr

NUM_EPOCHS = 10
BATCH_SIZE = 4
NUM_SERVERS = 1
NUM_NEIGHBORS = [2, 2, 2]

print("Batch size:", BATCH_SIZE)
print("Number of epochs:", NUM_EPOCHS)
print("Number of neighbors:", NUM_NEIGHBORS)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print("Using device:", device)

gs.set_option(log_level="DEBUG")
gs.set_option(show_log=True)

# load the ogbn_arxiv graph.
sess = gs.session(cluster_type="hosts", num_workers=NUM_SERVERS)
g = load_ogbn_arxiv(sess=sess)

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

model = GraphSAGE(
    in_channels=128,
    hidden_channels=256,
    num_layers=3,
    out_channels=47,
).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)


graph_topo = graph_store.get_edge_index(EdgeAttr(("paper", "citation", "paper"), "csr"))
DATA_LENGTH = graph_topo[0].size(0) - 1
TRAIN_DATA_LENGTH = 200
print("Number of nodes:", DATA_LENGTH)


def get_neighbors(node_id, neighbor_num):
    indices_start = graph_topo[0][node_id]
    indices_end = graph_topo[0][node_id + 1]
    neighbors = graph_topo[1][indices_start:indices_end]
    random = torch.randperm(neighbors.size(0))
    neighbors = neighbors[random[:neighbor_num]]
    return neighbors


def get_node_feature(node_id):
    node_feature = feature_store.get_tensor(
        GsTensorAttr(group_name="paper", index=node_id)
    )
    return node_feature


def get_node_label(node_id):
    node_feature = feature_store.get_tensor(
        GsTensorAttr(
            group_name="paper", attr_name="label", index=node_id, is_label=True
        )
    )
    return node_feature


def get_nodes_features(nodes):
    nodes_features = []
    for node_id in nodes:
        node_feature = get_node_feature(node_id)
        nodes_features.append(node_feature)
    node_features = torch.stack(nodes_features)
    return node_features


def get_nodes_labels(nodes):
    nodes_labels = []
    for node_id in nodes:
        node_feature = get_node_label(node_id)
        nodes_labels.append(node_feature)
    nodes_labels = torch.cat(nodes_labels)
    return nodes_labels


def sample_batch(batch, num_neighbors):
    nodes = {element: index for index, element in enumerate(batch)}
    row = []
    col = []
    current_sample_layer = batch
    for num in num_neighbors:
        # print(f"current_sample_layer({num}):{current_sample_layer}")
        next_sample_layer = []
        for node_id in current_sample_layer:
            neighbors = get_neighbors(node_id, num)
            for neighbor in neighbors.tolist():
                if neighbor not in nodes:
                    nodes[neighbor] = len(nodes)
                    next_sample_layer.append(neighbor)
                row.append(nodes[node_id])
                col.append(nodes[neighbor])
        # print(f"next_sample_layer:{next_sample_layer}")
        current_sample_layer = next_sample_layer
    edge_index = torch.tensor([col, row])
    x = get_nodes_features(nodes).to(dtype=torch.float32)
    y = get_nodes_labels(nodes)
    batch_size = len(batch)
    return x, edge_index, y, batch_size


def test(model, shuffle_node_id):
    evaluator = Evaluator(name="ogbn-arxiv")
    model.eval()
    xs = []
    y_true = []
    test_data_length = TRAIN_DATA_LENGTH // 2
    l = DATA_LENGTH - test_data_length - 1
    for i in range(DATA_LENGTH - 1, l, -BATCH_SIZE):
        end = i
        start = max(i - BATCH_SIZE, 0)
        batch = shuffle_node_id[start:end]
        x, edge_index, y, batch_size = sample_batch(batch, NUM_NEIGHBORS)
        out = model(x, edge_index)[:batch_size]
        xs.append(out.cpu())
        y_true.append(y[:batch_size].cpu())

    y_pred = torch.cat(xs, dim=0)
    y_pred = y_pred.argmax(dim=-1, keepdim=True)
    y_true = torch.cat(y_true, dim=0).unsqueeze(-1)
    test_acc = evaluator.eval(
        {
            "y_true": y_true,
            "y_pred": y_pred,
        }
    )["acc"]
    return test_acc


for epoch in range(NUM_EPOCHS):
    shuffle_node_id = torch.randperm(DATA_LENGTH).tolist()
    model.train()
    start = time.time()
    for i in range(0, TRAIN_DATA_LENGTH, BATCH_SIZE):
        optimizer.zero_grad()
        start = i
        end = min(i + BATCH_SIZE, TRAIN_DATA_LENGTH)
        batch = shuffle_node_id[start:end]
        x, edge_index, y, batch_size = sample_batch(batch, NUM_NEIGHBORS)
        out = model(x, edge_index)[:batch_size].log_softmax(dim=-1)
        loss = F.nll_loss(out, y[:batch_size])
        loss.backward()
        optimizer.step()

    end = time.time()
    print(f"-- Epoch: {epoch:03d}, Loss: {loss:.4f}, Epoch Time: {end - start}")
    # Test accuracy.
    if epoch == 0 or epoch > (NUM_EPOCHS // 2):
        test_acc = test(model, shuffle_node_id)
        print(f"-- Test Accuracy: {test_acc:.4f}")

print("-- Shutdowning ...")
glt.distributed.shutdown_client()

print("-- Exited ...")
