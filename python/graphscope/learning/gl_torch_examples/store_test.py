import torch
from torch_geometric.data import EdgeAttr

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
from graphscope.dataset import load_ogbn_arxiv
from graphscope.learning.gs_feature_store import GsTensorAttr

NUM_EPOCHS = 10
LOADER_BATCH_SIZE = 512
NUM_SERVERS = 2

print("Batch size:", LOADER_BATCH_SIZE)
print("Number of epochs:", NUM_EPOCHS)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print("Using device:", device)

gs.set_option(log_level="DEBUG")
gs.set_option(show_log=True)

# load the ogbn_arxiv graph as an example.
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
    num_servers=2,
    num_clients=1,
    client_rank=0,
    master_addr=graph_store.master_addr,
    master_port=graph_store.server_client_master_port,
    num_rpc_threads=4,
    is_dynamic=True,
)

feature_result = feature_store.get_tensor(GsTensorAttr("paper", "feat_0", 0))
print(feature_result)
feature_result = feature_store.get_tensor(GsTensorAttr("paper", "label", 0, True))
print(feature_result)
feature_result = feature_store.get_tensor_size(GsTensorAttr("paper"))
print(feature_result)
feature_result = feature_store.get_all_tensor_attrs()
print(feature_result)

edge_result = graph_store.get_edge_index(
    EdgeAttr(("paper", "citation", "paper"), "csr")
)
print(edge_result)
edge_result = graph_store.get_all_edge_attrs()
print(edge_result)


print("-- Shutdowning ...")
glt.distributed.shutdown_client()

print("-- Exited ...")
