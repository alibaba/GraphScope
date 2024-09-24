import torch
from torch_geometric.data import TensorAttr

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
from graphscope.dataset import load_ogbn_arxiv

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

result = feature_store.get_tensor(TensorAttr("paper", 'feat_0', 0))
print(result)
result = feature_store.get_tensor_size(TensorAttr("paper"))
print(result)
result = feature_store._get_partition_id(TensorAttr("paper", 'feat_0', 0))
print(result)
result = feature_store.get_all_tensor_attrs()
print(result)

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


print("-- Shutdowning ...")
glt.distributed.shutdown_client()

print("-- Exited ...")