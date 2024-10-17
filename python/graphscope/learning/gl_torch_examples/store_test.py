import random

import torch
from torch_geometric.data import EdgeAttr

import graphscope as gs
import graphscope.learning.graphlearn_torch as glt
from graphscope.dataset import load_ogbn_arxiv
from graphscope.learning.glt_neighbor_loader import GltNeighborLoader
from graphscope.learning.graphlearn_torch.typing import Split
from graphscope.learning.gs_feature_store import GsTensorAttr
from graphscope.learning.pyg_neighbor_sampler import PygNeighborSampler

NUM_EPOCHS = 10
LOADER_BATCH_SIZE = 1024
NUM_SERVERS = 1

print("Batch size:", LOADER_BATCH_SIZE)
print("Number of epochs:", NUM_EPOCHS)

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


sampler = PygNeighborSampler(
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

loader = GltNeighborLoader(
    data=(feature_store, graph_store),
    neighbor_sampler=sampler,
    input_nodes="paper",
)

# test functions in feature_store.
feature_result = feature_store.get_tensor(GsTensorAttr("paper", "feat_0", 20))
print(feature_result)
feature_result = feature_store.get_tensor(GsTensorAttr("paper", "label", 10, True))
print(feature_result)
feature_result = feature_store.get_tensor_size(GsTensorAttr("paper"))
print(feature_result)
feature_result = feature_store.get_all_tensor_attrs()
print(feature_result)


# test functions in graph_store.
edge_result = graph_store.get_edge_index(
    EdgeAttr(("paper", "citation", "paper"), "csr")
)
print(edge_result)
edge_result = graph_store.get_all_edge_attrs()
print(edge_result)


# check whether the data obtained by feature_store and graph_store is correct.
for batch in loader:
    print(batch)
    print("batch.num_sampled_nodes:", batch.num_sampled_nodes)
    print("batch.num_sampled_edges:", batch.num_sampled_edges)
    random_choose = random.randint(0, batch.batch_size - 1)
    node_id = batch.batch[random_choose].item()
    print("node_id:", node_id)
    feature_result = feature_store.get_tensor(
        GsTensorAttr(group_name="paper", index=node_id)
    )
    print(f"node feature from loader:{batch.x[random_choose]}")
    print(f"node feature from feature_store:{feature_result}")
    is_equal = torch.equal(batch.x[random_choose], feature_result)
    print(f"feature_store check:{is_equal}")

    edge_result = graph_store.get_edge_index(
        EdgeAttr(("paper", "citation", "paper"), "csr")
    )

    indices_start = edge_result[0][node_id]
    indices_end = edge_result[0][node_id + 1]
    if indices_start == indices_end:
        print("No neighbors\n")
        continue
    neighbors1 = edge_result[1][indices_start:indices_end]
    print(f"neighbors of node {node_id} from graph_store:{neighbors1}")

    col_start = (
        (batch.edge_index[1] == random_choose).nonzero(as_tuple=False)[0][0].item()
    )
    col_end = (
        (batch.edge_index[1] == random_choose).nonzero(as_tuple=False)[-1][0].item()
    )
    col = batch.edge_index[0][col_start : col_end + 1]
    neighbors2 = batch.node[col]
    print(f"sample neighbors of node {node_id} from loader:{neighbors2}")
    is_exist = torch.isin(neighbors2, neighbors1).all().item()
    print(f"graph_store check:{is_exist}")

    print("\n")

print("-- Shutdowning ...")
glt.distributed.shutdown_client()

print("-- Exited ...")
