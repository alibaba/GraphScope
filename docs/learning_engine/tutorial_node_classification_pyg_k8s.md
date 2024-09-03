# Tutorial: Training a Node Classification Model (PyG) on a K8S Cluster

This tutorial presents a server-client example that illustrates how GraphScope trains the GraphSAGE model (implemented in PyG) for a node classification task on a Kubernetes cluster.

## Set parameters & load graph

```python
import graphscope as gs
from graphscope.dataset import load_ogbn_arxiv

gs.set_option(log_level="DEBUG")
gs.set_option(show_log=True)

params = {
    "NUM_SERVER_NODES": 2,
    "NUM_CLIENT_NODES": 2,
}

# load the ogbn_arxiv graph as an example.
sess = gs.session(
    with_dataset=True,
    k8s_service_type="NodePort",
    k8s_vineyard_mem="8Gi",
    k8s_engine_mem="8Gi",
    vineyard_shared_mem="8Gi",
    k8s_image_pull_policy="IfNotPresent",
    k8s_image_tag="0.26.0a20240115-x86_64",
    num_workers=params["NUM_SERVER_NODES"],
)
g = load_ogbn_arxiv(sess=sess, prefix="/dataset/ogbn_arxiv")
```

## Launch the Server Engine
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
    num_clients=params["NUM_CLIENT_NODES"],
    # Specify the client yaml with the client pods' configuration.
    manifest_path="./client.yaml",
    # Specify the client folder path that contains the client scripts.
    client_folder_path="./",
)

print("Exiting...")
```

## Configure the parameters for client pods
```yaml
apiVersion: "kubeflow.org/v1"
kind: PyTorchJob
metadata:
  name: graphlearn-torch-client
  namespace: default
spec:
  pytorchReplicaSpecs:
    Master:
      replicas: 1
      restartPolicy: OnFailure
      template:
        spec:
          containers:
            - name: pytorch
              image: registry.cn-hongkong.aliyuncs.com/graphscope/graphlearn-torch-client:0.26.0a20240115-x86_64
              imagePullPolicy: IfNotPresent
              command:
                - bash
                - -c
                - |- 
                  python3 /workspace/client.py --node_rank 0 --master_addr ${MASTER_ADDR} --num_server_nodes ${NUM_SERVER_NODES} --num_client_nodes ${NUM_CLIENT_NODES}
              volumeMounts:
              - mountPath: /dev/shm
                name: cache-volume
              - mountPath: /workspace
                name: client-volume
          volumes:
            - name: cache-volume
              emptyDir:
                medium: Memory
                sizeLimit: "8G"
            - name: client-volume
              configMap:
                name: graphlearn-torch-client-config
    Worker:
      replicas: ${NUM_WORKER_REPLICAS}
      restartPolicy: OnFailure
      template:
        spec:
          containers:
            - name: pytorch
              image: registry.cn-hongkong.aliyuncs.com/graphscope/graphlearn-torch-client:0.26.0a20240115-x86_64
              imagePullPolicy: IfNotPresent
              command:
                - bash
                - -c
                - |-
                  python3 /workspace/client.py --node_rank $((${MY_POD_NAME: -1}+1)) --master_addr ${MASTER_ADDR} --group_master ${GROUP_MASTER} --num_server_nodes ${NUM_SERVER_NODES} --num_client_nodes ${NUM_CLIENT_NODES}
              env:
                - name: GROUP_MASTER
                  value: graphlearn-torch-client-master-0
                - name: MY_POD_NAME
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.name
              volumeMounts:
              - mountPath: /dev/shm
                name: cache-volume
              - mountPath: /workspace
                name: client-volume
          volumes:
            - name: cache-volume
              emptyDir:
                medium: Memory
                sizeLimit: "8G"
            - name: client-volume
              configMap:
                name: graphlearn-torch-client-config
```

## Write training and testing script

### Import packages
```python

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
from graphscope.learning.gl_torch_graph import GLTorchGraph
from graphscope.learning.graphlearn_torch.typing import Split

gs.set_option(log_level="DEBUG")
gs.set_option(show_log=True)
```

### Define test function
```python
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
```
### Define the loader and training process
```python
def run_client_proc(
    glt_graph,
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
        master_addr=glt_graph.master_addr,
        master_port=glt_graph.server_client_master_port,
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
    print("-- Creating training dataloader ...")
    train_loader = glt.distributed.DistNeighborLoader(
        data=None,
        num_neighbors=[5, 3, 2],
        input_nodes=Split.train,
        batch_size=batch_size,
        shuffle=True,
        collect_features=True,
        to_device=device,
        worker_options=glt.distributed.RemoteDistSamplingWorkerOptions(
            server_rank=server_rank_list,
            num_workers=1,
            worker_devices=[torch.device("cpu")],
            worker_concurrency=1,
            buffer_size="256MB",
            prefetch_size=1,
            glt_graph=glt_graph,
            workload_type="train",
        ),
    )

    # Create distributed neighbor loader on remote server for testing.
    print("-- Creating testing dataloader ...")
    test_loader = glt.distributed.DistNeighborLoader(
        data=None,
        num_neighbors=[5, 3, 2],
        input_nodes=Split.test,
        batch_size=batch_size,
        shuffle=False,
        collect_features=True,
        to_device=device,
        worker_options=glt.distributed.RemoteDistSamplingWorkerOptions(
            server_rank=server_rank_list,
            num_workers=1,
            worker_devices=[torch.device("cpu")],
            worker_concurrency=1,
            buffer_size="256MB",
            prefetch_size=1,
            glt_graph=glt_graph,
            workload_type="test",
        ),
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
    epochs = 10
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
```
### main function
```python
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
    glt_graph = GLTorchGraph(
        [
            args.master_addr + ":9001",
            args.master_addr + ":9002",
            args.master_addr + ":9003",
            args.master_addr + ":9004",
        ]
    )
    print("--- Launching client processes ...")
    run_client_proc(
        glt_graph,
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
```

## Run the script
```shell
python3 k8s_launch.py
```