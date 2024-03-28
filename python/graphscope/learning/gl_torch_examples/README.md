# The Local GLTorch Example

This example demonstrates how to run a local server-client GLTorch job

### Prerequisites
Install ogb and PyG according to the PyTorch version by running the following command :
```shell
pip3 install ogb
pip3 install torch_geometric
pip3 install pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv -f https://data.pyg.org/whl/torch-x.x.x+cpu.html
```

### Train and evaluate
1. For local single machine mode.
```shell
python3 local.py
```
2. For local multi-server/multi-client mode with DDP.
Here num_server_nodes indicates the number of server nodes, and num_client_nodes indicates the number of client nodes.
The server nodes are responsible for the sampling, and the client nodes are responsible for the computation of the model.
```shell
python3 local_sc_ddp.py --num_server_nodes 2 --num_client_nodes 2
```
