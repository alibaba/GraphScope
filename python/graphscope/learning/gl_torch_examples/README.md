# The Local GLTorch Server-Client Example

This example demonstrates how to run a local server-client GLTorch job

### Prerequisites
Install ogb and PyG according to the PyTorch version by running the following command :
```shell
pip3 install ogb
pip3 install torch_geometric
pip3 install pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv -f https://data.pyg.org/whl/torch-x.x.x+cpu.html
```

### Train and evaluate
First, launch the GraphScope server on local for sampling service. 
`GLTorchGraph` which contains information of endpoints for sampler servers is dumped as the `.pkl` file to the disk.
```shell
python3 load.py
```
Then, launch the client job by reading the GLTorchGraph from the disk.
The `node_rank` is the rank of the client node, which is used to distinguish different client processes.
```shell
python3 client.py --node_rank=0
```

