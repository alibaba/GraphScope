# Copyright 2022 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

from kubernetes import client
from kubernetes import config
from utils import fill_params_in_yaml
from utils import launch_client

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
    master_id=0,
)

params["MASTER_ADDR"] = glt_graph.master_addr
params["NUM_WORKER_REPLICAS"] = params["NUM_CLIENT_NODES"] - 1

# start the client process
config.load_kube_config()
# fill the parameters in the client.yaml
pytorch_job_manifest = fill_params_in_yaml("client.yaml", params)
# create the CustomObjectsApi instance
api_instance = client.CustomObjectsApi()
# launch the client process
launch_client(api_instance, pytorch_job_manifest)

print("Exiting...")
