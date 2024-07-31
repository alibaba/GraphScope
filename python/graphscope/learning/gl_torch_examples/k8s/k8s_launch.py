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
    enabled_engines="gae,glt",
    k8s_service_type="LoadBalancer",
    k8s_namespace="default",  # to guarantee auth to launch the pytorchjobs
    k8s_vineyard_mem="8Gi",
    k8s_engine_mem="8Gi",
    vineyard_shared_mem="8Gi",
    k8s_image_pull_policy="IfNotPresent",
    k8s_image_tag="0.28.0a20240526",
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
    num_clients=params["NUM_CLIENT_NODES"],
    manifest_path="./client.yaml",
    client_folder_path="./",
)

print("Exiting...")
