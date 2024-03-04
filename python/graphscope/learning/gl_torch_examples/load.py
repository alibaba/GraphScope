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

import time
import pickle
import graphscope as gs
from graphscope.dataset import load_ogbn_arxiv


gs.set_option(log_level='DEBUG')
gs.set_option(show_log=True)

# load the ogbn_arxiv graph as an example.
sess = gs.session(cluster_type="hosts", num_workers=3)
g = load_ogbn_arxiv(sess=sess)

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
)
print(glt_graph)
with open('glt_graph.pkl', 'wb') as f:
    pickle.dump(glt_graph, f)
time.sleep(1800)