from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

import graphscope as gs
from graphscope.dataset import load_ogbn_arxiv
from utils import fill_params_in_yaml, launch_client


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
