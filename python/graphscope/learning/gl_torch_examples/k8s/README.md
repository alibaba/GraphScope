# How to run on kubernetes

### 1. Prepare data and code

We use [Kubeflow](https://github.com/kubeflow/training-operator) to deploy **GLT** jobs on K8s clusters. Make sure Kubeflow is installed before running the following examples.

### 2. Launch a GraphScope cluster on k8s and start training.
`client.yaml` is an example of launching GLTorch jobs with a default setting, 2 servers and 2 workers, using pytorch-operator.
`client.py` is the GLT training script example, and `k8s_launch.py` is the script to launch the GraphScope k8s cluster and the client jobs.

First, please specify the corresponding file paths in the `gs.graphlearn_torch` function in `k8s_launch.py` file:
- `manifest_path`: the path for configuration yaml file, `client.yaml` in this case.
- `client_folder_path`: the folder path for the client scripts, default `./` in this case.

Then, configure the parameters in class `params` of `k8s_launch.py` file:
- NUM_SERVER_NODES: number of server nodes
- NUM_WORKER_NODES: number of worker nodes

Now, we can run the following command to start the training and evaluation process.
```shell
python3 k8s_launch.py
```
You can check the status of the job by running `kubectl logs [pod_name]`.