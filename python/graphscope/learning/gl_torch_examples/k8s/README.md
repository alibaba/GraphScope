# How to run on kubernetes

### 1. Prepare data and code

We use [Kubeflow](https://github.com/kubeflow/training-operator) to deploy **GL** jobs on K8s clusters. Make sure Kubeflow is installed before running the following examples.

### 2. Launch a GraphScope cluster on k8s for sampling service.
```shell
python3 k8s_load.py
```

### 3. Train and evaluate.
`client.yaml` is an example of launching a GLTorch job with a default setting, 2 parameter-servers and 2 workers, using pytorch-operator. 
And `launch_client.sh` is the script to configure parameters and launch the client job.

First, configure the parameters in the launch_client.sh file:
- NUM_SERVER_NODES: number of server nodes
- NUM_WORKER_NODES: number of worker nodes
- MASTER_ADDR: the master node address provided by the GraphScope cluster, which can be found in the output of the previous step

Then, run the following command to start the training and evaluation process.
```shell
bash launch_client.sh
```
You can check the status of the job by running `kubectl logs [pod_name]`.