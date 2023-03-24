# Standalone Deployment for GIE

We have demonstrated [how to execute interactive queries](./getting_started.md) easily by installing Graphscope via `pip` on a local machine. However, in real-life applications, graphs are often too large to fit on a single machine. In such cases, Graphscope can be deployed on a cluster, such as a [self-managed k8s cluster](../deploy_graphscope_on_self_managed_k8s.md), for processing large-scale graphs. But you may wonder, "what if I only need the GIE engine and not the whole package that includes GAE and GLE?" This tutorial will walk you through the process of standalone deployment of GIE on a self-managed k8s cluster.

Throughout the tutorial, we assume all machines are running Linux system.
We donot guarantee that it works as smoothly as Linux on the other platform.
For your reference, we've tested the tutorial on Ubuntu 20.04.

## The K8s Cluster
If you do not have a K8s cluster to work on, don't worry. We have three simple ways for you to create one and get started with the deployment:

- Use a K8s cluster from Cloud Providers like [ACK](https://www.aliyun.com/product/kubernetes) from Alibaba Cloud.
- Create a K8s cluster using [kubeadm](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/create-cluster-kubeadm/).
- Create a local K8s cluster using [minikube](https://minikube.sigs.k8s.io/docs/start/):
  ```Bash
  # Install `minikube` on your platform
  # Recommend using `none` driver on a Linux machine to free from loading image to control plane.
  # Check https://minikube.sigs.k8s.io/docs/handbook/pushing/ for details.
  minikube start --driver=none
  ```
- Use a local k8s cluster in [docker desktop](https://docs.docker.com/desktop/kubernetes/).

To learn more about the creation of a k8s cluster, please refer to the [official guide](https://kubernetes.io/zh-cn/docs/tutorials/kubernetes-basics/create-cluster/).


## Deploy Your First GIE Service

The easiest way to deploy GIE standalone is by using Helm, which is a package manager for K8s that simplifies the
deployment and management of applications. To deploy GIE standalone using Helm, you can follow these steps:

1. Install Helm on your local machine if you do not have it by following the
   instructions on the [official Helm website](https://helm.sh/docs/intro/install/).
2. Add the GraphScope Helm repository to your local Helm client:
   ```
   helm repo add graphscope https://graphscope.oss-cn-hongkong.aliyuncs.com/chart
   ```
3. Update the Helm repository:
   ```
   helm repo update
   ```
4. Install the GIE chart:
   ```
   helm install [YOUR_RELEASE_NAME] graphscope/gie-standalone
   ```

   ````{tip}
   This command will deploy the GIE chart with the default configuration.
   using [modern graph](https://tinkerpop.apache.org/docs/3.6.2/tutorials/getting-started/) for demo purpose.
   We will later show how you can customize your own graph data, and other useful options.
   ````
5. Verify that the GIE service is running:
   ```
   kubectl get pods
   ```
   You should see the `[YOUR_RELEASE_NAME]-gie-standalone-frontend-0` and `[YOUR_RELEASE_NAME]-gie-standalone-store-0` pods running.

That's it! You have successfully deployed GIE standalone using Helm. 

The next step is try to run some Gremlin queries. 

6. Get the endpoint of the GIE frontend service:
   ```
   kubectl get svc | grep frontend | grep [YOUR_RELEASE_NAME]
   ```
   You should see the ip address of the GIE frontend service. The port is 8182 by default, but can also be configurable. 
   
7. Connect to the GIE frontend service using the official Python SDKs or Gremlin console.

   The following codes show how you can submit Gremlin queries in GIE.
   ```Python
   import sys
   from gremlin_python import statics
   from gremlin_python.structure.graph import Graph
   from gremlin_python.process.graph_traversal import __
   from gremlin_python.process.strategies import *
   from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

   graph = Graph()
   gremlin_endpoint = # the endpoint you've obtained from step 6.
   remoteConn = DriverRemoteConnection('ws://' + gremlin_endpoint + '/gremlin','g')
   g = graph.traversal().withRemote(remoteConn)

   res = g.V().count().next()
   assert res == 6
   ```

## Using Your Own Data


## Deploy on Actual Cluster
With K8s, it's also convenient to deploy GIE in a cluster with multiple machines:
```
helm install [YOUR_RELEASE_NAME] graphscope/gie-standalone --set executor.replicaCount=3
```

This instruction deploys the GIE chart using 3 executors that process graph partitions in v6d. The number of
replicas can be modified according to your needs, but better be less than the number of CPUs in your cluster. 
When specifying the number of executors, v6d loads data from the specified location and partitions graph data automatically for each executor.
It is recommended to store data in a distributed file system like HDFS for convenience. To configure the HDFS data source in helm,
simply use the `hdfs://` scheme for the data source.


## Other Useful Configurations
Extra configurations can be set as:
```bash
helm install [YOUR_RELEASE_NAME] graphscope/gie-standalone --set [key1]=[value1],[key2]=[value2]
```
We've listed useful configuration keys in the following:

- gremlinPort: the port for accessing the Gremlin service (Default: 8182).
- pegasusWorkerNum: the number of working threads per each executor (Default: 2).
  Obviously, the total number of working threads is: 'executor.replicaCount x pegasusWorkerNum' .
- pegasusTimeout: The maximum dueration in `ms` you allow each query to run (Default: 24,000).

## Uninstall the Chart
```bash
helm uninstall [YOUR_RELEASE_NAME]
```


