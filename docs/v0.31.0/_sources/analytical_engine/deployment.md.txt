# Deployment for GAE

We have already [presented](../deployment/install_on_local.md) you that how to deploy GraphScope on a Local machine, and how to take advantage of 3 engines (GAE, GIE and GLE) to address different and complex problems that related to graphs.

We also have [showed](../deployment/deploy_graphscope_on_self_managed_k8s.md) you that how to deploy GraphScope on Kubernetes cluster.

However, such scenarios are also quite common, where users intend to run graph analysis algorithms on offline graph data every day and export the results to the data warehouse, without any need for real-time graph querying or sampling. In such cases, it is unnecessary for users to deploy both GIE and GLE. Hence, we provide the capability to deploy GAE independently.

In this document, we will walk you through the process of standalone deployment of GAE on a self-managed k8s cluster.

## Prerequisites

- Kubernetes Cluster
- Python >= 3.7

To get started, you need to install graphscope-client and prepare a Kubernetes Cluster to continue.

Incase you doesn't have one, you could refer to the [Install graphscope-client](../deployment/deploy_graphscope_on_self_managed_k8s.md#install-graphscope-client) and [create kubernetes cluster](../deployment/deploy_graphscope_on_self_managed_k8s.md#prepare-a-kubernetes-cluster) to get them.


## Deployment GAE only

We offer two distinct methods for standalone installation of GAE. It is entirely at your discretion to utilize either of the aforementioned methods as per your preference.

### Deployment with Python SDK

The `Session` class has options to control which subset of engines containers to be created upon launching. For example:

```python
import graphscope
graphscope.set_option(show_log=True)

sess = graphscope.session(enabled_engines='gae')
g = sess.g()
```


### Deployment with Helm

You could customize options on the command line parameters passed to the `helm install`. For example:

```shell
# helm install <release-name> graphscope/graphscope
# Replace <release-name> to the name of this release, use `my-gs` for illustration.
helm install my-gs graphscope/graphscope --set enabled_engines="gae"
```

Then use python client to connect to the service. See more details in the [deploy graphscope with helm](../deployment/deploy_graphscope_with_helm.md)

```python
import graphscope

sess = graphscope.session(addr="<ip>:<port>")
g = sess.g()
```

### Execute graph analytical algorithms

You may execute graph analytical algorithms in the same manner as you would with previous settings. For example:

```python
wcc_result = graphscope.wcc(g)
print(wcc_result.to_dataframe({'id': 'v.id', 'group': 'r'}))
```

> **Tip:** If you encounter the error **Failed to project to simple graph as no vertex exists in this graph.**, it means that the graph is empty. You can import the loading method as `from graphscope.dataset import load_modern_graph` , then load the modern graph with current session, e.g. `g = load_modern_graph(sess, "path/to/your/graph")`, the "path/to/your/graph" is the path where the graph data is stored in the pod.

The only difference is the elimination of extraneous resources, resulting in less complexity, fewer inconveniences, fewer resources required, and equivalent functionality.

## Uninstall deployment

When deployed with Python SDK, you need to use `sess.close()` to close the session and free the resources.

When deployed with helm, you would use `helm uninstall my-gs` to delete the release and free the resources.