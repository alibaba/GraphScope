# Deploy GraphScope with `helm`

This guide provides instructions for deploying GraphScope using [helm](https://helm.sh/), a popular package manager for Kubernetes. 

This method is ideal for developers who are already familiar with helm and want to automate the deployment process, as well as for those who need better support for versioning and managing the deployment of GraphScope. 

## Prerequisites

Before deploying GraphScope with helm, you will need the following:

- A Kubernetes cluster
- Helm

If you do not have a Kubernetes cluster, you can follow the instructions in [deploy_graphscope_on_self_managed_k8s](./deploy_graphscope_on_self_managed_k8s.md) to create one.

If you do not have helm installed, you can install it by following the instructions in [Installing Helm](https://helm.sh/docs/intro/install/).

## Installation

We have published a [chart](https://artifacthub.io/packages/helm/graphscope/graphscope) to the artifact hub, which you can easily obtain by running the following commands:

```shell
helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/charts/
helm repo update
```

For more information on the `repo` command, see the [*helm repo*](https://helm.sh/docs/helm/helm_repo/) documentation.

Once you have added the chart to your repository, you can install it by running the following command:

```shell
# helm install <release-name> graphscope/graphscope
# Replace <release-name> to the name of this release, use `my-gs` for illustration.
helm install my-gs graphscope/graphscope
```

For more information on the `install` command, see the [*helm install*](https://helm.sh/docs/helm/helm_install/) documentation.

Note that it may take a few minutes to pull the image the first time you run this command. You can monitor the status by executing `helm test` multiple times:

```shell
# After installation, you can check service availability by:
helm test my-gs
```


After the `helm test` shows that the installation was succeed, it would print out the informations to connect to the GraphScope.
After which you could copy and paste the instructions to save the connection informations to the environment variables.
You could also get the endpoints of GraphScope by

```shell
helm status my-gs
```

When the service type is `NotePort`, a sample output will be 

```shell
 export NODE_IP=$(kubectl --namespace default  get pod -l graphscope.coordinator.name=coordinator-my-gs -ojsonpath="{.items[0].status.hostIP}")
  export NODE_PORT=$(kubectl --namespace default get services coordinator-service-my-gs  -ojsonpath="{.spec.ports[0].nodePort}")
  echo "GraphScope service listen on ${NODE_IP}:${NODE_PORT}"
```

## Connect to GraphScope

You'll need `graphscope-client` package to `import graphscope`.

Since the GraphScope has been running in the cluster, you only need to **connect** to it.
Other than the connection procedure, other statements are identical to those GraphScope clusters launched by Python client.

```python
import graphscope
graphscope.set_option(show_log=True)
sess = graphscope.session(addr='<ip>:<port>')

# Load an empty graph to get started
g = sess.g()
# create an interactive instance
interactive = graphscope.gremlin(g)
```

The param `addr` is an endpoint for connecting a pre-launched service. The `<ip>` and `<port>` is the connection informations you get from previous step.

Note that only one session can be connected to the service at the same time, but you can reconnect the same service after session close.

````{tip}
In helm installation, the `sess.close()` doesn't remove the resources (pods, services, etc.) of GraphScope, since the resources are managed by helm.
The close only disconnect current session from the backend.

It also means you could connect to the same GraphScope service after a while.

To remove the resources, use `helm uninstall`. See next section for details.
````

```python
# sess1 = graphscope.session(addr='<ip>:<port>')
sess1.close()
sess2 = graphscope.session(addr='<ip>:<port>')
```

See [*create a session*](https://graphscope.io/docs/reference/session.html) for command documentation.

## Uninstall the release

```shell
helm uninstall my-gs
```
This removes all the Kubernetes components associated with the chart and deletes the release.
See [*helm uninstall*](https://helm.sh/docs/helm/helm_uninstall/) for `helm uninstall` command documentation.


## Configuration

See [*Customizing the Chart Before Installing*](https://helm.sh/docs/intro/using_helm/#customizing-the-chart-before-installing). 

To see all configurable options with detailed comments, visit the chart's [values.yaml](https://github.com/alibaba/GraphScope/blob/main/charts/graphscope/values.yaml), or run these configuration commands:


```shell
helm show values graphscope/graphscope
```

And you could see more details in the [homepage](https://artifacthub.io/packages/helm/graphscope/graphscope) of graphscope charts.


## Offline Installation
While it's convenient to install graphscope by quering the remote repository, users may want to use it in some environment that doesn't have internet access.
Or user may want to make some customization of the charts before installation.

To cater these needs, We provide two ways to install graphscope by helm without internet access.
1. you can download the charts file:

```shell
wget https://graphscope.oss-cn-beijing.aliyuncs.com/charts/graphscope-0.20.0.tgz
```

Then, copy the package to the server, unzip, and install graphscope charts.

```shell
tar zxvf graphscope-0.20.0.tgz
helm install my-gs ./graphscope
```

2. You could clone the repo of GraphScope to get the source code of the charts

```shell
git clone https://github.com/alibaba/GraphScope
cd GraphScope/charts
helm install my-gs ./graphscope
```
