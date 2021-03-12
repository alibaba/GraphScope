GraphScope charts
=================

[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/graphscope)](https://artifacthub.io/packages/helm/graphscope/graphscope)

[GraphScope](https://graphscope.io) is a unified distributed graph computing platform that provides a one-stop environment for performing diverse graph operations on a cluster of computers through a user-friendly Python interface. GraphScope makes multi-staged processing of large-scale graph data on compute clusters simple by combining several important pieces of Alibaba technology: including GRAPE, MaxGraph, and Graph-Learn (GL) for analytics, interactive, and graph neural networks (GNN) computation, respectively, and the [vineyard store](https://v6d.io/) that offers efficient in-memory data transfers.

## Get Repo Info

```shell
$ helm repo add graphscope https://dl.bintray.com/graphscope/charts/
$ helm update
```
See [*helm repo*](https://helm.sh/docs/helm/helm_repo/) for command documentation.

## Install Chart

```shell
# Helm 3
$ helm install [RELEASE_NAME] graphscope/graphscope

# Helm 2
$ helm install --name [RELEASE_NAME] graphscope/graphscope
```
See configuration below.
See [*helm install*](https://helm.sh/docs/helm/helm_install/) for command documentation.


## Get GraphScope Service Endpoint 

```shell
# Helm 3 or 2 
# After installation, you can check service availability by:
$ helm test [RELEASE_NAME]

# Default, with kubernetes `NodePort` service type, you can get service endpoint by:
$ export NODE_IP=$(kubectl --namespace [NAMESPACE] get pod -o jsonpath="{.status.hostIP}" [GRAPHSCOPE-FULLNAME]-coordinator)
$ export NODE_PORT=$(kubectl --namespace [NAMESPACE] get services -o jsonpath="{.spec.ports[0].nodePort}" [GRAPHSCOPE-FULLNAME]-coordinator-service)
$ echo "GraphScope service listen on ${NODE_IP}:${NODE_PORT}"
```

## An Example to Connect service by Python Client.

```python
import graphscope
graphscope.set_option(show_log=True)
sess = graphscope.session(addr='<ip>:<port>')
```
See [*create a session*](https://graphscope.io/docs/reference/session.html) for command documentation.


## Uninstall Chart

```shell
# Helm 3
$ helm uninstall [RELEASE_NAME]

# Helm 2
$ helm delete --purge [RELEASE_NAME]
```
This removes all the Kubernetes components associated with the chart and deletes the release.
See [*helm uninstall*](https://helm.sh/docs/helm/helm_uninstall/) for command documentation.


## Upgrading Chart

```shell
# Helm 3 or 2
$ helm upgrade [RELEASE_NAME] [CHART] --install
```
Sess [*helm upgrade*](https://helm.sh/docs/helm/helm_upgrade/) for command documentation.


## Configuration

See [*Customizing the Chart Before Installing*](https://helm.sh/docs/intro/using_helm/#customizing-the-chart-before-installing). To see all configurable options with detailed comments, visit the chart's [values.yaml](https://github.com/alibaba/GraphScope/blob/main/charts/graphscope/values.yaml), or run these configuration commands:
```shell
# Helm 2
$ helm inspect values graphscope/graphscope

# Helm 3
$ helm show values graphscope/graphscope
```
In most cases, you want to mount volumes into GraphScope's pod for loading graph. Here is an example to do it:

```yaml
# Mount hostpath `/testingdata` to `/tmp/testingdata` in pod.
# cat values.yaml
# 
volumes:
  enabled: true
  items:
    data:
      type: hostPath
      field:
        type: Directory
        path: /testingdata
      mounts:
      - mountPath: /tmp/testingdata
  
# pass that values file during installation.    
$ helm install -f values.yaml graphscope/graphscope --generate-name
```

## Useful links

- https://graphscope.io/
- https://github.com/alibaba/GraphScope
- https://try.graphscope.app/
