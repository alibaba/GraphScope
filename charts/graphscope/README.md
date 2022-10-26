GraphScope charts
=================

[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/graphscope)](https://artifacthub.io/packages/helm/graphscope/graphscope)

[GraphScope](https://graphscope.io) is a unified distributed graph computing platform that provides a one-stop environment for performing diverse graph operations on a cluster of computers through a user-friendly Python interface. GraphScope makes multi-staged processing of large-scale graph data on compute clusters simple by combining several important pieces of Alibaba technology: including GRAPE, MaxGraph, and Graph-Learn (GL) for analytics, interactive, and graph neural networks (GNN) computation, respectively, and the [vineyard store](https://v6d.io/) that offers efficient in-memory data transfers.

## Get Repo Info

```shell
$ helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/charts/
$ helm repo update
```
See [*helm repo*](https://helm.sh/docs/helm/helm_repo/) for command documentation.

## Install Chart

GraphScope rely on some permissions to delete resources.

```shell
# example for `default` ServiceAccount with `default` namespace
$ wget https://raw.githubusercontent.com/alibaba/GraphScope/main/charts/role_and_binding.yaml
$ kubectl create -f ./role_and_binding.yaml
```

```shell
# Helm 3
$ helm install [RELEASE_NAME] graphscope/graphscope

# Helm 2
$ helm install --name [RELEASE_NAME] graphscope/graphscope
```
See configuration below.
See [*helm install*](https://helm.sh/docs/helm/helm_install/) for command documentation.


## Get GraphScope Service Endpoint

Note that it may take a few minutes for pulling image at first time, you can watch the status by running `helm test` many times.

```shell
# Helm 3 or 2
# After installation, you can check service availability by:
$ helm test [RELEASE_NAME]

# Default, with kubernetes `NodePort` service type, you can get service endpoint by:
$ export NODE_IP=$(kubectl --namespace [NAMESPACE] get pod -o jsonpath="{.status.hostIP}" coordinator-[GRAPHSCOPE-FULLNAME])
$ export NODE_PORT=$(kubectl --namespace [NAMESPACE] get services -o jsonpath="{.spec.ports[0].nodePort}" coordinator-service-[GRAPHSCOPE-FULLNAME])
$ echo "GraphScope service listen on ${NODE_IP}:${NODE_PORT}"
```

## An Example to (Re)connect service by Python Client.

```python
import graphscope
graphscope.set_option(show_log=True)
sess = graphscope.session(addr='<ip>:<port>')
```

The param `addr` is an endpoint for connecting a pre-launched service. Note that only one session can be connected to the service at the same time, but you can reconnect the same service after session close.

```python
# sess1 = graphscope.session(addr='<ip>:<port>')
sess1.close()
sess2 = graphscope.session(addr='<ip>:<port>')
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
See [*helm upgrade*](https://helm.sh/docs/helm/helm_upgrade/) for command documentation.


## Configuration

See [*Customizing the Chart Before Installing*](https://helm.sh/docs/intro/using_helm/#customizing-the-chart-before-installing). To see all configurable options with detailed comments, visit the chart's [values.yaml](https://github.com/alibaba/GraphScope/blob/main/charts/graphscope/values.yaml), or run these configuration commands:
```shell
# Helm 2
$ helm inspect values graphscope/graphscope

# Helm 3
$ helm show values graphscope/graphscope
```

### configure volumes for loading graph
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

### configure resource (cpu/memory)

By default, one graphscope instance contains a coordinator pod with 4 CPUs/4Gi memory, 2 engine pods with 2 CPUs/4Gi memory in engine container and 0.5 CPUs/512M in vineyard container respectively, and 3 etcd pods with 0.5 CPU and 128M memory respectively, You can adjust these resources in `values.yaml`.

```
# cat values.yaml
coordinator:
  resources:
    requests:
      cpu: 3.0
      memory: 4Gi
    limits:
      cpu: 3.0
      memory: 4Gi

# one engine pod contains a engine container and a vineyard container
engines:
  resources:
    requests:
      cpu: 2.0
      memory: 4Gi
    limits:
      cpu: 2.0
      memory: 4Gi

vineyard:
  resources:
    requests:
      cpu: 0.5
      memory: 512Mi
    limits:
      cpu: 0.5
      memory: 512Mi
  ## Init size of vineyard shared memory.
  shared_mem: 8Gi


etcd:
  resources:
    requests:
      cpu: 0.5
      memory: 128Mi
    limits:
      cpu: 0.5
      memory: 128Mi

# pass that values file during installation.
$ helm install -f values.yaml graphscope/graphscope --generate-name
```

## Offline Deployment
With offline environment, you can download the charts file first:

```shell
# version 0.11.0
$ wget https://graphscope.oss-cn-beijing.aliyuncs.com/charts/graphscope-0.11.0.tgz
```

Then, transfer the package to the server, unzip, and install graphscope charts.

```shell
$ tar zxvf graphscope-0.11.0.tgz
$ helm install graphscope ./graphscope --namespace=default
```

## Useful links

- https://graphscope.io/
- https://github.com/alibaba/GraphScope
- https://try.graphscope.app/
