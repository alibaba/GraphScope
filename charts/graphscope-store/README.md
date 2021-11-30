GraphStore Charts
=================

[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/graphscope)](https://artifacthub.io/packages/helm/graphscope/graphscope-store)

[GraphScope Store](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/groot/src/main) is a new disk-based row-oriented multi-versioned persistent graph store.


## TL;DR

```bash
$ helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/charts/
$ helm install my-release graphscope/graphscope-store
```

## Introduction

This chart bootstraps a [GraphScope Store](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/groot/src/main) cluster deployment on a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Kubernetes 1.12+
- Helm 3.1.0
- PersistentVolume(PV) provisioner support in the underlying infrastructure

## Installing the Chart

To install the chart with the release name `my-release`:

```bash
$ helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/store/charts/
$ helm install my-release graphscope/graphscope-store
```

These commands deploy GraphScope Store on the Kubernetes cluster in the default configuration. The [Parameters](#parameters) section lists the parameters that can be configured during installation.

> **Tip**: List all releases using `helm list`

## Get GraphScope Store Endpoint

Note that it may take a few minutes for pulling image at first time, you can watch the status by running `helm test` many times.

```bash
# After installation, you can check service availability by:
$ helm test my-release

# Default, with kubernetes `NodePort` service type, you can get service endpoint by:
$ export NODE_IP=$(kubectl get nodes --namespace default -o jsonpath="{.items[0].status.addresses[0].address}")
$ export GRPC_PORT=$(kubectl get --namespace default -o jsonpath="{.spec.ports[0].nodePort}" services my-release-graphscope-store-frontend)
$ export GREMLIN_PORT=$(kubectl get --namespace default -o jsonpath="{.spec.ports[1].nodePort}" services my-release-graphscope-store-frontend)
$ echo "GRPC endpoint is: ${NODE_IP}:${GRPC_PORT}"
$ echo "GREMLIN endpoint is: ${NODE_IP}:${GREMLIN_PORT}"
```

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
$ helm delete my-release
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

> **Note**: The PersistentVolume remains even after the release was uninstalled. To delete the PV manually:

```bash
$ kubectl delete pvc -l app.kubernetes.io/instance=my-release
```


## Parameters

Here we give a list of most frequently used parameters.

### Common parameters

| Name | Description | Value |
|---|---|---|
| image.registry | Docker image registry | registry.cn-hongkong.aliyuncs.com |
| image.repository | Docker image repository | graphscope/graphscope-store |
| image.tag | Docker image tag | "" |
| image.pullPolicy | Docker image pull policy | IfNotPresent |
| image.pullSecrets | Docker-registry secrets | [] |
| clusterDomain | Default Kubernetes cluster domain | cluster.local |
| commonLabels | Labels to add to all deployed objects | {} |
| commonAnnotations | Annotations to add to all deployed objects | {} |
| executor | Executor type, "maxgraph" or "gaia" | maxgraph |
| javaOpts | Java options | "" |


### Statefulset parameters

| Name | Description | Value |
|---|---|---|
| store.replicaCount | Number of nodes | 2 |
| store.updateStrategy | Update strategy for the store | RollingUpdate |
| frontend.replicaCount | Number of nodes | 1 |
| frontend.updateStrategy | Update strategy for the frontend | RollingUpdate |
| frontend.service.type| Kubernetes Service type| NodePort |
| ingestor.replicaCount | Number of nodes | 1 |
| ingestor.updateStrategy | Update strategy for the ingestor | RollingUpdate |
| coordinator.replicaCount | Number of nodes | 1 |
| coordinator.updateStrategy | Update strategy for the coordinator | RollingUpdate |

### Kafka chart parameters

| Name | Description | Value |
|---|---|---|
| kafka.enabled | Switch to enable or disable the Kafka helm chart | true |
| kafka.replicaCount | Number of Kafka nodes | 1 |
| externalKafka.servers | Server or list of external Kafka servers to use | [] |

## Configuration

See [*Customizing the Chart Before Installing*](https://helm.sh/docs/intro/using_helm/#customizing-the-chart-before-installing). To see all configurable options with detailed comments, visit the chart's [values.yaml](https://github.com/alibaba/GraphScope/blob/main/charts/graphscope-store/values.yaml), or run these configuration commands:

```bash
$ helm show values graphscope/graphscope-store
```

Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
$ helm install my-release \
  --set image.tag=latest graphscope/graphscope-store
```

Add multiple extra config to the component which is defined in the configmap by
`--set extraConfig=k1=v1:k2=v2`. Note we use `:` to seperate config items. For example,

```bash
$ helm install my-release \
  --set extraConfig=k1=v1:k2=v2 graphscope/graphscope-store
```


Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart. For example,

```bash
$ helm install my-release -f values.yaml graphscope/graphscope-store
```

> **Tip**: You can use the default [values.yaml](values.yaml)


## Persistence

The [GraphScope Store](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/src/v2/src/main) image stores the GraphScope Store data at the `/var/lib/graphscope-store` and configurations at the `/etc/graphscope-store/my.cnf`, and coordinator meta information at the `/etc/graphscope-store/my.meta` path of the container.

The chart mounts a [Persistent Volume](https://kubernetes.io/docs/user-guide/persistent-volumes/) volume at this location. The volume is created using dynamic volume provisioning by default. An existing PersistentVolumeClaim can also be defined for this purpose.


## Pod affinity

This chart allows you to set your custom affinity using the `XXX.affinity` parameter(s). Find more information about Pod affinity in the [Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity).


## Troubleshooting

Find more information about how to deal with common errors related to Bitnami’s Helm charts in [this troubleshooting guide](https://docs.bitnami.com/general/how-to/troubleshoot-helm-chart-issues).


## Upgrading

```bash
$ helm upgrade my-release graphscope/graphscope-store
```
See [*helm upgrade*](https://helm.sh/docs/helm/helm_upgrade/) for command documentation.
