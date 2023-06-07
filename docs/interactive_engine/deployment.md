# Standalone Deployment for GIE

We have demonstrated [how to execute interactive queries](./getting_started.md) easily by installing Graphscope via `pip` on a local machine. However, in real-life applications, graphs are often too large to fit on a single machine. In such cases, Graphscope can be deployed on a cluster, such as a [self-managed k8s cluster](../deploy_graphscope_on_self_managed_k8s.md), for processing large-scale graphs. But you may wonder, "what if I only need the GIE engine and not the whole package of GraphScope?" This tutorial will walk you through the process of standalone deployment of GIE on a self-managed k8s cluster.

Throughout the tutorial, we assume all machines are running Linux system.
We do not guarantee that it works as smoothly as Linux on the other platform.
For your reference, we've tested the tutorial on Ubuntu 20.04.

## Prerequisites

- Kubernetes Cluster
- Python >= 3.9

To get started, you need to prepare a Kubernetes Cluster to continue.

Incase you doesn't have one, you could refer to the instruction of [create kubernetes cluster](../deployment/deploy_graphscope_on_self_managed_k8s.md#prepare-a-kubernetes-cluster).


## Deploy Your First GIE Service

The easiest way to deploy GIE standalone is by using Helm, which is a package manager for K8s that simplifies the
deployment and management of applications. To deploy GIE standalone using Helm, you can follow these steps:

- Install Helm on your local machine if you do not have it by following the
   instructions on the [official Helm website](https://helm.sh/docs/intro/install/).
- Pull the Helm repository to your local disk:
   ```bash
   helm pull graphscope/gie-standalone --untar
   ```
- Prepare the `etcd` pod.
   ```bash
   kubectl apply -f gie-standalone/tools/etcd.yaml
   ```
- Prepare graph data
   ```
   cp -r gie-standalone/data /tmp/
   ```
   Check whether the raw data is there:
   ```
   tree /tmp/data
   ```
   You should be able to see the raw data of the [modern graph](https://tinkerpop.apache.org/docs/3.6.2/tutorials/getting-started/).
   ```
   /tmp/data
   └── modern_graph
      ├── created.csv
      ├── knows.csv
      ├── person.csv
      └── software.csv
   ```
   Then create K8s persistent volume (PV) and persistent volume claim (PVC).
   ```bash
   kubectl apply -f gie-standalone/tools/pvc.yaml
   ```
   The modern graph raw data in `/tmp/data` will be automatically loaded into the GIE graph store (by default on [Vineyard](https://v6d.io)).

   ```{tip}
   You can load the data from any `/path/to/your/data`. All you need to do is copy the raw data to `/path/to/your/data`
   and modify the `hostPath.path` in `gie-standalone/tools/pvc.yaml` to `/path/to/your/data`.
   ```

- Install the GIE chart:
   ```
   helm install [YOUR_RELEASE_NAME] gie-standalone
   ```
- Verify that the GIE service is running:
   ```
   kubectl get pods
   ```
   You should see the `[YOUR_RELEASE_NAME]-gie-standalone-frontend-0` and `[YOUR_RELEASE_NAME]-gie-standalone-store-0` pods running.

- Get the endpoint of the GIE Frontend service:
   ```bash
   kubectl describe svc [YOUR_RELEASE_NAME]-gie-standalone-frontend \
   | grep "Endpoints:" | awk -F' ' '{print $2}'
   ```
   You should see the GIE Frontend service endpoint as `<ip>:<gremlinPort>`.

- Connect to the GIE frontend service using the Tinkerpop's official SDKs or Gremlin console, which
can be found [here](./tinkerpop_eco.md).

## Remove the GIE Service
```bash
   helm uninstall [YOUR_RELEASE_NAME]
```

## Using Your Own Data
Currently, a single instance of GIE can only handle one set of graph data. This means that you must
indicate which raw data should be uploaded into GIE's graph store, and all subsequent queries made
through the GIE instance will pertain to the uploaded graph.

The above tutorial uses modern graph to demonstrate the launching procedural. However, it's easy to
specify your own data. To do so, you just need to provide a little specification about your data.

Let's look into the specification of modern graph in `gie-standalone/config/v6d_modern_loader.json`:
```json
{
    "vertices": [
        {
            "data_path": "$STORE_DATA_PATH/modern_graph/person.csv",
            "label": "person",
            "options": "header_row=true&delimiter=|"
        },
        {
            "data_path": "$STORE_DATA_PATH/modern_graph/software.csv",
            "label": "software",
            "options": "header_row=true&delimiter=|"
        }
    ],
    "edges": [
        {
            "data_path": "$STORE_DATA_PATH/modern_graph/knows.csv",
            "label": "knows",
            "src_label": "person",
            "dst_label": "person",
            "options": "header_row=true&delimiter=|"
        },
        {
            "data_path": "$STORE_DATA_PATH/modern_graph/created.csv",
            "label": "created",
            "src_label": "person",
            "dst_label": "software",
            "options": "header_row=true&delimiter=|"
        }
    ],
    "directed": 1,
    "retain_oid": 1,
    "generate_eid": 1,
}
```

There're a few things to notice:
- For now, we support loading raw data that are a CSV-like files.
- Prepare an individual file for each type of vertex and edge. For example, in the modern
  graph, the data of "person" vertex is in the file of `modern/person.csv`.
- Place the raw data in the `hostPath.path` specified above.
- For each type of vertex, configure
  - `data_path`: as `hostPath.path`. The default value is `/tmp/data`.
  - `label`: the label of the vertex. For example, "person", "software".
  - `options`: configure as "key1=value1&key2=value2&...". Details can be found in this [guide](https://github.com/v6d-io/v6d/tree/main/modules/graph), while we provide some useful keys here:
    - `header_row`: define whether the file contains a header, the default value is `false`.
    - `delimiter`: the token that separates the data fields of a row of data, the default value is `','`.
    - `column_types`: the data types of all data fields separated by the `delimiter`. If not specified, such as in the modern graph example, the store will attempt to infer the data types from the raw data.
    You can also specify according to your need. For example, if there're two data fields, "filed1" and "filed2", you can specify `column_types=string,int64_t` to indicate their types.
- For each type of edge, configure
  - `data_path`, `label`, `options` are similar to those of vertices. To save you from some
  unexpected trouble, you'd better make the first two data fields record the ids of the source and destination vertices, and if `column_types` is given, the first two data fields are configured
    to `int64_t` correspondingly.
  - `src_label`: the label of the source vertex of this edge.
  - `dst_label`: the label of the destination vertex of this edge.

```{tip}
For your reference, we have provided a sample for loading LDBC data in `gie-standalone/config/v6d_ldbc_loader.json`.
```

## Deploy on a Cluster
In K8s, it’s convenient to deploy GIE in a cluster with multiple machines.
You don’t need to be aware of the physical machines, but simply configure the number of executors
to make GIE scalable. These GIE executors will be seamlessly assigned by K8s to the physical machines.

You simply set the number of executors as:
```
helm install [YOUR_RELEASE_NAME] graphscope/gie-standalone --set executor.replicaCount=3
```

This instruction deploys the GIE chart using 3 executors that process graph partitions in v6d.
The number of replicas can be modified according to your needs, but better be less than the number
of CPUs in your cluster. When specifying the number of executors, v6d loads data from the specified
location and partitions graph data automatically for each executor. It is recommended to store data
in a distributed file system like `HDFS` for convenience. In this case, you can simply configure
the above `data_path` to use the `hdfs://` scheme.


## Other Useful Configurations
Extra configurations can be set as:
```bash
helm install [YOUR_RELEASE_NAME] graphscope/gie-standalone --set [key1]=[value1],[key2]=[value2]
```
We've listed useful configuration keys in the following:

- gremlinPort: the port for accessing the Gremlin service (Default: 8182).
- pegasusWorkerNum: the number of working threads per each executor (Default: 2).
  Obviously, the total number of working threads is: 'executor.replicaCount x pegasusWorkerNum'.
- pegasusTimeout: The maximum duration in `ms` you allow each query to run (Default: 24,000).

