# GraphScope Interactive Deployment

## QuickStart

```bash
$ helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/charts/
$ helm install {your-release-name} graphscope/graphscope-interactive
```

Now you can the endpoint via
```bash
$ kubectl describe svc {your-release-name} -gie-standalone-frontend | grep "Endpoints:" | awk -F' ' '{print $2}'
```

Delete the deployment via 
```bash
helm delete {your-release-name}
```

## Customizing Configuration

By default, all pods of `GraphScope Interactive` is deployed on the same k8s node.
In such case, all pods can share the storage via k8s pvc of `hostPath` kind.

To offer higher throughput and query performance, we also support launching pods across multiple nodes. However, in this mode, you need a storage solution like NFS to ensure that all nodes can access the same storage.


## Customize Graph Data.

By default we provide builtin `modern_graph` for `Interactive`. We also support customizing graph data by providing raw graph data via `csv` files. 
For detail about Graph Model and DataLoading, please check []();

### Single Node

If you just want to deploy Interactive on a single node, then you can just put all `csv` files into a directory, i.e. `/tmp/data/`.

Then you can create a PV on the node, via the following commands
```bash
# 0. Download the template pvc configuration
curl -O -S https://raw.githubusercontent.com/shirly121/GraphScope/add_gie_deploy/charts/gie-standalone/tools/pvc_hostpath.yaml

# 1. Customize the pvc configuration
vim pvc.yaml
#hostPath:
#  path: {} # use the directory where your raw data exists.

# 2. Create the persistent volume on the single node.
kubectl apply -f pvc.yaml
```

### Multiple nodes

If you want to deploy Interactive on multiple nodes, then you need something like `nfs` to store you raw graph data, such that all k8s nodes can share the access to same files.

```bash
# 0. Download the template pvc configuration
curl -O -S https://raw.githubusercontent.com/shirly121/GraphScope/add_gie_deploy/charts/gie-standalone/tools/pvc_hostpath.yaml

# 1. Customize the pvc configuration
vim pvc.yaml
#hostPath:
#  path: {} # path to raw data on your nfs

# 2. Create the persistent volume on the single node.
kubectl apply -f pvc_nfs.yaml
```

### Customize `Values.yaml`

- deploy_mode: single_node or multi_node

- docker artifacts

```yaml
# docker artifacts for vineyard store
engine:
  image:
    registry: registry.cn-hongkong.aliyuncs.com
    repository: graphscope/interactive
    tag: "0.20.0"

# docker artifacts for frontend
frontend:
  image:
    registry: registry.cn-hongkong.aliyuncs.com
    repository: graphscope/interactive
    tag: "0.20.0"
```

- pvc

```yaml
  persistence:
    ## If true, use a Persistent Volume Claim, If false, use emptyDir
    ##
    enabled: true
    ## Name of existing PVC to hold GraphScope store data
    ## NOTE: When it's set the rest of persistence parameters are ignored
    ##
    existingClaim: "graphscope-interactive-pvc"
    # existingClaim: ""
```

- common

```yaml
frontend:
  replicaCount: 1 # frontend num
    service:
      gremlinPort: 8182 # gremlin service port
    
executor:
  replicaCount: 1 # executor num

# job config
pegasusWorkerNum: 2
pegasusTimeout: 240000
pegasusBatchSize: 1024
pegasusOutputCapacity: 16


# data path where the inner pod read graph data from
storeDataPath: "/tmp/data"
# hdfs path is supported in vineyard
# storeDataPath: "hdfs://{ip}:{port}"
```
