# GraphScope Interactive Deployment

## QuickStart

```bash
helm install release  -f ./charts/graphscope-interactive/nginx_ingress_custom.yaml oci://ghcr.io/nginxinc/charts/nginx-ingress --version 1.2.1
$ helm install {your-release-name} .
```

Now you can the endpoint via
```bash
$ kubectl describe svc {your-release-name} -graphscope-interactive-frontend | grep "Endpoints:" | awk -F' ' '{print $2}'
#192.168.0.44:8182
#192.168.0.44:7687
# the first is the gremlin endpoint(currently not supported)
# the second is the cypher endpoint
$ kubectl describe svc {your-release-name} -graphscope-interactive-engine | grep "Endpoints:" | awk -F' ' '{print $2}'
# the first is the admin port
# the second is the query port
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
For detail about Graph Model and DataLoading, please check [Interactive Data Model](https://graphscope.io/docs/latest/flex/interactive/data_model);

### Single Node

If you just want to deploy Interactive on a single node, then you can just put all `csv` files into a directory, i.e. `/tmp/data/`.

Then you can create a PV on the node, via the following commands
```bash
# 0. Download the template pvc configuration

# 1. Customize the pvc configuration
vim pvc/pvc.yaml
#hostPath:
#  path: {} # use the directory where your raw data exists.

# 2. Create the persistent volume on the single node.
kubectl apply -f pvc.yaml
```

### Multiple nodes

If you want to deploy Interactive on multiple nodes, then you need something like `nfs` to store you raw graph data, such that all k8s nodes can share the access to same files.

TODO

### Customize `Values.yaml`

- docker artifacts

```yaml
engine:
  image:
    registry: registry.cn-hongkong.aliyuncs.com
    repository: graphscope/interactive
    tag: "v0.2.4"

# docker artifacts for frontend
frontend:
  image:
    registry: registry.cn-hongkong.aliyuncs.com
    repository: graphscope/interactive
    tag: "v0.2.4"
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

# hiactor config
hiactorWorkerNum: 1 # currently only support 1.
hiactorTimeout: 240000

```


## TODO

- TODO: Support cypher/gremlin queries.

## Installtion

```bash
helm install lei-test -f settings.yaml . --set odps.access.id="",odps.access.key="",odps.endpoint=""
export NODE_IP=$(ktl -n kubetask get pod lei-test-graphscope-interactive-primary-0 -o jsonpath="{.status.podIP}")
export ADMIN_PORT=$(ktl get pod lei-test-graphscope-interactive-primary-0 -ojsonpath='{.spec.containers[0].ports[0].containerPort}')
export QUERY_PORT=$(ktl get pod lei-test-graphscope-interactive-primary-0 -ojsonpath='{.spec.containers[1].ports[0].containerPort}')
export ADMIN_ENDPOINT=${NODE_IP}:${ADMIN_PORT}
export QUERY_ENDPOINT=${NODE_IP}:${QUERY_PORT}
echo "ADMIN_ENDPOINT: ${ADMIN_ENDPOINT}"
echo "QUERY_ENDPOINT: ${QUERY_ENDPOINT}"
```

```bash
export NODE_IP=$(118f -n kubetask get pod lei-test-graphscope-interactive-primary-0 -o jsonpath="{.status.podIP}")
export ADMIN_PORT=$(118f get pod lei-test-graphscope-interactive-primary-0 -ojsonpath='{.spec.containers[0].ports[0].containerPort}')
export QUERY_PORT=$(118f get pod lei-test-graphscope-interactive-primary-0 -ojsonpath='{.spec.containers[1].ports[0].containerPort}')
```

