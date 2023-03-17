# GIE Standalone Deployment
## Directory Structure
```
./gie-standalone/
├── Chart.yaml
├── README.md
├── templates
│   ├── configmap.yaml
│   ├── frontend
│   │   ├── statefulset.yaml
│   │   └── svc.yaml
│   ├── _helpers.tpl
│   ├── serviceaccount.yaml
│   └── store
│       ├── statefulset.yaml
│       └── svc-headless.yaml
├── tools
│   ├── etcd.yaml
│   └── pvc.yaml
└── values.yaml
```
## Prerequisite
### etcd
```
cd GraphScope/charts
kubectl apply -f gie-standalone/tools/etcd.yaml
```
### prepare graph data
- experimental
```
# experimental storage will create modern graph for tests by default,
# prepare your own data under the directories of `graph_schema` and `graph_data_bin` if needed.
cp -r graph_schema /tmp/data/
cp -r graph_data_bin /tmp/data
```
- vineyard
```
# there are some sample data for tests under the `resource` directory, just copy them to the target directory
# prepare your own data under the directories of /tmp/data, and config mappings to load graph in the file `gie-standalone/templates/configmap.yaml`.
cp -r GraphScope/interactive_engine/tests/src/main/resources/* /tmp/data/
```
### prepare k8s volume
- config `gie-standalone/tools/pvc.yaml`
```
hostPath:
  path: /tmp/data # keep consistent with the directory where the graph data is located
```
- create pvc and pv
```
kubectl apply -f gie-standalone/tools/pvc.yaml
```
## Getting Started
### config `gie-standalone/values.yaml`
- experimental
```
# docker artifacts
image:
  # for experimental store
  registry: registry.cn-hongkong.aliyuncs.com
  repository: graphscope/gie-exp-runtime
  tag: ""
 
# storage type
storageType: Experimental

# schema needed by compiler, config in `gie-standalone/templates/configmap.yaml`
schemaConfig: "expr_modern_schema.json"
```
- vineyard
```
# docker artifacts
image:
  # for vineyard store
  registry: registry.cn-hongkong.aliyuncs.com
  repository: graphscope/graphscope
  tag: "ir_standalone"
  
# storage type
storageType: Vineyard

# schema needed by compiler, config in `gie-standalone/templates/configmap.yaml`
schemaConfig: "v6d_modern_schema.json"

# mappings from csv to v6d data type for graph loading, config in `gie-standalone/templates/configmap.yaml`
htapLoaderConfig: "v6d_modern_loader.json" 
```
- common
```
frontend:
  replicaCount: 1 # frontend num
    service:
      gremlinPort: 8182 # gremlin service port
    
store:
  replicaCount: 1 # store num

# job config
pegasusWorkerNum: 2
pegasusTimeout: 240000
pegasusBatchSize: 1024
pegasusOutputCapacity: 16

# k8s volume to store graph data
existingClaim: "test-graphscope-store-pvc"
```
### start gie deployment
```
helm install <your-release-name> gie-standalone
```
### stop gie deployment
```
helm delete <your-release-name>
```
### get service endpoint
```
# execute in advance if in minikube environment
minikube tunnel 
# external ip
kubectl get svc | grep frontend | grep <your-release-name>
# external port is the configuration of `gremlinPort`
```
