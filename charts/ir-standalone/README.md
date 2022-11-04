# IR Standalone Deployment
## Directory Structure
```
├── ir-standalone
│   ├── Chart.yaml
│   ├── README.md
│   ├── etcd.yaml
│   ├── pvc.yaml
│   ├── templates
│   │   ├── _helpers.tpl
│   │   ├── configmap.yaml
│   │   ├── frontend
│   │   │   ├── statefulset.yaml
│   │   │   └── svc.yaml
│   │   ├── serviceaccount.yaml
│   │   └── store
│   │       ├── statefulset.yaml
│   │       └── svc-headless.yaml
│   └── values.yaml
└── role_and_binding.yaml
```
## Prepare Dependencies (just initialize once)
### rbac authorization
```
kubectl apply -f role_and_binding.yaml
```
### etcd
```
cd GraphScope/charts
kubectl apply -f ir-standalone/tools/etcd.yaml
```
### prepare graph data
- prepare raw data
```
# for vineyard store (there are some sample data for tests under the `resource` directory, just copy them to the target directory)
cp -r GraphScope/interactive_engine/tests/src/main/resources/ /tmp/data/

# for experimental store (experimental storage will create modern graph for tests by default, prepare your own raw data under the directories of graph_schema and graph_data_bin if you need other graph data for benchmark)
cp -r graph_schema /tmp/data/
cp -r graph_data_bin /tmp/data
```
- config `ir-standalone/pvc.yaml`
```
hostPath:
  path: /tmp/data # be consistent with the directory where the graph data is stored
```
- create pvc and pv
```
kubectl apply -f ir-standalone/tools/pvc.yaml
```
## Getting Started
### config `ir-standalone/values.yaml`
```
# docker artifacts
image:
  registry: registry.cn-hongkong.aliyuncs.com
  repository: graphscope/gie-exp-runtime
  tag: "latest"

# store num
store:
    replicaCount: 1

# storage type: Experimental or Vineyard
storageType: Experimental

# need by compiler service to access meta, the concrete content is in ir-standalone/templates/configmap.yaml
schemaConfig: "exp_modern_schema.json"

# gremlin service port
gremlinPort: 12312

# Pegasus Config
pegasusWorkerNum: 2
pegasusTimeout: 240000
pegasusBatchSize: 1024
pegasusOutputCapacity: 16

# pvc used by pod instance (default is the pvc created above)
existingClaim: "test-graphscope-store-pvc"

# extra configurations if based on vineyard storage
htapLoaderConfig: "v6d_modern_loader.json" # need by vineyard instance to load raw data into in-memory graph structure, the concrete content is in ir-standalone/templates/configmap.yaml
```
### start ir deployment
```
helm install <your-release-name> ir-standalone
```
### stop ir deployment
```
helm delete <your-release-name>
```
### get service endpoint
```
minikube tunnel # execute in advance if in minikube environment

kubectl get svc | grep frontend # EXTERNAL-IP:12312
```