# GraphScope Interactive deployment

## Prerequisite


### prepare graph data

Put all the data you need to use into a local directory, for example `/tmp/data/`.


### prepare k8s volume

- download `pvc.yaml`
```
# TODO: the link need to be updated after merging to main
curl -O -S https://raw.githubusercontent.com/shirly121/GraphScope/add_gie_deploy/charts/gie-standalone/tools/pvc.yaml
```
- config `pvc.yaml`
```
hostPath:
  path: /tmp/data # keep consistent with the directory where the graph data is located
```
- create pvc and pv
```
kubectl apply -f pvc.yaml
```

## Getting Started

### start gie deployment
- from remote
```
helm repo update
helm install <your-release-name> graphscope/gie-standalone
```
- from local (for customized config)
```
# download helm package
helm pull graphscope/gie-standalone --untar
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
# gremlin endpoint
kubectl describe svc <your-release-name>-gie-standalone-frontend | grep "Endpoints:" | awk -F' ' '{print $2}'
```
## Customized Config
### download helm package
```
helm pull graphscope/gie-standalone --untar 

gie-standalone/
├── Chart.yaml
├── config
│   └── v6d_modern_loader.json
├── data
│   └── modern_graph
│       ├── created.csv
│       ├── knows.csv
│       ├── person.csv
│       └── software.csv
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
### config `gie-standalone/values.yaml`
#### vineyard
- docker artifacts
```
# docker artifacts for vineyard store
executor:
  image:
    registry: registry.cn-hongkong.aliyuncs.com
    repository: graphscope/interactive-executor
    tag: "0.20.0"

# docker artifacts for frontend
frontend:
  image:
    registry: registry.cn-hongkong.aliyuncs.com
    repository: graphscope/interactive-frontend
    tag: "0.20.0"
```
- load your own graph data
```
# add your graph loader config under `gie-standalone/config`

config
└── v6d_modern_loader.json

# config the file name in `gie-standalone/values.yaml`
htapLoaderConfig: "v6d_modern_loader.json" 
```
#### common
```
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

# k8s volume to store graph data
existingClaim: "test-graphscope-store-pvc"

# data path where the inner pod read graph data from
storeDataPath: "/tmp/data"
# hdfs path is supported in vineyard
# storeDataPath: "hdfs://{ip}:{port}"
```
#### experimental
```
# docker artifacts for experimental store
executor:
  image:
    registry: registry.cn-hongkong.aliyuncs.com
    repository: graphscope/gie-exp-runtime
    tag: ""

# docker artifacts for frontend
frontend:
  image:
    registry: registry.cn-hongkong.aliyuncs.com
    repository: graphscope/gie-exp-runtime
    tag: ""
 
# storage type
storageType: Experimental

# schema needed by compiler, config in `gie-standalone/templates/configmap.yaml`
schemaConfig: "expr_modern_schema.json"
```
