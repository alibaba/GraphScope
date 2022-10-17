# IR Standalone Deployment
## Directory Structure
```
├── ir-standalone
│   ├── Chart.yaml
│   ├── etcd.yaml
│   ├── pvc.yaml
│   ├── README.md
│   ├── templates
│   │   ├── configmap.yaml
│   │   ├── frontend
│   │   │   ├── statefulset.yaml
│   │   │   └── svc.yaml
│   │   ├── _helpers.tpl
│   │   ├── serviceaccount.yaml
│   │   ├── store
│   │   │   ├── statefulset.yaml
│   │   │   └── svc-headless.yaml
│   │   └── test
│   │       └── test-rpc.yaml
│   └── values.yaml
└── role_and_binding.yaml

```
## Prepare Dependencies (只需第一次初始化执行)
### rbac authorization
```
kubectl apply -f role_and_binding.yaml
```
### etcd
```
cd GraphScope/charts
kubectl apply -f ir-standalone/etcd.yaml
```
### prepare graph data
- 在本地机器上准备图数据
```
# for vineyard store (resources目录下存储了modern/crew等小图的csv文件，直接拷贝到本地机器的相应目录)
cp -r GraphScope/interactive_engine/tests/src/main/resources/ /tmp/data/

# for experimental store (实验存储默认会创建modern graph, 如加载其他图需在目录graph_schema和graph_data_bin下准备相应数据)
cp -r graph_schema /tmp/data/
cp -r graph_data_bin /tmp/data
```
- 配置`ir-standalone/pvc.yaml`
```
hostPath:
  path: /tmp/data # 和本地数据所在目录保持一致
```
- 创建pvc和pv
```
kubectl apply -f ir-standalone/pvc.yaml
```
## Getting Started
- 配置`ir-standalone/values.yaml`
```
# 配置启动服务所需的docker镜像
image:
  registry: registry.cn-hongkong.aliyuncs.com
  repository: graphscope/gie-exp-runtime
  tag: ""

# 配置engine节点的个数
store:
    replicaCount: 1

# 指定启动的存储类型, Experimental or Vineyard
storageType: Experimental

# 用于compiler查询schema的配置文件, json具体内容在ir-standalone/templates/configmap.yaml
schemaConfig: "exp_modern_schema.json"

# 配置外部访问的gremlin server端口
gremlinPort: 12312

# Pegasus Config
pegasusWorkerNum: 2
pegasusTimeout: 240000
pegasusBatchSize: 1024
pegasusOutputCapacity: 16

# 配置pod使用的pvc (默认为上述创建的pvc，可以配置其他)
existingClaim: "test-graphscope-store-pvc"

# 如果基于vineyard存储，需额外配置:
htapLoaderConfig: "v6d_modern_loader.json" # 用于vineyard导入数据的配置文件, json具体内容在ir-standalone/templates/configmap.yaml
```
- 部署GIE
```
kubectl get pods # 确认先前服务已停止

helm install <your-release-name> ir-standalone
```
## delete service
```
helm delete <your-release-name>
```
## get gremlin endpoint
```
minikube tunnel # minikube环境中需额外执行

kubectl get svc | grep frontend # EXTERNAL-IP:12312
```