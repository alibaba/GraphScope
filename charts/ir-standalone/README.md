# IR Standalone Deployment
## directory structure
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
## prepare dependencies (只需第一次初始化执行)
### etcd
```
cd GraphScope/charts
kubectl apply -f ir-standalone/etcd.yaml
```
### rbac authorization
```
kubectl apply -f role_and_binding.yaml
```
## prepare modern graph (只需第一次初始化执行)
- 准备vineyard导图的csv文件
```
cp -r GraphScope/interactive_engine/tests/src/main/resources/ /tmp/data/ # 确保数据在k8s调度角色的所在机器

minikube mount GraphScope/interactive_engine/tests/src/main/resources:/tmp/data # minikube环境需mount到vm中
```
- 配置`ir-standalone/pvc.yaml`
```
hostPath:
  path: /tmp/data
```
- 创建persistent volume claim (pvc)
```
kubectl apply -f ir-standalone/pvc.yaml
```
## start service 
- 配置`ir-standalone/values.yaml`
```
image: # k8s会在部署角色所在的机器上自动下载 
  registry: registry.cn-hongkong.aliyuncs.com
  repository: graphscope/graphscope
  tag: "ir_standalone"
  
store:
    replicaCount: 1 # 配置engine节点的个数
  
htapLoaderConfig: "modern_loader.json" # ir-standalone/templates/configmap.yaml中准备了modern_loader.json的相关内容

gremlinPort: 12312 # 配置gremlin server port
```
- 部署ir+vineyard
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


