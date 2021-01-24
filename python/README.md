# graphscope可视化
## ipygraphin
### 安装
查看ipygraphin安装教程，安装jupyterlab和ipygraphin插件
```bash
$ vi jupyter/ipygraphin/README.zh.md
```
## graphscope
### 安装
```bash
$ pip3 install graphscope
$ cd GraphScope/python
$ python3 setup.py install
```
## 测试
### 本地测试
安装kubectl，在本机安装docker desktop，然后在docker desktop安装kubernetes
```bash
$ brew install kubectl
```
下载测试数据到本机
```bash
$ git clone https://github.com/7br/gstest.git
```
修改jupyter lab默认运行根目录
```bash
$ # GraphScope/python 目录提供了.ipynb测试脚本，
$ vi ~/.jupyter/jupyter_notebook_config.py
$ c.NotebookApp.notebook_dir = 'path_to_graphscope/GraphScope/python'
```
运行jupyter lab
```bash
$ jupyter lab --watch
```
在jupyter lab窗口打开test_graphin_local.ipynb，
修改测试数据路径，修改完后直接运行测试脚本。
```python
os.environ["GS_TEST_DIR"] = os.path.expanduser("path_to_gstest/gstest/")
```
### 远程连接k8s集群
通过config文件可远程连接k8s集群，无需在本地安装docker desktop和kubernetes。config文件可向开发人员索取。
```bash
$ # 备份 ~/.kube/config
$ cp config ~/.kube/config
```
安装kubectl

修改jupyter lab默认运行根目录

运行jupyter lab

在jupyter lab窗口打开test_graphin_k8s_cluster.ipynb，

无需修改测试数据路径，直接运行测试脚本。

## 注意事项
远程k8s集群可能因为网络波动出现连接超时现象

jupyter lab重启kernel前记得关闭session，释放资源

```bash
$ # 查看namespaces
$ kubectl get ns
$ # 删除namespaces
$ kubectl delete ns gs-xxx
```
