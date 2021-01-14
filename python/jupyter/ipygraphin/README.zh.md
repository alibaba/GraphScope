# ipygraphin

#### 安装

使用 conda 安装：

```bash
$ conda install -c conda-forge notebook
```

使用 pip 安装：

```bash
$ pip install notebook
```

也可以直接 clone jupyterlab 源码进行安装，这样方便后续的调试。

> jupyterlab 官方刚刚升到 3.0 版本，此版本还不稳定，推荐切换到 `2.3.x` 分支，使用这个版本进行安装

```bash
$ git clone https://github.com/<your-github-username>/jupyterlab.git
$ cd jupyterlab
$ git checkout 2.3.x
$ pip install -e .
$ jlpm install # jupyterlab 安装以后，jlpm 会安装
$ jlpm run build  # Build the dev mode assets (optional)
$ jlpm run build:core  # Build the core mode assets (optional)
$ jupyter lab build  # Build the app dir assets (optional)
```

安装完以后，通过以下命令启动 Jupyterlab 应用。

```bash
$ jupyter lab --watch
```

启动成功后，访问 Jupyterlab 应用，点击扩展的 icon，点击 enabled 开启扩展支持，开启后，如下图所示：
![image.png](https://intranetproxy.alipay.com/skylark/lark/0/2021/png/178530/1610077731275-74d023e5-6e2c-4d9a-b65b-e11603489eb6.png#align=left&display=inline&height=607&margin=%5Bobject%20Object%5D&name=image.png&originHeight=1214&originWidth=680&size=294726&status=done&style=none&width=340)

#### 开启 ipywidget 支持

通过安装 [jupyterlab-manager](https://www.npmjs.com/package/@jupyter-widgets/jupyterlab-manager) 包来开启 ipywidget 支持，由于使用的是 Jupyterlab 2.3.x 版本，所以需要安装如下版本：

```bash
$ jupyter labextension install @jupyter-widgets/jupyterlab-manager@2
```

完成以后，查看是否成功。

```bash
$ jupyter labextension list
```

如果输出类似下面的信息，则说明已经成功。

![image.png](https://intranetproxy.alipay.com/skylark/lark/0/2021/png/178530/1610078022456-ffb1e5a8-ae3a-4fb3-9e9c-59e3b3d94a82.png#align=left&display=inline&height=117&margin=%5Bobject%20Object%5D&name=image.png&originHeight=234&originWidth=1114&size=124260&status=done&style=none&width=557)

#### 安装 Graph 扩展

准备好以上的开发环境以后，停止 jupyterlab 进程，然后 clone Graph 的扩展包代码。

**安装扩展包**

```bash
$ git clone https://code.alipay.com/iai/ipygraphiin
$ cd ipygraphiin
$ pip install -e .
$ pip show # 查看是否安装成功
```

显示以下信息，则说明安装成功。
![image.png](https://intranetproxy.alipay.com/skylark/lark/0/2021/png/178530/1610078427437-227e6f57-a30c-4bbf-9829-15d15e53bfda.png#align=left&display=inline&height=159&margin=%5Bobject%20Object%5D&name=image.png&originHeight=318&originWidth=850&size=132299&status=done&style=none&width=425)

**安装对应的前端扩展**

```bash
$ cd ipygraphiin
$ jupyter labextension instanll .
```

**重新构建 jupyterlab 应用**

```bash
$ jlpm run build:core
$ jupyter lab build
```

**重新启动 jupyterlab**

```bash
$ jupyter lab --watch
```

卸载已经安装的扩展

```bash
$ jupyter labextension uninstall .
```
