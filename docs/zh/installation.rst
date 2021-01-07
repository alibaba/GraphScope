安装
====
GraphScope的客户端以Python程序包的形式发布，它背后是通过容器机制管理一组引擎和协调器。

实际上，GraphScope可以运行在Kubernetes管理的群集上。方便起见，我们可以按照本文档的以下步骤进行安装，其使用 `minikube <https://minikube.sigs.k8s.io/>`_ 部署的本地Kubernetes集群，并加载预编译好的镜像。

下述依赖项是本地运行GraphScope的前提。

- Docker
- minikube
- Python 3.8 (with pip)

对于Windows和MacOS的用户，可通过官方文档来安装上述依赖。对于常见的Linux发行版用户，我们提供了脚本来准备运行时环境。或者，您可以在Windows上安装 `WSL2 <https://docs.microsoft.com/zh-cn/windows/wsl/install-win10>`_ 以使用脚本。

.. code:: bash

    # if on WSL2, we need to enable systemd first. Otherwise, skip this step.
    ./script/wsl/enable_systemd.sh

    # run the environment preparing script.
    ./scripts/prepare_env.sh

接下来，通过 `pip <https://pip.pypa.io/en/stable/>`_ 安装GraphScope客户端:

.. code:: shell

    pip install graphscope

或者，您可以通过源码安装GraphScope客户端:

.. code:: shell

    pip install 'git+https://github.com/alibaba/GraphScope.git'

或者，您可以通过如下命令直接安装 :code:`.wheel` 包:

.. code:: shell

    pip install graphscope-0.1.macosx-10.14-x86_64.tar.gz


如果您想从源代码构建软件包，请使用git从 `仓库 <https://github.com/alibaba/GraphScope.git>`_ 下载最新版本的代码:

.. code:: shell

    git clone git@github.com:alibaba/GraphScope.git
    git submodule update --init
    cd python

源码的安装命令如下:

.. code:: shell

    pip install -U -r requirements.txt
    python setup.py install
