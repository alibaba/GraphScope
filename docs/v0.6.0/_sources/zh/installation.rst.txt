安装
====
GraphScope 客户端以 Python 程序包的形式发布，背后通过容器机制管理一组引擎和一个协调器。

GraphScope 被设计为运行在 Kubernetes 管理的群集上。
方便起见，我们可以按照本文档的以下步骤部署一个本地 Kubernetes 集群，并加载预编译好的镜像。

本地运行 GraphScope 需要预先安装以下依赖。

- Docker
- Python 3.6+ (with pip)
- Local Kubernetes cluster set-up tool (e.g. `Kind <https://kind.sigs.k8s.io>`_)

对于 Windows 和 MacOS 的用户，可通过官方文档来安装上述依赖, 并在Docker中开启Kubernetes功能。
对于Ubuntu/CentOS Linux 发行版用户，我们提供了脚本来准备运行时环境。
您也可以在 Windows 上安装 `WSL2 <https://docs.microsoft.com/zh-cn/windows/wsl/install-win10>`_ 以使用脚本。

.. code:: bash

    # run the environment preparing script.
    ./scripts/prepare_env.sh

接下来，通过 `pip <https://pip.pypa.io/en/stable/>`_ 安装 GraphScope 客户端：

.. code:: shell

    pip install graphscope

或者，您可以通过源码安装 GraphScope 客户端:

.. code:: shell

    pip install 'git+https://github.com/alibaba/GraphScope.git'

或者，您可以通过如下命令直接安装 :code:`.wheel` 包:

.. code:: shell

    pip install graphscope-0.1.macosx-10.14-x86_64.tar.gz


如果您想从源代码构建软件包，请使用 git 从 `alibaba/graphscope <https://github.com/alibaba/GraphScope.git>`_ 下载最新版本的代码:

.. code:: shell

    git clone git@github.com:alibaba/GraphScope.git
    git submodule update --init
    cd python

源码的安装命令如下:

.. code:: shell

    pip install -U -r requirements.txt
    python setup.py install
