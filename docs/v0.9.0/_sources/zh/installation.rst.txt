安装
====

GraphScope 目前支持的平台如下:

- Python 3.6 - 3.9
- Ubuntu 18.04 or later
- CentOS 7 or later
- macOS 11.2.1 (Big Sur) or later (Apple M1 is not support yet!)


单机环境下安装
------------

GraphScope 以 `Python 程序包 <https://pypi.org/project/graphscope>`_ 的形式发布, 可直接通过 `pip <https://pip.pypa.io/en/stable/>`_ 安装。

**注意: 需要满足 pip 版本 > 19.0 (or > 20.3 for macOS)**

.. code:: bash

    # Requires the latest pip
    pip3 install --upgrade pip

    # Current stable release
    pip3 install --upgrade graphscope


基于 Kubernetes 安装 GraphScope 客户端
------------------------------------

我们可以按照本文档的以下步骤部署一个本地 Kubernetes 集群，并加载预编译好的镜像，在 Kubernetes 环境下运行 GraphScope。

首先需要预先安装以下依赖。

- Python 3.6 - 3.9 (with pip)
- Local Kubernetes cluster set-up tool (e.g. `Kind <https://kind.sigs.k8s.io>`_)

对于 Windows 和 MacOS 的用户，可通过官方文档来安装上述依赖, 并在 Docker 中开启 Kubernetes 功能。
对于 Ubuntu/CentOS Linux 发行版用户，我们提供了脚本来准备运行时环境。
您也可以在 Windows 上安装 `WSL2 <https://docs.microsoft.com/zh-cn/windows/wsl/install-win10>`_ 以使用脚本。

.. code:: bash

    # run the environment preparing script.
    ./scripts/install_deps.sh --k8s

接下来，我们只需要通过 `pip <https://pip.pypa.io/en/stable/>`_ 安装 GraphScope 客户端：

.. code:: bash

    # Requires the latest pip
    pip3 install --upgrade pip

    # Current stable release
    pip3 install --upgrade graphscope-client