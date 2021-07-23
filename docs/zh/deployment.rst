部署
====

GraphScope 以 docker 镜像的方式分发引擎等组件。
默认情况下，如果运行 GraphScope 的集群机器上不存在该镜像，则会拉取对应版本的最新镜像。
因此，请确保您的集群可以访问公共镜像仓库。

会话（:ref:`Session`) 作为 GraphScope 在 Python 客户端，它封装、管理着 GraphScope 引擎的各种组件，
并部署、连接、操作用户在 Kubernetes 集群上的 GraphScope 引擎。

.. code:: python

    import graphscope

    sess = graphscope.session()

如上述代码所示，用户可以很容易的通过会话来拉起一个 GraphScope 实例。

Kubernetes 上的一个 GraphScope 实例包含: 一个运行 etcd 的 pod，负责元信息的同步；一个运行 Coordinator 的 pod，负责对 GraphScope 引擎容器的管理；
以及一组运行 GraphScope 引擎容器的 ReplicaSet 对象。

Coordinator 作为 GraphScope 后端服务的入口，通过 grpc 接收来自 Python 客户端的连接和命令，并管理着图分析引擎(GAE)、
图查询引擎(GIE)，图学习引擎(GLE)的生命周期。

您可以根据需求配置引擎镜像地址等参数，请在 :ref:`Session` 中参阅更多的详细信息。


使用Helm部署GraphScope
--------------------

获取并添加仓库信息

.. code:: bash

    $ helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/charts/
    $ helm repo update

安装

.. code:: bash

    # Helm 3
    $ helm install [RELEASE_NAME] graphscope/graphscope

    # Helm 2
    $ helm install --name [RELEASE_NAME] graphscope/graphscope

检测GraphScope服务可用性

.. code:: bash

    # Helm 3 or 2
    $ helm test [RELEASE_NAME]

参考 `该链接 <https://github.com/alibaba/GraphScope/blob/main/charts/graphscope/README.md>`_ 以通过python客户端连接到预启动的GraphScope服务。


在AWS/阿里云上部署集群
------------------------
我们提供了一个可在AWS或阿里云上创建 Kubernetes 集群的交互式脚本。这一脚本可以帮助用户使用已有的集群或创建新的 Kubernetes 集群，然后输出集群的配置文件。
用法如下。你也可以通过 `./scripts/launch_cluster.py --help` 获得更详细的帮助信息。

* AWS
.. code:: shell

    pip3 install click PyYAML boto3
    ./scripts/launch_cluster.py --type aws --id your_access_key_id --secret your_access_key_secret --region your_region_name --output kube_config_path

* Aliyun
.. code:: shell

    pip3 install click PyYAML alibabacloud_cs20151215 alibabacloud_ecs20140526 alibabacloud_vpc20160428
    ./scripts/launch_cluster.py --type aliyun --id your_access_key_id --secret your_access_key_secret --region your_region_id --output kube_config_path

本地部署GraphScope
----------------------
我们提供了一个可在本地安装GraphScope相关依赖以及部署GraphScope的脚本，这一脚本可以运行在
Ubuntu 20.04+或MacOS平台上, 主要的用法如下。你可以通过 `./scripts/deploy_local.sh -h`
获取更详细的帮助信息。

* 使用`deploy_local.sh`安装GraphScope相关依赖
.. code:: shell

    ./scripts/deploy_local.sh install_deps
    source ~/.graphscope_env

* 使用`deploy_local.sh`本地部署GraphScope
.. code:: shell

    ./scripts/deploy_local.sh build_and_deploy
    export GRAPHSCOPE_HOME=/usr/local