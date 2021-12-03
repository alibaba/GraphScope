部署
====

GraphScope 以 docker 镜像的方式分发引擎等组件。默认情况下，如果运行 GraphScope 的集群机器上不存在该镜像，
则会拉取对应版本的最新镜像。因此，请确保您的集群可以访问公共镜像仓库。

会话（:ref:`Session`) 作为 GraphScope 在 Python 客户端，它封装、管理着 GraphScope 引擎的各种组件，并
部署、连接、操作用户在 Kubernetes 集群上的 GraphScope 引擎。

.. code:: python

    import graphscope

    sess = graphscope.session()

如上述代码所示，用户可以很容易的通过会话来拉起一个 GraphScope 实例。

用户有时需要使用在本机磁盘上的数据，我们提供了选项供用户将本机的目录挂载到集群上。

假定我们要将本机的 `~/test_data` 的路径挂载到 Pod 中的 `/testingdata` 路径，我们可以定义如下一个字典，然后
将其通过`k8s_volumes` 的参数传给会话的构造函数。

注意这里的本机路径是相对于 k8s 节点的概念，也就是说，如果你是通过 `Kind` 创建的集群，那么你需要将这个目录拷贝到
`Kind` 的虚拟主机的相应路径上，或者在配置中将路径提前挂载到虚拟主机上去。在 `这里 <https://kind.sigs.k8s.io/docs/user/configuration/#extra-mounts>`_ 查看更多细节。

.. code:: python


    import os
    import graphscope

    k8s_volumes = {
        "data": {
            "type": "hostPath",
            "field": {
                "path": os.path.expanduser("~/test_data/"),
                "type": "Directory"
            },
            "mounts": {
            "mountPath": "/testingdata"
            }
        }
    }

    sess = graphscope.session(k8s_volumes=k8s_volumes)

Kubernetes 上的一个 GraphScope 实例包含: 一个运行 etcd 的 pod，负责元信息的同步；一个运行 Coordinator 的 pod，
负责对 GraphScope 引擎容器的管理；以及一组运行 GraphScope 引擎容器的 ReplicaSet 对象。

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
我们提供了一个可在本地安装GraphScope相关依赖的脚本，该脚本可以运行在 Ubuntu 20.04+ 或 MacOS 11.2+ 平台上, 主要的用法如下：
你可以通过 `./scripts/install_deps.sh -h` 获取更详细的帮助信息。

* 安装 GraphScope 开发相关依赖
.. code:: shell

    ./scripts/install_deps.sh --dev

* 本地部署 GraphScope
.. code:: shell

    source ~/.graphscope_env
    make graphscope
