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

