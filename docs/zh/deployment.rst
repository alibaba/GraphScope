部署
====

GraphScope以docker镜像的方式发布引擎等组件，默认情况下，如果worker所在的机器上不存在该镜像，则会拉取最新的镜像。
因此，请确保您的集群可以访问公共镜像仓库。

Session做为GraphScope在python客户端的入口，它封装、管理着GraphScope引擎的各种组件，并允许用户在Kubernetes集群上
部署、连接、以及操作GraphScope引擎。

.. code:: ipython

    import graphscope

    sess = graphscope.session()

如上述代码所示，用户可以很容易的通过session来拉起一个GraphScooe实例。

Kubernetes上的一个GraphScope实例包含: 一个运行etcd容器的pod，负责同步元信息；一个运行Coordinator容器的pod；
以及一组运行Engines容器的ReplicaSet对象。

Coordinator做为GraphScope后端服务的入口地址，它通过grpc接收来自python客户端的连接，并管理着图分析引擎(GAE)、
图查询引擎(GIE)，图学习引擎(GLE)的生命周期。

您可以根据需求配置引擎镜像地址等参数，可在 :ref:`Session` 中参阅更多的详细信息。

