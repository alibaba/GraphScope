常见问题
========

我们收集了用户使用 GraphScope 过程中的常见问题，如果在下面的列表中仍然无法找到您遇到的问题的答案，可通过提交 `Issues`_ 或者在 `Discussions`_ 与我们联系。


**1. 运行 GraphScope 系统需要的最小资源是多少？**

    在通过 Python 客户端使用 GraphScope 过程中，需要的最低 Python 版本是3.6+，最低pip版本是 19.0+，GraphScope 系统支持以单机或分布式的方式部署，单机情况下，资源需求 CPU >= 4 核，内存 >= 8 G；
    GraphScope 在支持以下环境中运行

    - CentOS 7+
    - Ubuntu 18.04+
    - MacOS 10.15+

    对于分布式部署，需要用户拥有一个 Kubernetes 集群，GraphScope 在 **Kubernetes version >= v1.12.0+** 的环境上测试通过。


**2. GraphScope 是否强依赖 Kubernetes?**

    GraphScope 支持在单机模式安装运行。 GraphScope 的预编译包以 Python wheel 的形式分发，可以用 ``pip`` 快速安装：`pip3 install graphscope`。


**3. 如何如查看 GraphScope 的运行时信息？**

    默认情况下，GraphScope 运行不打印日志信息，你可以通过 ``show_log`` 参数开启日志输出。

    .. code:: python
       
       graphscope.set_option(show_log=True)


    如果您的GraphScope运行在k8s集群上，可以使用 `kubectl describe/logs <https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands>`_ 来查看系统的当前状态，同时如果你可以访问 Pod 的磁盘，也可在 `/tmp/gs/runtime/logs` 目录下查看运行时日志。


**4. 为什么在使用 `kubectl get pod` 命令时，发现了一些多余的 Pod?**

    对于其中一些失败的 Pod，只能通过手动执行命令如 `kubectl delete pod <pod_name>` 进行删除。
    通常情况下，该问题出现在使用 Helm 部署 GraphScope 时，GraphScope 依赖一些权限来删除运行时的资源，如果用户没有正确设置 ``role`` 和 ``rolebinding`` 等权限，`helm uninstall <release-name>` 可能不能正确回收分配的资源。详细细节可以查看 `Helm Support <https://artifacthub.io/packages/helm/graphscope/graphscope>`_


**5. GraphScope 是图数据库吗？**

    GraphScope 并不是一个图数据库，但其包含一个持久图存储的组件 `graphscope-store <https://graphscope.io/docs/persistent_graph_store.html>`_ 可以被用作数据库。


**6. GraphScope 在 Gremlin 上的兼容性如何？**

    目前，GraphScope支持Gremlin语言中的大部分查询算子，可通过 `该文档 <https://graphscope.io/docs/interactive_engine.html#unsupported-features>`_ 查看详细的支持信息。


**7. GraphScope 看起来在运行过程中卡住了？**

    如果 GraphScope 看起来像卡住，可能的原因有：

    - 在会话的拉起阶段，如果当前网络下载镜像过慢，或当前集群资源无法满足请求的资源时，会造成卡住的现象。
    - 载图阶段，可能会由于数据量过大造成短暂的卡住现象。
    - 在执行图算法分析阶段，或者使用用户自定义的算法，编译构建应用的库时会花费一些时间。

**8. 为什么载图时报找不到文件的错误？**

    这通常发生在以集群方式部署运行 GraphScope 时，数据文件必须要对 ``engine`` Pod 可见。你也许需要挂载磁盘或者使用云存储提供商的服务。

    如果你的集群是使用 `kind <https://kind.sigs.k8s.io>`_ 部署的, 你应该需要设置 `extra-mounts <https://kind.sigs.k8s.io/docs/user/configuration/#extra-mounts>`_ 来把本机目录挂载到 Kind 虚拟的 Node 中

**其他问题**

    您可以提交 `Issues`_ 或者在 `Discussions`_ 提出你的问题，同时，您也可以使用 `Slack`_ 或 `DingTalk`_ 与我们联系。

.. _Issues: https://github.com/alibaba/GraphScope/issues/new/choose
.. _Discussions: https://github.com/alibaba/GraphScope/discussions
.. _Slack: http://slack.graphscope.io
.. _DingTalk: https://h5.dingtalk.com/circle/healthCheckin.html?dtaction=os&corpId=ding82073ee2a22b2f86748126f6422b5d02&109d1=d3892&cbdbhh=qwertyuiop