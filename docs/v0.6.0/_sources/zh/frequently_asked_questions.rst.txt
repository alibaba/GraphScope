常见问题
========

我们收集了用户使用GraphScope过程中的常见问题，如果在下面的列表中仍然无法找到您遇到的问题的答案，可通过 `issue的方式 <https://github.com/alibaba/GraphScope/issues/new/choose>`_ 或 `在讨论区 <https://github.com/alibaba/GraphScope/discussions>`_ 与我们联系。


*1. 运行GraphScope系统需要的最小资源是多少？*

    在通过Python客户端使用GraphScope过程中，需要的最低Python版本是3.6+，最低pip版本是19.0+，GraphScope系统支持单机和分布式的方式部署，单机情况下，要求CPU至少4核，内存至少8G；
    同时，GraphScope系统支持CentOS7+、Ubuntu18+、以及Kubernetes v1.12.0+


*2. GraphScope是否强依赖Kubernetes?*

    GraphScope不强依赖Kubernetes，同时也支持在 `本地环境环境下部署 <https://graphscope.io/docs/deployment.html#deployment-on-local>`_
    然而，由于Graphscope依赖众多第三方的库和项目，为了节省你的时间，我们建议基于我们提供的镜像构建并运行GraphScope，如果你没有一个运行的Kubernetes集群，可以使用官方提供的 `kind <https://kind.sigs.k8s.io/>`_ 在本地快速搭建一个虚拟集群。


*3. 如何如查看GraphScope的运行时信息？*

    默认情况下，GraphScope运行不打印任何日志信息，你可以通过`show_log`参数开启日志输出。

    .. code:: python
       
       graphscope.set_option(show_log=True)


    如果您的GraphScope运行在k8s集群上，可以使用 `kubectl describe/logs <https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands>`_ 来查看系统的当前状态，同时如果你挂载可本地磁盘，也可在 `/tmp/graphscope/runtime/logs` 目录下查看运行时日志。


*4. 为什么在使用 `kubectl get pod`命令时，发现了一些多余的pod?*

    对于其中一些失败的pod，只能通过手动命令如 `kubectl delete pod <pod_name>` 进行删除。
    通常情况下，该问题出现在使用Helm部署GraphScope时，目前，GraphScope依赖一些权限来删除运行时的资源，详细细节可以查看 `Helm Support <https://artifacthub.io/packages/helm/graphscope/graphscope>`_


*5. GraphScope是图数据库吗？*

    GraphScope并不是一个图数据库，其无法提供图上的一些事物性操作。GraphScope 将图分布式地载入内存作为不可变数据，来支持对该图的查询和分析操作。


*6. GraphScope在Gremlin上的兼容性如何？*

    目前，GraphScope支持Gremlin语言中的大部分查询算子，可通过 `该文档 <https://graphscope.io/docs/interactive_engine.html#unsupported-features>`_ 查看详细的支持信息。


*7. GraphScope运行过程中常见的卡住原因*

    - 在session拉起阶段，如果当前网络下载镜像过慢，或当前集群资源无法满足pod的拉起时，会造成一定的卡住现象。
    - 载图阶段，可能会由于数据量过大造成短暂的卡住现象。
    - 在执行图算法分析阶段，如果当前系统资源紧张，编译构建app时会花费一些时间。


*8. 其他问题*

    您可以通过 `issue的方式 <https://github.com/alibaba/GraphScope/issues/new/choose>`_ 或 `在讨论区 <https://github.com/alibaba/GraphScope/discussions>`_ 提出你的问题，同时，您也可以使用 `Slack <http://slack.graphscope.io>`_ 或 `DingTalk <https://h5.dingtalk.com/circle/healthCheckin.html?dtaction=os&corpId=ding82073ee2a22b2f86748126f6422b5d02&109d1=d3892&cbdbhh=qwertyuiop>`_ 与我们联系。
