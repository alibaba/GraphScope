Frequently Asked Questions
==========================

Below are some questions which are frequently by our end users. If the following sections still doesn’t answer your question, feel free to open an `open an issue <https://github.com/alibaba/GraphScope/issues/new/choose>`_ or `post it to discussions <https://github.com/alibaba/GraphScope/discussions>`_.

*1. What are the minimum resources and system requirements required to run GraphScope?*

    To use GraphScope Python interface, **Python >= 3.6** and **pip >= 19.0** is required.
    GraphScope engine can be deployed in standalone mode or distributed mode. For standalone deployment, a physical machine with at least **4 cores CPU** and **8G memory** is required. GraphScope is tested and supported on the following systems:

    - CentOS7+
    - Ubuntu18+

    For distributed depolyment, a cluster managed by Kubernetes is required. GraphScope has tested on
    k8s **version >= v1.12.0+**.


*2. Is Kubernetes an enssential to run GraphScope?*

    No. GraphScope supports `build and run on local <https://graphscope.io/docs/deployment.html#deployment-on-local>`_ in a single machine. 
    However, GraphScope depends on many third-party libraries and projects. To make our life easier, we suggest you build and run GraphScope on k8s with our provided dev-image and release-image. If you don't have a kubernetes cluster, you may use tools like `kind <https://kind.sigs.k8s.io/>`_ to setup a local cluster to use the images for trying graphscope.


*3. How to debug or get detailed information when run GraphScope?*

    By default, GraphScope is usually running in a silent mode following the convention of Python applications.
    To enable verbose logging, turn on it by this:

    .. code:: python
       
       graphscope.set_option(show_log=True)

    If you are running GraphScope in k8s, you can use `kubectl describe/logs <https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands>`_ to check the log/status of the cluster. If the disk space is accessible(on local or via pods), you may also find logs in `/tmp/graphscope/runtime/logs`.


*4. Why I find more pods than expected with command `kubectl get pod`?*

    For the failed pods, you may need to delete them manually.
    This case is observed when using GraphScope with helm. If users did not correctly set the `role` and `rolebinding`, the command `helm uninstall GraphScope` may not correctly recycle allocated resources. More details please refer to `Helm Support <https://artifacthub.io/packages/helm/graphscope/graphscope>`_.


*5. Is GraphScope a graph database?*

    No, GraphScope is not a graph database.  It cannot provide transactions on graph.
    Instead, it provides an efficient "immutable" in-memory store for fast queries and analysis, and a persistent store(service) for updates on graphs. Both are scale very well - you can launch a larger session from your python notebook to handle a bigger graph or run a complex algorithm.


*6. What's the compatibility of Gremlin in GraphScope?*

    GraphScope supports most querying operators in Gremlin. You may check the compatibility in this `link <https://graphscope.io/docs/interactive_engine.html#unsupported-features>`_.


*7. The system seems get stuck, what are the possible reasons?*

    If GraphScope seems to get stuck, the possible cause might be:

    - In the session launching stage, the most cases are waiting for pods ready. The time consuming may be caused by a poor network connection during pulling image, or caused by the resources cannot meet the need to launch a session.
    - In the graph loading stage, it is time consuming to load and build a large graph.
    - When running a user-defined or built-in analytical algorithm, it takes time to compile/distribute the algorithm over the loaded graph.


*I do have many other questions...*

    Please feel free to contact us. You may reach us by `submitting an issue(prefered) <https://github.com/alibaba/GraphScope/issues/new/choose>`_, ask questions in `Discussions <https://github.com/alibaba/GraphScope/discussions>`_, or drop a message in `Slack <http://slack.graphscope.io>`_ or `DingTalk <https://h5.dingtalk.com/circle/healthCheckin.html?dtaction=os&corpId=ding82073ee2a22b2f86748126f6422b5d02&109d1=d3892&cbdbhh=qwertyuiop>`_. We are happy to answer your questions responsively.
