Frequently Asked Questions
==========================

If you don't find an answer to your question here, feel free to file a `Issues`_ or post it to `Discussions`_.

1. What are the minimum resources and system requirements required to run GraphScope?

    To use GraphScope Python interface, **Python >= 3.7** and **pip >= 19.0** is required.
    GraphScope engine can be deployed in standalone mode or distributed mode. For standalone deployment, the mininum requirement is  **4 cores CPU** and **8G memory**.

    GraphScope is tested and supported on the following systems:

    - CentOS 7+
    - Ubuntu 18.04+
    - MacOS 10.15+

    For distributed depolyment, a cluster managed by Kubernetes is required. GraphScope has been tested on
    Kubernetes **version >= v1.12.0+**.


2. Is Kubernetes an enssential to use GraphScope?

    No. GraphScope supports run in standalone mode on a single machine. GraphScope pre-compiled package is distributed as a python package and can be easily installed with `pip`: `pip3 install graphscope`.


3. How to debug or get detailed information when using GraphScope?

    By default, GraphScope is usually running in a silent mode following the convention of Python applications.
    To enable verbose logging, turn on it by this:

    .. code:: python

       graphscope.set_option(show_log=True)

    If you are running GraphScope in k8s, you can use `kubectl describe/logs <https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands>`_ to check the log/status of the cluster. If the disk storage is accessible(on local or via Pods), you may also find logs in `/tmp/gs/runtime/logs`.


4. Why I find more Pods than expected with command `kubectl get pod`?

    For the failed Pods, you may need to delete them manually by `kubectl delete pod <pod-names>`
    This case is observed when using GraphScope with helm. If users did not correctly set the `role` and `rolebinding`, the command `helm uninstall <release-name>` may not correctly recycle allocated resources. More details please refer to `Helm Support <https://artifacthub.io/packages/helm/graphscope/graphscope>`_.


5. Is GraphScope a graph database?

    GraphScope is not a graph database, however there is a persistent storage component that can serve as database inside GraphScope called `graphscope-store <https://graphscope.io/docs/persistent_graph_store.html>`_.


6. What's the compatibility of Gremlin in GraphScope?

    GraphScope supports most querying operators in Gremlin. You may check the compatibility in this `link <https://graphscope.io/docs/interactive_engine.html#unsupported-features>`_.


7. The system seems get stuck, what are the possible reasons?

    If GraphScope seems to get stuck, the possible cause might be:

    - In the session launching stage, the most cases are waiting for Pods ready. The time consuming may be caused by a poor network connection during pulling image, or by failing to acquire the requested resources to launch a session.
    - In the graph loading stage, it is time consuming to load and build a large graph.
    - When running a user-defined or built-in analytical algorithm, it may takes time to compile the algorithm over the loaded graph.

8. Why `No such file or directory` error when loading graph?

    This mostly occur when you are deploying GraphScope in a Kubernetes cluster, the file must be visible to the ``engnine`` Pod of GraphScope. You may need to mount a volume to the Pods or use cloud storage providers.

    Specifically, if your cluster is deployed with `kind <https://kind.sigs.k8s.io>`_, you may need to setup `extra-mounts <https://kind.sigs.k8s.io/docs/user/configuration/#extra-mounts>`_ to mount your local directory to kind nodes.

9. What's the relationship between ``k8s_vineyard_mem``, ``vineyard_shared_mem`` and ``k8s_engine_mem``?

    - ``k8s_vineyard_mem``: The memory allocated for the vineyard container.  It stores the metadata of blobs managed by vineyard, such as the shape, id, name, and so forth. As the metadata would be much smaller than datasets, the default configuration is sufficient in most cases. It's equivalent to ``vineyard.resources.memory.requests`` and ``vineyard.resources.memory.limits`` in graphscope helm charts.

    - ``vineyard_shared_mem``: The memory where the data would be loaded in. Its value needs to be adjusted according to the size of the datasets. We found that setting the value to 5 times the size of the datasets on disk is usually a reasonable value. It's equivalent to ``vineyard.shared_mem`` in graphscope helm charts.

    -  ``k8s_engine_mem``: The memory of the engine pods, can just be set equal to the value of ``vineyard_shared_mem``. Equivalent to ``engines.resources.memory.requests`` and ``engines.resources.memory.limits`` in graphscope helm charts.

10. Failed to install GraphScope on Apple M1 with python3.8?

    - Compile ``grpcio`` failed: You can try to use ``openssl`` from system by ``export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=True``. See more details in `grpc issue <https://github.com/grpc/grpc/issues/25082>`_.

    - compile ``scipy`` failed: You can follow `this <https://stackoverflow.com/questions/65745683/how-to-install-scipy-on-apple-silicon-arm-m1>`_ to build scipy from source or try ``pip3 install --pre -i https://pypi.anaconda.org/scipy-wheels-nightly/simple scipy`` to workaround this problem.

11. How to resolve the ``Permission denied`` error when allocating PV on NFS volumes?

    - ENV: Use helm to install graphscope-store, NFS to supply PV.

    - Appearance: Pod ``graphscope-store-kafka-0`` , ``graphscope-store-zookeeper-0`` reports ``CrashLoopBackOff`` status.

    - Check: First use ``kubectl logs graphscope-store-zookeeper-0`` to check log. If the log shows ``mkdir: cannot create directory '/bitnami/zookeeper/data': Permission denied``.

    - Reason: Normaly, the permission of NFS directories we created is ``root 755`` (depends on your sepcify environment), but the default user of graphscope-store is ``graphscope(1001)``, so these pods have no permission to write on NFS.

    - Solution: There are two slutions to solve this.

        The brutal one is using ``chmod 777`` on all related PV directories, this is efficient but not recommended in production environment.

        The elegant one is creating ``graphscope`` user and user group first, and then grant the access permission on ``graphscope`` to the related NFS directories.

12. why ``Timeout Exception`` raised during launching GraphScope instance on kubernetes cluster?

    It will take a few minutes for pulling image during the first time for launching GraphScope instance. Thus, the ``Timeout Exception`` may caused by a poor network connection.
    You can increase the value of ``timeout_seconds`` parameter as your expectation by ``graphscope.set_option(timeout_seconds=600))``.


**I do have many other questions...**

    Please feel free to contact us. You may reach us by `Issues`_, ask questions in `Discussions`_, or drop a message in `Slack`_ or `DingTalk`_. We are happy to answer your questions responsively.

.. _Issues: https://github.com/alibaba/GraphScope/issues/new/choose
.. _Discussions: https://github.com/alibaba/GraphScope/discussions
.. _Slack: http://slack.graphscope.io
.. _DingTalk: https://h5.dingtalk.com/circle/healthCheckin.html?dtaction=os&corpId=ding82073ee2a22b2f86748126f6422b5d02&109d1=d3892&cbdbhh=qwertyuiop
