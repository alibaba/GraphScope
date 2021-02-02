Deployment
============

The engines of GraphScope are distributed as a docker image. 
The `graphscope` python client will pull the image if they are not present.
If you run GraphScope on a k8s cluster, make sure the cluster is able 
to access the public registry.

A session encapsulates the control and state of the GraphScope engines.
It serves as the entrance in the python client to GraphScope. A session
allows users to deploy and connect GraphScope on a k8s cluster.

.. code:: ipython

    import graphscope

    sess = graphscope.session()


As shown above, a session can easily launch a cluster on k8s.

A cluster on k8s contains a pod running an etcd container for meta-data syncing, a
pod running the Coordinator, and a replica set of GraphScope engines.

The Coordinator in GraphScope is the endpoint of the backend. It
manages the connections from python client via grpc, 
and takes responsibility for applying or releasing the pods for interactive, analytical
and learning engines.

The image URIs for the engines are configurable, see more details in :ref:`Session`.


