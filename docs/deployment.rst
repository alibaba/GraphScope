Deployment
============

The engines of GraphScope are distributed as a docker image.
The `graphscope` python client will pull the image if they are not present.
If you run GraphScope on a k8s cluster, make sure the cluster is able
to access the public registry.

A session encapsulates the control and state of the GraphScope engines.
It serves as the entrance in the python client to GraphScope. A session
allows users to deploy and connect GraphScope on a k8s cluster.

.. code:: python

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


Deploy with AWS/Aliyun
----------------------------

In addition to local cluster setup script, we also provide a interactive script to set up a Kubernetes cluster on AWS or Aliyun. The script would output a kube config file of the Kubernetes cluster.
You can use the script as follows or use `./script/launch_cluster.py --help` to get the useage.

* AWS
.. code:: shell

    pip3 install click PyYAML boto3
    ./scripts/launch_cluster.py --type aws --id your_access_key_id --secret your_access_key_secret --region your_region_name --output kube_config_path

* Aliyun
.. code:: shell

    pip3 install click PyYAML alibabacloud_cs20151215 alibabacloud_ecs20140526 alibabacloud_vpc20160428
    ./scripts/launch_cluster.py --type aliyun --id your_access_key_id --secret your_access_key_secret --region your_region_id --output kube_config_path
