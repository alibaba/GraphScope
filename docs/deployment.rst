Deployment
===========

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

Sometimes users may want to use their dataset on the local disk, in this case, we provide options to mount a
host directory to the cluster.

Assume we want to mount `~/test_data` in the host machine to `/testingdata` in pods, we can define
a dict as follows, then pass it as `k8s_volumes` in session constructor.

Note that the host path is relative to the kubernetes node, that is, if you have a cluster created by `kind`, then you need to copy that directory to the kind node, or mount that path to kind node.
See more details `here <https://kind.sigs.k8s.io/docs/user/configuration/#extra-mounts>`_.


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

A cluster on k8s contains a pod running an etcd container for meta-data syncing, a
pod running the Coordinator, and a replica set of GraphScope engines.

The Coordinator in GraphScope is the endpoint of the backend. It
manages the connections from python client via grpc,
and takes responsibility for applying or releasing the pods for interactive, analytical
and learning engines.

The image URIs for the engines are configurable, see more details in :ref:`Session`.


Deployment with Helm
--------------------

Get Repo Info

.. code:: bash

    $ helm repo add graphscope https://graphscope.oss-cn-beijing.aliyuncs.com/charts/
    $ helm repo update

Install Chart

.. code:: bash

    # Helm 3
    $ helm install [RELEASE_NAME] graphscope/graphscope

    # Helm 2
    $ helm install --name [RELEASE_NAME] graphscope/graphscope

Check Service Availability

.. code:: bash

    # Helm 3 or 2
    $ helm test [RELEASE_NAME]

Find `more details <https://github.com/alibaba/GraphScope/blob/main/charts/graphscope/README.md>`_ on how to connect a pre-launched service in python client.


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


Deployment on local
----------------------
We provide script to install dependencies of GraphScope on
Ubuntu 20.04+ or MacOS.

* install development independencies of GraphScope
.. code:: shell

    ./scripts/install_deps.sh --dev

* then you can build and deploy GraphScope locally
.. code:: shell

    source ~/.graphscope_env
    make graphscope
