#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import atexit
import logging
import multiprocessing
import os
import random
import re
import subprocess
import sys
import time

from kubernetes import client as kube_client
from kubernetes import watch as kube_watch
from kubernetes.client import CoreV1Api
from kubernetes.client.rest import ApiException as K8SApiException

from graphscope.deploy.kubernetes.resource_builder import ClusterRoleBindingBuilder
from graphscope.deploy.kubernetes.resource_builder import ClusterRoleBuilder
from graphscope.deploy.kubernetes.resource_builder import GSCoordinatorBuilder
from graphscope.deploy.kubernetes.resource_builder import NamespaceBuilder
from graphscope.deploy.kubernetes.resource_builder import RoleBindingBuilder
from graphscope.deploy.kubernetes.resource_builder import RoleBuilder
from graphscope.deploy.kubernetes.resource_builder import ServiceBuilder
from graphscope.deploy.kubernetes.utils import delete_kubernetes_object
from graphscope.deploy.kubernetes.utils import is_minikube_cluster
from graphscope.framework.errors import K8sError
from graphscope.framework.utils import random_string

logger = logging.getLogger("graphscope")


class KubernetesCluster(object):
    """Class for setting up GraphScope instance on kubernetes cluster.

    Args:
        api_client: ApiClient
            An Kubernetes ApiClient object, initialized with the client args.

        namespace: str, optional
            Kubernetes namespace. Defaults to None.

        minikube_vm_driver: bool, optional
            True if minikube cluster :code:`--vm-driver` is not :code:`None`

        num_workers: int
            Number of workers to launch graphscope engine.

        log_level: str
            Verbosity of logging in graphscope engine.

        gs_image: str
            GraphScope engine image.

        etcd_image: str
            Etcd image.

        gie_graph_manager_image: str
            Graph manager image for interactive engine.

        zookeeper_image: str
            Zookeeper image for interactive engine.

        image_pull_policy: str, optional
            Kubernetes image pull policy. Defaults to IfNotPresent.

        image_pull_secrets: list of str, optional
            A list of secret name used to pulling image. Defaults to None.

        vineyard_cpu: float
            Minimum number of CPU cores request for vineyard container.

        vineyard_mem: str
            Minimum number of memory request for vineyard container.

        vineyard_shared_mem: str
            Initial size of vineyard shared memory.

        engine_cpu: float
            Minimum number of CPU cores request for engine container.

        engine_mem: str
            Minimum number of memory request for engine container.

        coordinator_cpu: float
            Minimum number of CPU cores request for coordinator pod.

        coordinator_mem: str
            Minimum number of memory request for coordinator pod.

        timeout_seconds: int
            Timeout when setting up graphscope instance on kubernetes cluster.

        waiting_for_delete: bool
            Waiting for service delete or not.
    """

    _coordinator_builder_cls = GSCoordinatorBuilder

    _coordinator_name_prefix = "coordinator-"
    _coordinator_service_name_prefix = "coordinator-service-"
    _coordinator_container_name = "coordinator"

    _role_name = "gs-reader"
    _role_binding_name = "gs-reader-binding"
    _cluster_role_name_prefix = "gs-cluster-reader-"
    _cluster_role_binding_name_prefix = "gs-cluster-reader-binding-"

    _random_coordinator_service_port = random.randint(59001, 60000)

    _url_pattern = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"  # noqa: E501
    _endpoint_pattern = r"(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*"

    def __init__(
        self,
        api_client=None,
        namespace=None,
        minikube_vm_driver=None,
        num_workers=None,
        log_level=None,
        gs_image=None,
        etcd_image=None,
        gie_graph_manager_image=None,
        zookeeper_image=None,
        image_pull_policy=None,
        image_pull_secrets=None,
        vineyard_cpu=None,
        vineyard_mem=None,
        vineyard_shared_mem=None,
        engine_cpu=None,
        engine_mem=None,
        coordinator_cpu=None,
        coordinator_mem=None,
        timeout_seconds=None,
        waiting_for_delete=None,
        **kwargs
    ):
        self._api_client = api_client
        self._core_api = kube_client.CoreV1Api(api_client)
        self._app_api = kube_client.AppsV1Api(api_client)
        self._rbac_api = kube_client.RbacAuthorizationV1Api(api_client)

        self._namespace = namespace
        self._minikube_vm_driver = minikube_vm_driver
        self._gs_image = gs_image
        self._num_workers = num_workers
        self._log_level = log_level
        self._etcd_image = etcd_image
        self._gie_graph_manager_image = gie_graph_manager_image
        self._zookeeper_image = zookeeper_image

        self._image_pull_policy = image_pull_policy
        self._image_pull_secrets = image_pull_secrets
        if self._image_pull_secrets is None:
            self._image_pull_secrets = []
        elif not isinstance(self._image_pull_secrets, list):
            self._image_pull_secrets = [self._image_pull_secrets]

        self._vineyard_cpu = vineyard_cpu
        self._vineyard_mem = vineyard_mem
        self._vineyard_shared_mem = vineyard_shared_mem
        self._engine_cpu = engine_cpu
        self._engine_mem = engine_mem
        self._waiting_for_delete = waiting_for_delete

        self._cluster_role_name = ""
        self._cluster_role_binding_name = ""

        # all resource created inside namsapce
        self._resource_object = []

        self._coordinator_name = self._coordinator_name_prefix + random_string(6)
        self._coordinator_service_name = (
            self._coordinator_service_name_prefix + random_string(6)
        )
        self._coordinator_cpu = coordinator_cpu
        self._coordinator_mem = coordinator_mem
        # environment variable
        self._coordinator_envs = kwargs.pop("coordinator_envs", dict())

        self._closed = False
        self._timeout_seconds = timeout_seconds

        self._delete_namespace = False

    def __del__(self):
        self.stop()

    def get_namespace(self):
        """Get kubernetes namespace which graphscope instance running on.

        Returns:
            str: Kubernetes namespace.
        """
        return self._namespace

    def check_and_set_vineyard_rpc_endpoint(self, engine_config):
        if is_minikube_cluster() and self._minikube_vm_driver:
            engine_config["vineyard_rpc_endpoint"] = self._get_minikube_service(
                self._namespace, engine_config["vineyard_service_name"]
            )

    def _get_free_namespace(self):
        while True:
            namespace = "gs-" + random_string(6)
            try:
                self._core_api.read_namespace(namespace)
            except K8SApiException as e:
                if e.status != 404:
                    raise RuntimeError(str(e))
                return namespace

    def _namespace_exist(self, namespace):
        try:
            self._core_api.read_namespace(namespace)
        except K8SApiException as e:
            if e.status != 404:
                raise RuntimeError(str(e))
            return False
        return True

    def _role_exist(self, namespace, role):
        try:
            self._rbac_api.read_namespaced_role(name=role, namespace=namespace)
        except K8SApiException as e:
            if e.status != 404:
                raise RuntimeError(str(e))
            return False
        return True

    def _cluster_role_exist(self, cluster_role):
        try:
            self._rbac_api.read_cluster_role(name=cluster_role)
        except K8SApiException as e:
            if e.status != 404:
                raise RuntimeError(str(e))
            return False
        return True

    def _role_binding_exist(self, namespace, role_binding):
        try:
            self._rbac_api.read_namespaced_role_binding(
                name=role_binding, namespace=namespace
            )
        except K8SApiException as e:
            if e.status != 404:
                raise RuntimeError(str(e))
            return False
        return True

    def _cluster_role_binding_exist(self, cluster_role_binding):
        try:
            self._rbac_api.read_cluster_role_binding(name=cluster_role_binding)
        except K8SApiException as e:
            if e.status != 404:
                raise RuntimeError(str(e))
            return False
        return True

    def _create_namespace(self):
        if self._namespace is None:
            self._namespace = self._get_free_namespace()
        if not self._namespace_exist(self._namespace):
            self._core_api.create_namespace(NamespaceBuilder(self._namespace).build())
            self._delete_namespace = True

    def _create_role_and_binding(self):
        self._cluster_role_name = self._cluster_role_name_prefix + str(self._namespace)
        self._cluster_role_binding_name = self._cluster_role_binding_name_prefix + str(
            self._namespace
        )
        # create a role and bind to default service account.
        targets = []
        if not self._role_exist(namespace=self._namespace, role=self._role_name):
            role_builer = RoleBuilder(
                name=self._role_name,
                namespace=self._namespace,
                api_groups="apps,",
                resources="configmaps,deployments,endpoints,events,pods,pods/log,pods/exec,pods/status,services,replicasets",
                verbs="create,delete,get,update,watch,list",
            )
            targets.append(
                self._rbac_api.create_namespaced_role(
                    self._namespace, role_builer.build()
                )
            )

        if not self._role_binding_exist(
            namespace=self._namespace, role_binding=self._role_binding_name
        ):
            role_binding_builder = RoleBindingBuilder(
                name=self._role_binding_name,
                namespace=self._namespace,
                role_name=self._role_name,
                service_account_name="default",
            )
            targets.append(
                self._rbac_api.create_namespaced_role_binding(
                    self._namespace, role_binding_builder.build()
                )
            )

        if not self._cluster_role_exist(cluster_role=self._cluster_role_name):
            cluster_role_builder = ClusterRoleBuilder(
                name=self._cluster_role_name,
                api_groups="apps,",
                resources="namespaces",
                verbs="create,delete,get,update,watch,list",
            )
            targets.append(
                self._rbac_api.create_cluster_role(cluster_role_builder.build())
            )

        if not self._cluster_role_binding_exist(
            cluster_role_binding=self._cluster_role_binding_name
        ):
            cluster_role_binding_builder = ClusterRoleBindingBuilder(
                name=self._cluster_role_binding_name,
                namespace=self._namespace,
                cluster_role_name=self._cluster_role_name,
                service_account_name="default",
            )
            targets.append(
                self._rbac_api.create_cluster_role_binding(
                    cluster_role_binding_builder.build()
                )
            )

        self._resource_object.extend(targets)

    def _create_coordinator(self):
        targets = []

        labels = {"name": self._coordinator_name}
        # create coordinator service
        service_builder = ServiceBuilder(
            self._coordinator_service_name,
            service_type="NodePort",
            port=self._random_coordinator_service_port,
            selector=labels,
        )
        targets.append(
            self._core_api.create_namespaced_service(
                self._namespace, service_builder.build()
            )
        )

        time.sleep(1)

        # create coordinator deployment
        coordinator_builder = self._coordinator_builder_cls(
            name=self._coordinator_name,
            labels=labels,
            replicas=1,
            image_pull_policy=self._image_pull_policy,
        )

        for name in self._image_pull_secrets:
            coordinator_builder.add_image_pull_secret(name)

        if "GS_TEST_DIR" in os.environ:
            envs = {
                "PYTHONPATH": "/root/gsa",
                "GS_TEST_DIR": os.environ["GS_TEST_DIR"],
                "PYTHONUNBUFFERED": "TRUE",
            }
        else:
            envs = {"PYTHONPATH": "/root/gsa", "PYTHONUNBUFFERED": "TRUE"}

        coordinator_builder.add_simple_envs(envs)

        coordinator_builder.add_coordinator_container(
            name=self._coordinator_container_name,
            port=self._random_coordinator_service_port,
            num_workers=self._num_workers,
            log_level=self._log_level,
            namespace=self._namespace,
            gs_image=self._gs_image,
            etcd_image=self._etcd_image,
            gie_graph_manager_image=self._gie_graph_manager_image,
            zookeeper_image=self._zookeeper_image,
            image_pull_policy=self._image_pull_policy,
            image_pull_secrets=",".join(self._image_pull_secrets),
            coordinator_name=self._coordinator_name,
            coordinator_cpu=self._coordinator_cpu,
            coordinator_mem=self._coordinator_mem,
            coordinator_service_name=self._coordinator_service_name,
            vineyard_cpu=self._vineyard_cpu,
            vineyard_mem=self._vineyard_mem,
            vineyard_shared_mem=self._vineyard_shared_mem,
            engine_cpu=self._engine_cpu,
            engine_mem=self._engine_mem,
            timeout_seconds=self._timeout_seconds,
            waiting_for_delete=self._waiting_for_delete,
            delete_namespace=self._delete_namespace,
        )

        targets.append(
            self._app_api.create_namespaced_deployment(
                self._namespace, coordinator_builder.build()
            )
        )

        self._resource_object.extend(targets)

    def _create_services(self):
        self._create_coordinator()

    def _waiting_for_services_ready(self):
        start_time = time.time()
        event_messages = []
        while True:
            deployments = self._app_api.list_namespaced_deployment(self._namespace)
            service_available = False
            for deployment in deployments.items:
                if deployment.metadata.name == self._coordinator_name:
                    # replica is 1
                    if deployment.status.available_replicas == 1:
                        # service is ready
                        service_available = True
                        break

                    # check container status
                    selector = ""
                    for k, v in deployment.spec.selector.match_labels.items():
                        selector += k + "=" + v + ","
                    selector = selector[:-1]
                    pods = self._core_api.list_namespaced_pod(
                        namespace=self._namespace, label_selector=selector
                    )
                    for pod in pods.items:
                        # output pod event
                        pod_name = pod.metadata.name
                        field_selector = "involvedObject.name=" + pod_name
                        stream = kube_watch.Watch().stream(
                            self._core_api.list_namespaced_event,
                            self._namespace,
                            field_selector=field_selector,
                            timeout_seconds=1,
                        )
                        for event in stream:
                            msg = "[{}]: {}".format(pod_name, event["object"].message)
                            if msg not in event_messages:
                                event_messages.append(msg)
                                logger.info(msg)
                                if event["object"].reason == "Failed":
                                    raise RuntimeError("Kubernetes event error: ", msg)
                        # check failed
                        if pod.status.container_statuses is not None:
                            for container_status in pod.status.container_statuses:
                                if (
                                    container_status.ready is False
                                    and container_status.restart_count > 0
                                ):
                                    service_available = False
                                    raise RuntimeError("Coordinator pod start failed.")

            if service_available:
                break

            if (
                self._timeout_seconds
                and self._timeout_seconds + start_time < time.time()
            ):
                raise RuntimeError("Coordinator service start timeout.")
            time.sleep(1)
        logger.info("Coordinator service is ready.")

    def _get_minikube_service(self, namespace, service_name):
        def minikube_get_service_url(queue, namespace, service_name):
            try:
                cmd = ["minikube", "service", service_name, "-n", namespace, "--url"]
                process = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
                )
                for line in process.stdout:
                    queue.put(line.decode("utf-8"))
            except Exception as e:
                queue.put(str(e))

        pqueue = multiprocessing.Queue()
        p = multiprocessing.Process(
            target=minikube_get_service_url,
            name="minikube_service",
            args=(
                pqueue,
                namespace,
                service_name,
            ),
        )
        p.start()
        # 10 seconds is enough
        p.join(10)
        if p.is_alive():
            p.terminate()
            p.join()
        minikube_service_endpoint_url = None
        msgs = ""
        while not pqueue.empty():
            msg = pqueue.get()
            msgs += msg
            # check for url in string
            for match in re.finditer(self._url_pattern, msg):
                minikube_service_endpoint_url = match.group()
        if minikube_service_endpoint_url is not None:
            endpoint_match = re.search(
                self._endpoint_pattern, minikube_service_endpoint_url
            )
            return "{}:{}".format(
                endpoint_match.group("host"), endpoint_match.group("port")
            )
        else:
            raise RuntimeError("Minikube get service error: {}".format(msgs))

    def _get_coordinator_endpoint(self):
        # Note that only support NodePort service type
        if is_minikube_cluster() and self._minikube_vm_driver:
            return self._get_minikube_service(
                self._namespace, self._coordinator_service_name
            )

        services = self._core_api.list_namespaced_service(self._namespace)
        for svc in services.items:
            if svc.metadata.name == self._coordinator_service_name:
                port = svc.spec.ports[0].node_port

                if svc.status.load_balancer.ingress is not None:
                    ingress = svc.status.load_balancer.ingress[0]
                    if ingress.hostname is not None:
                        host = ingress.hostname
                    else:
                        host = ingress.ip
                else:
                    selector = ""
                    for k, v in svc.spec.selector.items():
                        selector += k + "=" + v + ","
                    selector = selector[:-1]

                    # get pod
                    pods = self._core_api.list_namespaced_pod(
                        self._namespace, label_selector=selector
                    )
                    host = pods.items[0].status.host_ip
                return "{}:{}".format(host, port)
        raise RuntimeError("Get coordinator endpoint failed.")

    def _dump_cluster_logs(self):
        log_dict = dict()
        pod_items = self._core_api.list_namespaced_pod(self._namespace).to_dict()
        for item in pod_items["items"]:
            log_dict[item["metadata"]["name"]] = self._core_api.read_namespaced_pod_log(
                name=item["metadata"]["name"], namespace=self._namespace
            )
        return log_dict

    def _dump_coordinator_status(self):
        deployments = self._app_api.list_namespaced_deployment(
            namespace=self._namespace
        )
        for deployment in deployments.items:
            if deployment.metadata.name == self._coordinator_name:
                selector = ""
                for k, v in deployment.spec.selector.match_labels.items():
                    selector += k + "=" + v + ","
                selector = selector[:-1]

                pods = self._core_api.list_namespaced_pod(
                    namespace=self._namespace, label_selector=selector
                )
                for pod in pods.items:
                    pod_name = pod.metadata.name
                    try:
                        # dump logs
                        logger.error(
                            self._core_api.read_namespaced_pod_log(
                                name=pod_name,
                                namespace=self._namespace,
                                container=self._coordinator_container_name,
                            )
                        )
                    except K8SApiException as e:
                        # describe pod
                        logger.error(str(e))

    def start(self):
        """Launch graphscope instance on kubernetes cluster.

        Raises:
            RuntimeError: If instance launch failed or timeout.

        Returns:
            str: Coordinator service endpoint.
        """
        try:
            self._create_namespace()
            self._create_role_and_binding()

            self._create_services()
            self._waiting_for_services_ready()
            return self._get_coordinator_endpoint()
        except Exception as e:
            time.sleep(1)
            logger.error("Error when launching Coordinator on kubernetes cluster.")
            self._dump_coordinator_status()
            self.stop()
            raise K8sError(
                "Error when launching Coordinator on kubernetes cluster"
            ) from e

    def stop(self, wait=False):
        """Stop graphscope instance on kubernetes cluster.

        Args:
            wait: bool, optional
                Waiting for delete. Defaults to False.

        Raises:
            TimeoutError:
                Waiting for stop instance timeout when ``wait`` or ``_waiting_for_delete`` is True.
        """
        if not self._closed:
            # delete resources created by graphscope inside namespace
            for target in self._resource_object:
                delete_kubernetes_object(
                    api_client=self._api_client,
                    target=target,
                    wait=self._waiting_for_delete,
                    timeout_seconds=self._timeout_seconds,
                )
            self._resource_object = []
            if self._delete_namespace:
                # delete namespace
                api = CoreV1Api(self._api_client)
                try:
                    api.delete_namespace(self._namespace)
                except K8SApiException:
                    # namespace already deleted.
                    pass
                else:
                    if wait or self._waiting_for_delete:
                        start_time = time.time()
                        while True:
                            try:
                                api.read_namespace(self._namespace)
                            except K8SApiException as ex:
                                if ex.status != 404:
                                    raise
                                break
                            else:
                                time.sleep(1)
                                if (
                                    self._timeout_seconds
                                    and time.time() - start_time > self._timeout_seconds
                                ):
                                    logger.info(
                                        "Deleting namespace %s timeout"
                                        % self._namespace
                                    )
                                    break
            self._closed = True
