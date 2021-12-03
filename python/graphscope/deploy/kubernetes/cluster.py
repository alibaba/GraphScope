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


import json
import logging
import os
import queue
import random
import time

from kubernetes import client as kube_client
from kubernetes.client import CoreV1Api
from kubernetes.client.rest import ApiException as K8SApiException

from graphscope.config import GSConfig as gs_config
from graphscope.deploy.kubernetes.resource_builder import ClusterRoleBindingBuilder
from graphscope.deploy.kubernetes.resource_builder import ClusterRoleBuilder
from graphscope.deploy.kubernetes.resource_builder import GSCoordinatorBuilder
from graphscope.deploy.kubernetes.resource_builder import NamespaceBuilder
from graphscope.deploy.kubernetes.resource_builder import RoleBindingBuilder
from graphscope.deploy.kubernetes.resource_builder import RoleBuilder
from graphscope.deploy.kubernetes.resource_builder import ServiceBuilder
from graphscope.deploy.kubernetes.utils import KubernetesPodWatcher
from graphscope.deploy.kubernetes.utils import delete_kubernetes_object
from graphscope.deploy.kubernetes.utils import get_service_endpoints
from graphscope.deploy.kubernetes.utils import try_to_read_namespace_from_context
from graphscope.deploy.kubernetes.utils import wait_for_deployment_complete
from graphscope.deploy.launcher import Launcher
from graphscope.framework.errors import K8sError
from graphscope.framework.utils import random_string
from graphscope.version import __version__

logger = logging.getLogger("graphscope")


class KubernetesClusterLauncher(Launcher):
    """Class for setting up GraphScope instance on kubernetes cluster."""

    _coordinator_builder_cls = GSCoordinatorBuilder

    _coordinator_name_prefix = "coordinator-"
    _coordinator_service_name_prefix = "coordinator-service-"
    _coordinator_container_name = "coordinator"

    _role_name_prefix = "gs-reader-"
    _role_binding_name_prefix = "gs-reader-binding-"
    _cluster_role_name_prefix = "gs-cluster-reader-"
    _cluster_role_binding_name_prefix = "gs-cluster-reader-binding-"

    _random_coordinator_service_port = random.randint(59001, 60000)
    # placeholder port sometime is needed, such as sshd service
    _random_coordinator_placeholder_port = random.randint(60001, 61000)

    _url_pattern = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"  # noqa: E501
    _endpoint_pattern = r"(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*"

    def __init__(
        self,
        api_client=None,
        k8s_namespace=None,
        k8s_service_type=None,
        num_workers=None,
        preemptive=None,
        k8s_gs_image=None,
        k8s_etcd_image=None,
        k8s_image_pull_policy=None,
        k8s_image_pull_secrets=None,
        k8s_vineyard_daemonset=None,
        k8s_vineyard_cpu=None,
        k8s_vineyard_mem=None,
        vineyard_shared_mem=None,
        k8s_engine_cpu=None,
        k8s_engine_mem=None,
        k8s_coordinator_cpu=None,
        k8s_coordinator_mem=None,
        k8s_etcd_num_pods=None,
        k8s_etcd_cpu=None,
        k8s_etcd_mem=None,
        k8s_mars_worker_cpu=None,
        k8s_mars_worker_mem=None,
        k8s_mars_scheduler_cpu=None,
        k8s_mars_scheduler_mem=None,
        with_mars=None,
        k8s_volumes=None,
        timeout_seconds=None,
        dangling_timeout_seconds=None,
        k8s_waiting_for_delete=None,
        mount_dataset=None,
        k8s_dataset_image=None,
        **kwargs
    ):
        self._api_client = api_client
        self._core_api = kube_client.CoreV1Api(api_client)
        self._app_api = kube_client.AppsV1Api(api_client)
        self._rbac_api = kube_client.RbacAuthorizationV1Api(api_client)

        self._saved_locals = locals()

        self._namespace = self._saved_locals["k8s_namespace"]
        self._image_pull_secrets = self._saved_locals["k8s_image_pull_secrets"]
        if self._image_pull_secrets is None:
            self._image_pull_secrets = []
        elif not isinstance(self._image_pull_secrets, list):
            self._image_pull_secrets = [self._image_pull_secrets]
        self._image_pull_secrets_str = ",".join(self._image_pull_secrets)

        self._instance_id = random_string(6)
        self._role_name = self._role_name_prefix + self._instance_id
        self._role_binding_name = self._role_binding_name_prefix + self._instance_id
        self._cluster_role_name = ""
        self._cluster_role_binding_name = ""

        # all resource created inside namsapce
        self._resource_object = []

        self._coordinator_name = self._coordinator_name_prefix + self._instance_id
        self._coordinator_service_name = (
            self._coordinator_service_name_prefix + self._instance_id
        )
        # environment variable
        self._coordinator_envs = kwargs.pop("coordinator_envs", dict())

        if "GS_COORDINATOR_MODULE_NAME" in os.environ:
            self._coordinator_module_name = os.environ["GS_COORDINATOR_MODULE_NAME"]
        else:
            self._coordinator_module_name = "gscoordinator"

        self._closed = False

        # pods watcher
        self._coordinator_pods_watcher = []
        self._logs = []

        self._delete_namespace = False

    def __del__(self):
        self.stop()

    # TODO(dongze): Check the coordinator pod status, like the poll in Popen
    # we can use this to determine the coordinator status,
    # None for pending, 0 for successed (not likely), other int value for failed.
    def poll(self):
        return 0

    def get_namespace(self):
        """Get kubernetes namespace which graphscope instance running on.

        Returns:
            str: Kubernetes namespace.
        """
        return self._namespace

    def type(self):
        return "k8s"

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
            self._namespace = try_to_read_namespace_from_context()
            # Doesn't have any namespace info in kube context.
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
                api_groups="apps,extensions,",
                resources="configmaps,deployments,deployments/status,endpoints,events,pods,pods/log,pods/exec,pods/status,services,replicasets",  # noqa: E501
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

        if self._delete_namespace:
            # Create clusterRole to delete namespace.
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
        logger.info("Launching coordinator...")
        targets = []

        labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "coordinator",
        }

        # create coordinator service
        service_builder = ServiceBuilder(
            self._coordinator_service_name,
            service_type=self._saved_locals["k8s_service_type"],
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
            image_pull_policy=self._saved_locals["k8s_image_pull_policy"],
        )
        # enable host network
        if "ENABLE_HOST_NETWORK" in os.environ:
            coordinator_builder.host_network = True

        for name in self._image_pull_secrets:
            coordinator_builder.add_image_pull_secret(name)

        envs = {
            "PYTHONUNBUFFERED": "TRUE",
            "KUBE_NAMESPACE": self._namespace,
            "INSTANCE_ID": self._instance_id,
            "GREMLIN_EXPOSE": self._saved_locals["k8s_service_type"],
        }
        if "KUBE_API_ADDRESS" in os.environ:
            envs.update({"KUBE_API_ADDRESS": os.environ["KUBE_API_ADDRESS"]})

        coordinator_builder.add_simple_envs(envs)

        coordinator_builder.add_coordinator_container(
            cmd=["/bin/bash"],
            args=self._build_coordinator_cmd(),
            name=self._coordinator_container_name,
            image=self._saved_locals["k8s_gs_image"],
            cpu=self._saved_locals["k8s_coordinator_cpu"],
            mem=self._saved_locals["k8s_coordinator_mem"],
            preemptive=self._saved_locals["preemptive"],
            ports=[
                self._random_coordinator_service_port,
                self._random_coordinator_placeholder_port,
            ],
            module_name=self._coordinator_module_name,
        )

        targets.append(
            self._app_api.create_namespaced_deployment(
                self._namespace, coordinator_builder.build()
            )
        )

        self._resource_object.extend(targets)

    def _build_coordinator_cmd(self):
        cmd = [
            "unset",
            "LD_PRELOAD",
            "&&",
            "python3",
            "-m",
            self._coordinator_module_name,
            "--cluster_type",
            "k8s",
            "--port",
            str(self._random_coordinator_service_port),
            "--num_workers",
            str(self._saved_locals["num_workers"]),
            "--preemptive",
            str(self._saved_locals["preemptive"]),
            "--instance_id",
            self._instance_id,
            "--log_level",
            gs_config.log_level,
            "--k8s_namespace",
            self._namespace,
            "--k8s_service_type",
            str(self._saved_locals["k8s_service_type"]),
            "--k8s_gs_image",
            self._saved_locals["k8s_gs_image"],
            "--k8s_etcd_image",
            self._saved_locals["k8s_etcd_image"],
            "--k8s_image_pull_policy",
            self._saved_locals["k8s_image_pull_policy"],
            "--k8s_image_pull_secrets",
            self._image_pull_secrets_str if self._image_pull_secrets_str else '""',
            "--k8s_coordinator_name",
            self._coordinator_name,
            "--k8s_coordinator_service_name",
            self._coordinator_service_name,
            "--k8s_etcd_num_pods",
            str(self._saved_locals["k8s_etcd_num_pods"]),
            "--k8s_etcd_cpu",
            str(self._saved_locals["k8s_etcd_cpu"]),
            "--k8s_etcd_mem",
            self._saved_locals["k8s_etcd_mem"],
            "--k8s_vineyard_daemonset",
            str(self._saved_locals["k8s_vineyard_daemonset"]),
            "--k8s_vineyard_cpu",
            str(self._saved_locals["k8s_vineyard_cpu"]),
            "--k8s_vineyard_mem",
            self._saved_locals["k8s_vineyard_mem"],
            "--vineyard_shared_mem",
            self._saved_locals["vineyard_shared_mem"],
            "--k8s_engine_cpu",
            str(self._saved_locals["k8s_engine_cpu"]),
            "--k8s_engine_mem",
            self._saved_locals["k8s_engine_mem"],
            "--k8s_mars_worker_cpu",
            str(self._saved_locals["k8s_mars_worker_cpu"]),
            "--k8s_mars_worker_mem",
            self._saved_locals["k8s_mars_worker_mem"],
            "--k8s_mars_scheduler_cpu",
            str(self._saved_locals["k8s_mars_scheduler_cpu"]),
            "--k8s_mars_scheduler_mem",
            self._saved_locals["k8s_mars_scheduler_mem"],
            "--k8s_with_mars",
            str(self._saved_locals["with_mars"]),
            "--k8s_volumes",
            "'{0}'".format(json.dumps(self._saved_locals["k8s_volumes"])),
            "--timeout_seconds",
            str(self._saved_locals["timeout_seconds"]),
            "--dangling_timeout_seconds",
            str(self._saved_locals["dangling_timeout_seconds"]),
            "--waiting_for_delete",
            str(self._saved_locals["k8s_waiting_for_delete"]),
            "--k8s_delete_namespace",
            str(self._delete_namespace),
        ]
        if self._saved_locals["mount_dataset"] is not None:
            cmd.extend(
                [
                    "--mount_dataset",
                    self._saved_locals["mount_dataset"],
                    "--k8s_dataset_image",
                    self._saved_locals["k8s_dataset_image"],
                ]
            )
        return ["-c", " ".join(cmd)]

    def _create_services(self):
        self._create_coordinator()

    def _waiting_for_services_ready(self):
        deployment = self._app_api.read_namespaced_deployment_status(
            namespace=self._namespace, name=self._coordinator_name
        )

        # get deployment pods
        selector = ""
        for k, v in deployment.spec.selector.match_labels.items():
            selector += k + "=" + v + ","
        selector = selector[:-1]
        pods = self._core_api.list_namespaced_pod(
            namespace=self._namespace, label_selector=selector
        )

        for pod in pods.items:
            self._coordinator_pods_watcher.append(
                KubernetesPodWatcher(
                    self._api_client,
                    self._namespace,
                    pod,
                    self._coordinator_container_name,
                )
            )
            self._coordinator_pods_watcher[-1].start()

        if wait_for_deployment_complete(
            api_client=self._api_client,
            namespace=self._namespace,
            name=self._coordinator_name,
            timeout_seconds=self._saved_locals["timeout_seconds"],
        ):
            for pod_watcher in self._coordinator_pods_watcher:
                pod_watcher.stop()

    def _try_to_get_coordinator_service_from_configmap(self):
        config_map_name = "gs-coordinator-{}".format(self._instance_id)
        start_time = time.time()
        while True:
            try:
                api_response = self._core_api.read_namespaced_config_map(
                    name=config_map_name, namespace=self._namespace
                )
            except K8SApiException:
                pass
            else:
                return "{}:{}".format(
                    api_response.data["ip"], api_response.data["port"]
                )
            time.sleep(1)
            if time.time() - start_time > self._saved_locals["timeout_seconds"]:
                raise TimeoutError("Gete coordinator service from configmap timeout")

    def _get_coordinator_endpoint(self):
        if self._saved_locals["k8s_service_type"] is None:
            # try to get endpoint from configmap
            return self._try_to_get_coordinator_service_from_configmap()

        # Always len(endpoints) >= 1
        endpoints = get_service_endpoints(
            api_client=self._api_client,
            namespace=self._namespace,
            name=self._coordinator_service_name,
            type=self._saved_locals["k8s_service_type"],
        )

        return endpoints[0]

    def _dump_coordinator_failed_status(self):
        # Dump failed status even show_log is False
        if not gs_config.show_log:
            for pod_watcher in self._coordinator_pods_watcher:
                while True:
                    try:
                        message = pod_watcher.poll(timeout_seconds=3)
                    except queue.Empty:
                        pod_watcher.stop()
                        break
                    else:
                        logger.error(message, extra={"simple": True})
        else:
            for pod_watcher in self._coordinator_pods_watcher:
                pod_watcher.stop()

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
            time.sleep(1)
            self._waiting_for_services_ready()
            self._coordinator_endpoint = self._get_coordinator_endpoint()
            logger.info(
                "Coordinator pod start successful with address %s, connecting to service ...",
                self._coordinator_endpoint,
            )
        except Exception as e:
            time.sleep(1)
            self._dump_coordinator_failed_status()
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
            # make sure delete permission resouces in the end
            for target in reversed(self._resource_object):
                delete_kubernetes_object(
                    api_client=self._api_client,
                    target=target,
                    wait=self._saved_locals["k8s_waiting_for_delete"],
                    timeout_seconds=self._saved_locals["timeout_seconds"],
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
                    if wait or self._saved_locals["k8s_waiting_for_delete"]:
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
                                    self._saved_locals["timeout_seconds"]
                                    and time.time() - start_time
                                    > self._saved_locals["timeout_seconds"]
                                ):
                                    logger.info(
                                        "Deleting namespace %s timeout", self._namespace
                                    )
                                    break
            self._closed = True
