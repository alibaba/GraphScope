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

import base64
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
from graphscope.deploy.kubernetes.resource_builder import CoordinatorDeployment
from graphscope.deploy.kubernetes.resource_builder import ResourceBuilder
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

    _coordinator_name_prefix = "coordinator-"

    _role_name_prefix = "gs-reader-"
    _role_binding_name_prefix = f"{_role_name_prefix}binding-"
    _cluster_role_name_prefix = "gs-cluster-reader-"
    _cluster_role_binding_name_prefix = f"{_cluster_role_name_prefix}binding-"

    _random_coordinator_service_port = random.randint(59001, 60000)

    _url_pattern = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"  # noqa: E501
    _endpoint_pattern = r"(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*"

    _coordinator_container_name = "coordinator"
    _coordinator_service_port_name = "coordinator"

    def __init__(
        self,
        api_client=None,
        k8s_namespace=None,
        k8s_service_type=None,
        num_workers=None,
        preemptive=None,
        k8s_image_registry=None,
        k8s_image_repository=None,
        k8s_image_tag=None,
        k8s_image_pull_policy=None,
        k8s_image_pull_secrets=None,
        k8s_vineyard_image=None,
        k8s_vineyard_daemonset=None,
        k8s_vineyard_cpu=None,
        k8s_vineyard_mem=None,
        vineyard_shared_mem=None,
        k8s_engine_cpu=None,
        k8s_engine_mem=None,
        k8s_coordinator_cpu=None,
        k8s_coordinator_mem=None,
        k8s_mars_worker_cpu=None,
        k8s_mars_worker_mem=None,
        k8s_mars_scheduler_cpu=None,
        k8s_mars_scheduler_mem=None,
        k8s_coordinator_pod_node_selector=None,
        k8s_engine_pod_node_selector=None,
        with_mars=None,
        with_analytical=None,
        with_analytical_java=None,
        with_interactive=None,
        with_learning=None,
        k8s_volumes=None,
        timeout_seconds=600,
        dangling_timeout_seconds=None,
        k8s_waiting_for_delete=False,
        with_dataset=False,
        **kwargs,
    ):
        super().__init__()
        self._api_client = api_client
        self._core_api = kube_client.CoreV1Api(api_client)
        self._app_api = kube_client.AppsV1Api(api_client)
        self._rbac_api = kube_client.RbacAuthorizationV1Api(api_client)

        self._saved_locals = locals()
        self._service_type = k8s_service_type
        self._namespace = k8s_namespace
        self._registry = k8s_image_registry
        self._repository = k8s_image_repository
        self._tag = k8s_image_tag
        self._image_pull_policy = k8s_image_pull_policy
        self._image_pull_secrets = k8s_image_pull_secrets
        if self._image_pull_secrets is None:
            self._image_pull_secrets = []
        elif not isinstance(self._image_pull_secrets, list):
            self._image_pull_secrets = [self._image_pull_secrets]

        self._instance_id = random_string(6)
        self._role_name = self._role_name_prefix + self._instance_id
        self._role_binding_name = self._role_binding_name_prefix + self._instance_id
        self._cluster_role_name = ""
        self._cluster_role_binding_name = ""

        # all resource created inside namespace
        self._resource_object = []

        self._coordinator_name = self._coordinator_name_prefix + self._instance_id
        self._coordinator_service_name = self._coordinator_name

        self._closed = False

        # pods watcher
        self._coordinator_pods_watcher = None
        self._logs = []

        self._delete_namespace = False

        self._labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "coordinator",
        }

    def __del__(self):
        self.stop()

    def poll(self):
        """Check the coordinator pod status, 0 for success."""
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
            if not self._namespace_exist(namespace):
                return namespace

    def _resource_exist(self, func, *args):
        try:
            func(*args)
        except K8SApiException as e:
            if e.status != 404:  # Not found
                raise
            return False
        return True

    def _namespace_exist(self, namespace):
        return self._resource_exist(self._core_api.read_namespace, namespace)

    def _role_exist(self, namespace, role):
        return self._resource_exist(
            self._rbac_api.read_namespaced_role, role, namespace
        )

    def _cluster_role_exist(self, cluster_role):
        return self._resource_exist(self._rbac_api.read_cluster_role, cluster_role)

    def _role_binding_exist(self, namespace, role_binding):
        return self._resource_exist(
            self._rbac_api.read_namespaced_role_binding, role_binding, namespace
        )

    def _cluster_role_binding_exist(self, cluster_role_binding):
        return self._resource_exist(
            self._rbac_api.read_cluster_role_binding, cluster_role_binding
        )

    def _create_namespace(self):
        if self._namespace is None:
            self._namespace = try_to_read_namespace_from_context()
            # Doesn't have any namespace info in kube context.
            if self._namespace is None:
                self._namespace = self._get_free_namespace()
        if not self._namespace_exist(self._namespace):
            namespace = ResourceBuilder.get_namespace(self._namespace)
            self._core_api.create_namespace(namespace)
            self._delete_namespace = True

    def _create_role_and_binding(self):
        self._cluster_role_name = self._cluster_role_name_prefix + self._namespace
        self._cluster_role_binding_name = (
            self._cluster_role_binding_name_prefix + self._namespace
        )
        # create a role and bind to default service account.
        targets = []
        if not self._role_exist(namespace=self._namespace, role=self._role_name):
            role = ResourceBuilder.get_role(
                name=self._role_name,
                namespace=self._namespace,
                api_groups=",apps,extensions",  # The leading comma is necessary, represents for core api group.
                resources="configmaps,deployments,deployments/status,statefulsets,statefulsets/status,endpoints,events,pods,pods/log,pods/exec,pods/status,services,replicasets",  # noqa: E501
                verbs="create,delete,get,update,watch,list",
                labels=self._labels,
            )
            ret = self._rbac_api.create_namespaced_role(self._namespace, role)
            targets.append(ret)

        if not self._role_binding_exist(self._namespace, self._role_binding_name):
            role_binding = ResourceBuilder.get_role_binding(
                name=self._role_binding_name,
                namespace=self._namespace,
                role_name=self._role_name,
                service_account_name="default",
                labels=self._labels,
            )
            ret = self._rbac_api.create_namespaced_role_binding(
                self._namespace, role_binding
            )
            targets.append(ret)

        if self._delete_namespace:
            # Create clusterRole to delete namespace.
            if not self._cluster_role_exist(cluster_role=self._cluster_role_name):
                cluster_role = ResourceBuilder.get_cluster_role(
                    name=self._cluster_role_name,
                    api_groups="apps",
                    resources="namespaces",
                    verbs="create,delete,get,update,watch,list",
                    labels=self._labels,
                )
                ret = self._rbac_api.create_cluster_role(cluster_role)
                targets.append(ret)

            if not self._cluster_role_binding_exist(
                cluster_role_binding=self._cluster_role_binding_name
            ):
                cluster_role_binding = ResourceBuilder.get_cluster_role_binding(
                    name=self._cluster_role_binding_name,
                    namespace=self._namespace,
                    role_name=self._cluster_role_name,
                    service_account_name="default",
                    labels=self._labels,
                )
                ret = self._rbac_api.create_cluster_role_binding(cluster_role_binding)
                targets.append(ret)
        self._resource_object.extend(targets)

    def _create_coordinator(self):
        logger.info("Launching coordinator...")
        targets = []

        env = {
            "PYTHONUNBUFFERED": "TRUE",
            "KUBE_NAMESPACE": self._namespace,
            "INSTANCE_ID": self._instance_id,
            "GREMLIN_EXPOSE": self._service_type,
        }
        if "KUBE_API_ADDRESS" in os.environ:
            env["KUBE_API_ADDRESS"] = os.environ["KUBE_API_ADDRESS"]
        if self._registry:
            image_prefix = f"{self._registry}/{self._repository}"
        else:
            image_prefix = self._repository
        image = f"{image_prefix}/coordinator:{self._tag}"
        args = self._get_coordinator_args()

        image_pull_policy = self._saved_locals["k8s_image_pull_policy"]
        host_network = "ENABLE_HOST_NETWORK" in os.environ
        node_selector = self._saved_locals["k8s_coordinator_pod_node_selector"]
        port = self._random_coordinator_service_port

        coordinator = CoordinatorDeployment(
            namespace=self._namespace,
            name=self._coordinator_name,
            image=image,
            args=args,
            labels=self._labels,
            image_pull_secret=self._image_pull_secrets,
            image_pull_policy=image_pull_policy,
            node_selector=node_selector,
            env=env,
            host_network=host_network,
            port=port,
        )

        deployment = coordinator.get_coordinator_deployment()
        response = self._app_api.create_namespaced_deployment(
            self._namespace, deployment
        )
        targets.append(response)

        # create coordinator service
        service = coordinator.get_coordinator_service(
            service_type=self._service_type, port=port
        )
        response = self._core_api.create_namespaced_service(self._namespace, service)
        targets.append(response)

        self._resource_object.extend(targets)

    def base64_encode(self, string):
        return base64.b64encode(string.encode("utf-8")).decode("utf-8")

    def _get_coordinator_args(self):
        args = [
            "python3",
            "-m",
            "gscoordinator",
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
            self._service_type,
            "--k8s_image_repository",
            self._repository,
            "--k8s_image_pull_policy",
            self._image_pull_policy,
            "--k8s_coordinator_name",
            self._coordinator_name,
            "--k8s_coordinator_service_name",
            self._coordinator_service_name,
            "--k8s_vineyard_image",
            self._saved_locals["k8s_vineyard_image"],
            "--k8s_vineyard_cpu",
            str(self._saved_locals["k8s_vineyard_cpu"]),
            "--k8s_vineyard_mem",
            str(self._saved_locals["k8s_vineyard_mem"]),
            "--vineyard_shared_mem",
            str(self._saved_locals["vineyard_shared_mem"]),
            "--k8s_engine_cpu",
            str(self._saved_locals["k8s_engine_cpu"]),
            "--k8s_engine_mem",
            str(self._saved_locals["k8s_engine_mem"]),
            "--k8s_mars_worker_cpu",
            str(self._saved_locals["k8s_mars_worker_cpu"]),
            "--k8s_mars_worker_mem",
            str(self._saved_locals["k8s_mars_worker_mem"]),
            "--k8s_mars_scheduler_cpu",
            str(self._saved_locals["k8s_mars_scheduler_cpu"]),
            "--k8s_mars_scheduler_mem",
            str(self._saved_locals["k8s_mars_scheduler_mem"]),
            "--k8s_with_mars",
            str(self._saved_locals["with_mars"]),
            "--k8s_with_analytical",
            str(self._saved_locals["with_analytical"]),
            "--k8s_with_analytical_java",
            str(self._saved_locals["with_analytical_java"]),
            "--k8s_with_interactive",
            str(self._saved_locals["with_interactive"]),
            "--k8s_with_learning",
            str(self._saved_locals["with_learning"]),
            "--k8s_with_dataset",
            str(self._saved_locals["with_dataset"]),
            "--timeout_seconds",
            str(self._saved_locals["timeout_seconds"]),
            "--dangling_timeout_seconds",
            str(self._saved_locals["dangling_timeout_seconds"]),
            "--waiting_for_delete",
            str(self._saved_locals["k8s_waiting_for_delete"]),
            "--k8s_delete_namespace",
            str(self._delete_namespace),
        ]
        if self._registry:
            args.extend(
                [
                    "--k8s_image_registry",
                    self._registry,
                ]
            )
        if self._tag:
            args.extend(
                [
                    "--k8s_image_tag",
                    self._tag,
                ]
            )
        if self._image_pull_secrets:
            args.extend(
                [
                    "--k8s_image_pull_secrets",
                    ",".join(self._image_pull_secrets),
                ]
            )
        volumes = self._saved_locals["k8s_volumes"]
        if volumes:
            args.extend(
                [
                    "--k8s_volumes",
                    f"{self.base64_encode(json.dumps(volumes))}",
                ]
            )
        if self._saved_locals["k8s_vineyard_daemonset"] is not None:
            args.extend(
                [
                    "--k8s_vineyard_daemonset",
                    str(self._saved_locals["k8s_vineyard_daemonset"]),
                ]
            )

        if self._saved_locals["k8s_engine_pod_node_selector"] is not None:
            args.extend(
                [
                    "--k8s_engine_pod_node_selector",
                    f"{self.base64_encode(json.dumps(self._saved_locals['k8s_engine_pod_node_selector']))}",
                ]
            )
        print(args)
        return args

    def _create_services(self):
        self._create_coordinator()

    def _waiting_for_services_ready(self):
        response = self._app_api.read_namespaced_deployment_status(
            namespace=self._namespace, name=self._coordinator_name
        )

        # get deployment pods
        match_labels = response.spec.selector.match_labels
        selector = ",".join([f"{k}={v}" for k, v in match_labels.items()])
        pods = self._core_api.list_namespaced_pod(
            namespace=self._namespace, label_selector=selector
        )
        assert len(pods.items) == 1, "coordinator deployment should have only one pod"
        pod = pods.items[0]
        self._coordinator_pods_watcher = KubernetesPodWatcher(
            api_client=self._api_client,
            namespace=self._namespace,
            pod=pod,
            container="coordinator",
        )
        self._coordinator_pods_watcher.start()

        if wait_for_deployment_complete(
            api_client=self._api_client,
            namespace=self._namespace,
            name=self._coordinator_name,
            timeout_seconds=self._saved_locals["timeout_seconds"],
        ):
            self._coordinator_pods_watcher.stop()

    def _try_to_get_coordinator_service_from_configmap(self):
        config_map_name = f"gs-coordinator-{self._instance_id}"
        start_time = time.time()
        while True:
            try:
                response = self._core_api.read_namespaced_config_map(
                    name=config_map_name, namespace=self._namespace
                )
                return f"{response.data['ip']}:{response.data['port']}"
            except K8SApiException:
                pass
            time.sleep(1)
            if time.time() - start_time > self._saved_locals["timeout_seconds"]:
                raise TimeoutError("Get coordinator service from configmap timeout")

    def _get_coordinator_endpoint(self):
        if self._service_type is None:
            # try to get endpoint from configmap
            return self._try_to_get_coordinator_service_from_configmap()

        # Always len(endpoints) >= 1
        endpoints = get_service_endpoints(
            api_client=self._api_client,
            namespace=self._namespace,
            name=self._coordinator_service_name,
            service_type=self._service_type,
        )

        return endpoints[0]

    def _dump_coordinator_failed_status(self):
        # Dump failed status even show_log is False
        if self._coordinator_pods_watcher is None:
            return
        if not gs_config.show_log:
            while True:
                try:
                    message = self._coordinator_pods_watcher.poll(timeout_seconds=3)
                    logger.error(message, extra={"simple": True})
                except queue.Empty:
                    break
        self._coordinator_pods_watcher.stop()
        self._coordinator_pods_watcher = None

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

        Raises:
            TimeoutError:
                Waiting for stop instance timeout when ``wait`` or ``_waiting_for_delete`` is True.
        """
        # delete resources created by graphscope inside namespace
        # make sure delete permission resources in the end
        logger.info("Stopping coordinator")
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
                self._delete_namespace = False
            except K8SApiException as e:
                if e.status == 404:  # namespace already deleted.
                    pass
                else:
                    raise
        logger.info("Stopped coordinator")


if __name__ == "__main__":
    from kubernetes import config as kube_config

    kube_config.load_kube_config()
    client = kube_client.ApiClient()
    namespace = "demo"
    service_type = "NodePort"
    num_workers = 2
    k8s_image_registry = "registry-vpc.cn-hongkong.aliyuncs.com"
    k8s_image_repository = "graphscope"
    # k8s_image_tag = "0.17.0"
    k8s_image_tag = "siyuan"
    k8s_image_pull_policy = "IfNotPresent"

    launcher = KubernetesClusterLauncher(
        api_client=client,
        k8s_namespace=namespace,
        k8s_service_type=service_type,
        num_workers=num_workers,
        k8s_image_registry=k8s_image_registry,
        k8s_image_repository=k8s_image_repository,
        k8s_image_tag=k8s_image_tag,
        k8s_image_pull_policy=k8s_image_pull_policy,
    )
    launcher.start()
    print(launcher._get_coordinator_endpoint())
    launcher.stop()
