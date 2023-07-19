#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited.
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
import copy
import json
import logging
import os
import random
import shlex
import subprocess
import sys
import time

from gscoordinator.cluster_builder import EngineCluster
from gscoordinator.cluster_builder import MarsCluster

try:
    from kubernetes import client as kube_client
    from kubernetes import config as kube_config
    from kubernetes import watch as kube_watch
    from kubernetes.client import AppsV1Api
    from kubernetes.client import CoreV1Api
    from kubernetes.client.rest import ApiException as K8SApiException
    from kubernetes.config import ConfigException as K8SConfigException
except ImportError:
    kube_client = None
    kube_config = None
    kube_watch = None
    AppsV1Api = None
    CoreV1Api = None
    K8SApiException = None
    K8SConfigException = None

from graphscope.deploy.kubernetes.utils import delete_kubernetes_object
from graphscope.deploy.kubernetes.utils import get_kubernetes_object_info
from graphscope.deploy.kubernetes.utils import resolve_api_client
from graphscope.framework.utils import PipeWatcher
from graphscope.framework.utils import get_tempdir
from graphscope.proto import types_pb2

from gscoordinator.launcher import AbstractLauncher
from gscoordinator.utils import ANALYTICAL_ENGINE_PATH
from gscoordinator.utils import GRAPHSCOPE_HOME
from gscoordinator.utils import INTERACTIVE_ENGINE_SCRIPT
from gscoordinator.utils import WORKSPACE
from gscoordinator.utils import ResolveMPICmdPrefix
from gscoordinator.utils import delegate_command_to_pod
from gscoordinator.utils import parse_as_glog_level
from gscoordinator.utils import run_kube_cp_command
from gscoordinator.version import __version__

logger = logging.getLogger("graphscope")


class FakeKubeResponse:
    def __init__(self, obj):
        self.data = json.dumps(obj)


class KubernetesClusterLauncher(AbstractLauncher):
    def __init__(
        self,
        coordinator_name=None,
        coordinator_service_name=None,
        delete_namespace=None,
        engine_cpu=None,
        engine_mem=None,
        engine_pod_node_selector=None,
        image_pull_policy=None,
        image_pull_secrets=None,
        image_registry=None,
        image_repository=None,
        image_tag=None,
        instance_id=None,
        log_level=None,
        mars_scheduler_cpu=None,
        mars_scheduler_mem=None,
        mars_worker_cpu=None,
        mars_worker_mem=None,
        with_dataset=False,
        namespace=None,
        num_workers=None,
        preemptive=None,
        service_type=None,
        timeout_seconds=None,
        kube_timeout_seconds=1,
        retry_time_seconds=2,
        del_retry_time_seconds=1,
        vineyard_cpu=None,
        vineyard_deployment=None,
        vineyard_image=None,
        vineyard_mem=None,
        vineyard_shared_mem=None,
        volumes=None,
        waiting_for_delete=None,
        with_mars=False,
        enabled_engines="",
        dataset_proxy=None,
        deploy_mode="eager",
        **kwargs,
    ):
        super().__init__()
        self._api_client = resolve_api_client()
        self._core_api = kube_client.CoreV1Api(self._api_client)
        self._apps_api = kube_client.AppsV1Api(self._api_client)
        self._resource_object = ResourceManager(self._api_client)

        self._instance_id = instance_id
        self._namespace = namespace
        self._delete_namespace = delete_namespace

        self._coordinator_name = coordinator_name
        self._coordinator_service_name = coordinator_service_name

        self._owner_references = self.get_coordinator_owner_references()
        self._image_registry = image_registry
        self._image_repository = image_repository
        self._image_tag = image_tag
        self._image_pull_policy = image_pull_policy

        self._image_pull_secrets = (
            image_pull_secrets.split(",") if image_pull_secrets else []
        )

        self._glog_level = parse_as_glog_level(log_level)

        self._engine_pod_prefix = "gs-engine-"

        self._num_workers = num_workers

        self._vineyard_image = vineyard_image
        self._vineyard_mem = vineyard_mem
        self._vineyard_cpu = vineyard_cpu
        self._vineyard_size = vineyard_shared_mem

        self._vineyard_deployment = vineyard_deployment

        self._engine_cpu = engine_cpu
        self._engine_mem = engine_mem
        self._engine_pod_node_selector = engine_pod_node_selector
        self._vineyard_shared_mem = vineyard_shared_mem

        self._volumes = volumes
        self._dataset_proxy = dataset_proxy

        self._with_dataset = with_dataset
        self._preemptive = preemptive
        self._service_type = service_type

        assert timeout_seconds is not None
        self._timeout_seconds = timeout_seconds
        # timeout seconds waiting for kube service ready
        self._kube_timeout_seconds = kube_timeout_seconds
        # retry time when waiting for kube service ready
        self._retry_time_seconds = retry_time_seconds
        # retry time when deleting dangling coordinators
        self._del_retry_time_seconds = del_retry_time_seconds

        self._waiting_for_delete = waiting_for_delete
        self._serving = False

        self._with_analytical = False
        self._with_analytical_java = False
        self._with_interactive = False
        self._with_learning = False
        engines = set([item.strip() for item in enabled_engines.split(",")])
        valid_engines = set(
            "analytical,analytical-java,interactive,learning,gae,gae-java,gie,gle".split(
                ","
            )
        )

        for item in engines:
            if item not in valid_engines and item != "":
                raise ValueError(
                    f"Not a valid engine name: {item}, valid engines are {valid_engines}"
                )
            if item == "analytical" or item == "gae":
                self._with_analytical = True
            if item == "interactive" or item == "gie":
                self._with_interactive = True
            if item == "learning" or item == "gle":
                self._with_learning = True
            if item == "analytical-java" or item == "gae-java":
                self._with_analytical_java = True

        self._with_mars = with_mars
        self._mars_scheduler_cpu = mars_scheduler_cpu
        self._mars_scheduler_mem = mars_scheduler_mem
        self._mars_worker_cpu = mars_worker_cpu
        self._mars_worker_mem = mars_worker_mem

        # check the validity of deploy mode
        self._deploy_mode = deploy_mode
        if self._deploy_mode not in ["eager", "lazy"]:
            logger.error(
                "Invalid mode %s, choose from 'eager' or 'lazy'. Proceeding with default mode: 'eager'",
                self._deploy_mode,
            )
            self._deploy_mode = "eager"

        self._vineyard_pod_name_list = []

        # set the kube config file
        self._k8s_config_file = self.get_current_kubeconfig()
        config = kwargs.pop("k8s_client_config", {})
        if "config_file" in config:
            self._k8s_config_file = config["config_file"]

        if self._vineyard_deployment is not None:
            self._deploy_vineyard_deployment_if_not_exist()
            # check the if the vineyard deployment is ready again
            if not self._check_if_vineyard_deployment_exist():
                # if not ready, then set the vineyard deployment to None
                logger.error(
                    "Vineyard deployment %s is not ready, please check the deployment status."
                    "Proceeding with none vineyard deployment mode.",
                    self._vineyard_deployment,
                )
                self._vineyard_deployment = None

        # if the vineyard deployment is not set and use the eager mode,
        # which means deploy the engine as a single pod and there is no
        # external vineyard deployment. The vineyard objects are not
        # shared between the engine pods, so report an error here and set
        # the mode to eager.
        if self._deploy_mode == "lazy" and self._vineyard_deployment is None:
            logger.error(
                "Lazy mode is only possible with a vineyard deployment, "
                "please add a vineyard deployment name by k8s_vineyard_deployment='vineyardd-sample'. "
                "Proceeding with default mode: 'eager'"
            )
            self._deploy_mode = "eager"

        self._pod_name_list = []
        self._pod_ip_list = []
        self._pod_host_ip_list = []

        # analytical engine
        self._analytical_pod_name = []
        self._analytical_pod_ip = []
        self._analytical_pod_host_ip = []
        # analytical java engine
        self._analytical_java_pod_name = []
        self._analytical_java_pod_ip = []
        self._analytical_java_pod_host_ip = []
        # interactive engine
        self._interactive_resource_object = {}
        self._interactive_pod_name = {}
        self._interactive_pod_ip = {}
        self._interactive_pod_host_ip = {}
        # learning engine
        self._learning_resource_object = {}
        self._learning_pod_name = {}
        self._learning_pod_ip = {}
        self._learning_pod_host_ip = {}

        self._analytical_engine_endpoint = None
        self._mars_service_endpoint = None

        self._analytical_engine_process = None
        self._random_analytical_engine_rpc_port = random.randint(56001, 57000)
        # interactive engine
        # executor inter-processing port
        # executor rpc port
        # frontend port
        self._interactive_port = 8233
        # 8000 ~ 9000 is exposed
        self._learning_start_port = 8000

        self._graphlearn_services = {}
        self._learning_instance_processes = {}

        # workspace
        self._instance_workspace = os.path.join(WORKSPACE, instance_id)
        os.makedirs(self._instance_workspace, exist_ok=True)
        self._session_workspace = None

        self._engine_cluster = self._build_engine_cluster(
            with_analytical_container=self._with_analytical,
            with_analytical_java_container=self._with_analytical_java,
            with_interactive_container=self._with_interactive,
            with_learning_container=self._with_learning,
        )

        self._vineyard_service_endpoint = None
        self._vineyard_internal_service_endpoint = None
        self._mars_service_endpoint = None
        if self._with_mars:
            self._mars_cluster = MarsCluster(
                self._instance_id, self._namespace, self._service_type
            )

    def __del__(self):
        self.stop()

    def type(self):
        return types_pb2.K8S

    def vineyard_deployment_exists(self):
        return self._vineyard_deployment is not None

    def get_current_kubeconfig(self):
        """Gets the current kubeconfig file path.

        Returns:
            str: The path to the current kubeconfig file.
        """

        kubeconfig_path = os.environ.get("KUBECONFIG")
        if kubeconfig_path is None:
            kubeconfig_path = "~/.kube/config"

        return kubeconfig_path

    # the argument `with_analytical_` means whether to add the analytical engine
    # container to the engine statefulset, and the other three arguments are similar.
    def _build_engine_cluster(
        self,
        with_analytical_container: bool,
        with_analytical_java_container: bool,
        with_interactive_container: bool,
        with_learning_container: bool,
    ):
        return EngineCluster(
            engine_cpu=self._engine_cpu,
            engine_mem=self._engine_mem,
            engine_pod_node_selector=self._engine_pod_node_selector,
            engine_pod_prefix=self._engine_pod_prefix,
            glog_level=self._glog_level,
            image_pull_policy=self._image_pull_policy,
            image_pull_secrets=self._image_pull_secrets,
            image_registry=self._image_registry,
            image_repository=self._image_repository,
            image_tag=self._image_tag,
            instance_id=self._instance_id,
            learning_start_port=self._learning_start_port,
            with_dataset=self._with_dataset,
            namespace=self._namespace,
            num_workers=self._num_workers,
            preemptive=self._preemptive,
            service_type=self._service_type,
            vineyard_cpu=self._vineyard_cpu,
            vineyard_deployment=self._vineyard_deployment,
            vineyard_image=self._vineyard_image,
            vineyard_mem=self._vineyard_mem,
            vineyard_shared_mem=self._vineyard_shared_mem,
            volumes=self._volumes,
            with_mars=self._with_mars,
            with_analytical=with_analytical_container,
            with_analytical_java=with_analytical_java_container,
            with_interactive=with_interactive_container,
            with_learning=with_learning_container,
            dataset_proxy=self._dataset_proxy,
        )

    def get_coordinator_owner_references(self):
        owner_references = []
        if self._coordinator_name:
            try:
                deployment = self._apps_api.read_namespaced_deployment(
                    self._coordinator_name, self._namespace
                )
                owner_references.append(
                    kube_client.V1OwnerReference(
                        api_version="apps/v1",
                        kind="Deployment",
                        name=self._coordinator_name,
                        uid=deployment.metadata.uid,
                    )
                )
            except K8SApiException:
                logger.error("Coordinator %s not found", self._coordinator_name)

        return owner_references

    def waiting_for_delete(self):
        return self._waiting_for_delete

    def get_namespace(self):
        return self._namespace

    def get_vineyard_stream_info(self):
        hosts = [f"{self._namespace}:{host}" for host in self._pod_name_list]
        if self._vineyard_deployment is not None:
            hosts = [
                f"{self._namespace}:{host}" for host in self._vineyard_pod_name_list
            ]
        return "kubernetes", hosts

    def set_session_workspace(self, session_id):
        self._session_workspace = os.path.join(self._instance_workspace, session_id)
        os.makedirs(self._session_workspace, exist_ok=True)

    def launch_etcd(self):
        pass

    def configure_etcd_endpoint(self):
        pass

    @property
    def preemptive(self):
        return self._preemptive

    @property
    def hosts(self):
        """String of a list of pod name, comma separated."""
        return ",".join(self._pod_name_list)

    @property
    def hosts_list(self):
        return self._get_analytical_hosts()

    def distribute_file(self, path):
        pod_name_list, _, _ = self._allocate_analytical_engine()
        for pod in pod_name_list:
            container = self._engine_cluster.analytical_container_name
            try:
                # The library may exists in the analytical pod.
                test_cmd = f"test -f {path}"
                logger.debug(delegate_command_to_pod(test_cmd, pod, container))
                logger.info("Library exists, skip distribute")
            except RuntimeError:
                cmd = f"mkdir -p {os.path.dirname(path)}"
                logger.debug(delegate_command_to_pod(cmd, pod, container))
                logger.debug(run_kube_cp_command(path, path, pod, container, True))

    def close_analytical_instance(self):
        pass

    def launch_vineyard(self):
        """Launch vineyardd in k8s cluster."""
        # vineyardd is auto launched in vineyardd container
        # args = f"vineyardd -size {self._vineyard_shared_mem} \
        #  -socket {self._engine_cluster._sock} -etcd_endpoint http://{self._pod_ip_list[0]}:2379"
        pass

    def close_etcd(self):
        # etcd is managed by vineyard
        pass

    def close_vineyard(self):
        # No need to close vineyardd
        # Use delete deployment instead
        pass

    def check_if_engine_exist(self, engine_type, object_id=None):
        """Checks if the engine with the given type exists.

        Args:
            engine_type: The type of engine to check for.
            object_id: The object id of the engine to check for.

        Returns:
            True if the engine exists, False otherwise.
        """

        if object_id:
            engine_pod_name_dict = getattr(self, f"_{engine_type}_pod_name")
            engine_pod_name_list = engine_pod_name_dict.get(object_id, [])
            engine_pod_ip_dict = getattr(self, f"_{engine_type}_pod_ip")
            engine_pod_ip_list = engine_pod_ip_dict.get(object_id, [])
            engine_pod_host_ip_dict = getattr(self, f"_{engine_type}_pod_host_ip")
            engine_pod_host_ip_list = engine_pod_host_ip_dict.get(object_id, [])
        else:
            engine_pod_name_list = getattr(self, f"_{engine_type}_pod_name")
            engine_pod_ip_list = getattr(self, f"_{engine_type}_pod_ip")
            engine_pod_host_ip_list = getattr(self, f"_{engine_type}_pod_host_ip")

        return engine_pod_name_list and engine_pod_ip_list and engine_pod_host_ip_list

    def deploy_engine(self, engine_type, object_id=None):
        """Deploys the engine with the given type.

        Args:
            engine_type: The type of engine to deploy.
            object_id: The object ID to deploy the engine with.

        Returns:
            A tuple of the pod names, IP addresses, and host IP addresses of the
            deployed engine and the response of the engine and service.
        """

        if not self.check_if_engine_exist(engine_type, object_id):
            self._engine_pod_prefix = f"gs-{engine_type}-" + (
                f"{object_id}-" if object_id else ""
            ).replace("_", "-")
            self._engine_cluster = self._build_engine_cluster(
                with_analytical_container=engine_type == "analytical",
                with_analytical_java_container=engine_type == "analytical-java",
                with_interactive_container=engine_type == "interactive",
                with_learning_container=engine_type == "learning",
            )
            response = self._create_engine_stateful_set()
            self._waiting_for_services_ready()

            if object_id:
                resource_object = getattr(self, f"_{engine_type}_resource_object")
                pod_name = getattr(self, f"_{engine_type}_pod_name")
                pod_ip = getattr(self, f"_{engine_type}_pod_ip")
                pod_host_ip = getattr(self, f"_{engine_type}_pod_host_ip")
                resource_object[object_id] = response
                pod_name[object_id] = self._pod_name_list
                pod_ip[object_id] = self._pod_ip_list
                pod_host_ip[object_id] = self._pod_host_ip_list
            else:
                # Set the engine pod info
                setattr(self, f"_{engine_type}_pod_name", self._pod_name_list)
                setattr(self, f"_{engine_type}_pod_ip", self._pod_ip_list)
                setattr(self, f"_{engine_type}_pod_host_ip", self._pod_host_ip_list)

        return (
            getattr(self, f"_{engine_type}_pod_name")
            if object_id is None
            else getattr(self, f"_{engine_type}_pod_name")[object_id],
            getattr(self, f"_{engine_type}_pod_ip")
            if object_id is None
            else getattr(self, f"_{engine_type}_pod_ip")[object_id],
            getattr(self, f"_{engine_type}_pod_host_ip")
            if object_id is None
            else getattr(self, f"_{engine_type}_pod_host_ip")[object_id],
        )

    def delete_engine_stateful_set_with_object_id(self, engine_type, object_id):
        """delete the engine stateful set with the given object id.

        Args:
            object_id (int): The object id of the engine to delete.
        """
        resource_object = getattr(self, f"_{engine_type}_resource_object")
        obj = resource_object.get(object_id, {})
        if obj:
            delete_kubernetes_object(
                api_client=self._api_client,
                target=obj,
                wait=self._waiting_for_delete,
                timeout_seconds=self._timeout_seconds,
            )

            pod_name = getattr(self, f"_{engine_type}_pod_name")
            pod_ip = getattr(self, f"_{engine_type}_pod_ip")
            pod_host_ip = getattr(self, f"_{engine_type}_pod_host_ip")
            del resource_object[object_id]
            del pod_name[object_id]
            del pod_ip[object_id]
            del pod_host_ip[object_id]

    def deploy_analytical_engine(self):
        return self.deploy_engine("analytical")

    def deploy_analytical_java_engine(self):
        return self.deploy_engine("analytical-java")

    def deploy_interactive_engine(self, object_id):
        pod_name_list, pod_ip_list, pod_host_ip_list = self.deploy_engine(
            "interactive", object_id
        )
        try:
            response = self._core_api.read_namespaced_pod(
                pod_name_list[0], self._namespace
            )
        except K8SApiException:
            logger.exception(
                "Get pod %s error, please check if the pod is ready",
                pod_name_list[0],
            )
        owner_references = [
            kube_client.V1OwnerReference(
                api_version=response.metadata.owner_references[0].api_version,
                kind=response.metadata.owner_references[0].kind,
                name=response.metadata.owner_references[0].name,
                uid=response.metadata.owner_references[0].uid,
            )
        ]
        name = f"gs-interactive-frontend-{object_id}-{self._instance_id}"
        self._create_frontend_deployment(name, owner_references)

        return pod_name_list, pod_ip_list, pod_host_ip_list

    def deploy_learning_engine(self, object_id):
        return self.deploy_engine("learning", object_id)

    def delete_interactive_engine(self, object_id):
        self.delete_engine_stateful_set_with_object_id("interactive", object_id)

    def delete_learning_engine(self, object_id):
        self.delete_engine_stateful_set_with_object_id("learning", object_id)

    def _allocate_interactive_engine(self, object_id):
        # check the interactive engine flag
        if not self._with_interactive:
            raise NotImplementedError("Interactive engine not enabled")

        # allocate analytical engine based on the mode
        if self._deploy_mode == "eager":
            return self._pod_name_list, self._pod_ip_list, self._pod_host_ip_list
        return self.deploy_interactive_engine(object_id)

    def _distribute_interactive_process(
        self,
        hosts,
        object_id: int,
        schema_path: str,
        params: dict,
        engine_selector: str,
    ):
        """
        Args:
            hosts (str): hosts of the graph.
            object_id (int): object id of the graph.
            schema_path (str): path of the schema file.
            engine_selector(str): the label selector of the engine.
        """
        env = os.environ.copy()
        env["GRAPHSCOPE_HOME"] = GRAPHSCOPE_HOME
        container = self._engine_cluster.interactive_executor_container_name

        params = "\n".join([f"{k}={v}" for k, v in params.items()])
        params = base64.b64encode(params.encode("utf-8")).decode("utf-8")

        cmd = [
            INTERACTIVE_ENGINE_SCRIPT,
            "create_gremlin_instance_on_k8s",
            self._session_workspace,
            str(object_id),
            schema_path,
            hosts,
            container,
            str(self._interactive_port),  # executor port
            str(self._interactive_port + 1),  # executor rpc port
            str(self._interactive_port + 2),  # frontend gremlin port
            str(self._interactive_port + 3),  # frontend cypher port
            self._coordinator_name,
            engine_selector,
            params,
        ]
        self._interactive_port += 4
        logger.info("Create GIE instance with command: %s", " ".join(cmd))
        process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=os.getcwd(),
            env=env,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
        )
        return process

    def create_interactive_instance(
        self, object_id: int, schema_path: str, params: dict
    ):
        pod_name_list, _, _ = self._allocate_interactive_engine(object_id)
        if not pod_name_list:
            raise RuntimeError("Failed to allocate interactive engine")
        hosts = ",".join(pod_name_list)

        engine_selector = "gs-engine-" + self._instance_id
        if self._deploy_mode == "lazy":
            engine_selector = (
                "gs-interactive-" + str(object_id) + "-" + self._instance_id
            )

        return self._distribute_interactive_process(
            hosts, object_id, schema_path, params, engine_selector
        )

    def close_interactive_instance(self, object_id):
        if self._deploy_mode == "lazy":
            logger.info("Close interactive instance with object id: %d", object_id)
            self.delete_interactive_engine(object_id)
            return None
        pod_name_list, _, _ = self._allocate_interactive_engine(object_id)
        hosts = ",".join(pod_name_list)
        env = os.environ.copy()
        env["GRAPHSCOPE_HOME"] = GRAPHSCOPE_HOME
        container = self._engine_cluster.interactive_executor_container_name
        cmd = [
            INTERACTIVE_ENGINE_SCRIPT,
            "close_gremlin_instance_on_k8s",
            self._session_workspace,
            str(object_id),
            hosts,
            container,
            self._instance_id,
        ]
        logger.info("Close GIE instance with command: %s", " ".join(cmd))
        process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=os.getcwd(),
            env=env,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
        )
        return process

    def _create_mars_scheduler(self):
        logger.info("Launching mars scheduler pod for GraphScope ...")
        deployment = self._mars_cluster.get_mars_deployment()
        deployment.metadata.owner_references = self._owner_references
        response = self._apps_api.create_namespaced_deployment(
            self._namespace, deployment
        )
        self._resource_object.append(response)

    # The function is used to inject vineyard as a sidecar container into the workload
    # and return the json string of new workload which is injected with vineyard sidecar
    #
    # Assume we have a workload json as below:
    #
    # {
    #  "apiVersion": "apps/v1",
    #  "kind": "Deployment",
    #  "metadata": {
    #    "name": "nginx-deployment",
    #    "namespace": "vineyard-job"
    #  },
    #  "spec": {
    #    "selector": {
    #      "matchLabels": {
    #        "app": "nginx"
    #      }
    #    },
    #    "template": {
    #      "metadata": {
    #        "labels": {
    #          "app": "nginx"
    #        }
    #      },
    #      "spec": {
    #        "containers": [
    #          {
    #            "name": "nginx",
    #            "image": "nginx:1.14.2",
    #            "ports": [
    #              {
    #                "containerPort": 80
    #              }
    #            ]
    #          }
    #        ]
    #      }
    #    }
    #  }
    # }
    #
    # The function will return a new workload json as below:
    #
    # {
    #  "apiVersion": "apps/v1",
    #  "kind": "Deployment",
    #  "metadata": {
    #    "creationTimestamp": null,
    #    "name": "nginx-deployment",
    #    "namespace": "vineyard-job"
    #  },
    #  "spec": {
    #    "selector": {
    #      "matchLabels": {
    #        "app": "nginx"
    #      }
    #    }
    #  },
    #  "template": {
    #    "metadata": null,
    #    "labels": {
    #      "app": "nginx",
    #      "app.vineyard.io/name": "vineyard-sidecar"
    #    },
    #    "spec": {
    #      "containers": [
    #        {
    #          "command": null,
    #          "image": "nginx:1.14.2",
    #          "name": "nginx",
    #          "ports": [
    #            {
    #              "containerPort": 80
    #            }
    #          ],
    #          "volumeMounts": [
    #            {
    #              "mountPath": "/var/run",
    #              "name": "vineyard-socket"
    #            }
    #          ]
    #        },
    #        {
    #          "command": [
    #            "/bin/bash",
    #            "-c",
    #            "/usr/bin/wait-for-it.sh -t 60 vineyard-sidecar-etcd-service.vineyard-job.svc.cluster.local:2379; \\\n
    #             sleep 1; /usr/local/bin/vineyardd --sync_crds true --socket /var/run/vineyard.sock --size 256Mi \\\n
    #             --stream_threshold 80 --etcd_cmd etcd --etcd_prefix /vineyard \\\n
    #             --etcd_endpoint http://vineyard-sidecar-etcd-service:2379\n"
    #          ],
    #          "env": [
    #            {
    #              "name": "VINEYARDD_UID",
    #              "value": null
    #            },
    #            {
    #              "name": "VINEYARDD_NAME",
    #              "value": "vineyard-sidecar"
    #            },
    #            {
    #              "name": "VINEYARDD_NAMESPACE",
    #              "value": "vineyard-job"
    #            }
    #          ],
    #          "image": "vineyardcloudnative/vineyardd:latest",
    #          "imagePullPolicy": "IfNotPresent",
    #          "name": "vineyard-sidecar",
    #          "ports": [
    #            {
    #              "containerPort": 9600,
    #              "name": "vineyard-rpc",
    #              "protocol": "TCP"
    #            }
    #          ],
    #          "volumeMounts": [
    #            {
    #              "mountPath": "/var/run",
    #              "name": "vineyard-socket"
    #            }
    #          ]
    #        }
    #      ],
    #      "volumes": [
    #        {
    #          "emptyDir": {},
    #          "name": "vineyard-socket"
    #        }
    #      ]
    #    }
    #  }
    # }

    def _inject_vineyard_as_sidecar(self, workload):
        import vineyard

        # create the annotations for the workload's template if not exists
        if workload.spec.template.metadata.annotations is None:
            workload.spec.template.metadata.annotations = {}

        # create the labels for the workload's template if not exists
        if workload.spec.template.metadata.labels is None:
            workload.spec.template.metadata.labels = {}

        workload_json = json.dumps(
            self._api_client.sanitize_for_serialization(workload)
        )

        sts_name = (
            f"{self._engine_cluster.engine_stateful_set_name}-{self._instance_id}"
        )
        owner_reference = [
            {
                "apiVersion": self._owner_references[0].api_version,
                "kind": self._owner_references[0].kind,
                "name": self._owner_references[0].name,
                "uid": self._owner_references[0].uid,
            }
        ]

        owner_reference_json = json.dumps(owner_reference)
        # inject vineyard sidecar into the workload
        #
        # the name is used to specify the name of the sidecar container, which is also the
        # labelSelector of the rpc service and the etcd service.
        #
        # the apply_resources is used to apply resources to the kubernetes cluster during
        # the injection.
        #
        # for more details about vineyardctl inject, please refer to the link below:
        # https://github.com/v6d-io/v6d/tree/main/k8s/cmd#vineyardctl-inject

        new_workload_json = vineyard.deploy.vineyardctl.inject(
            kubeconfig=self._k8s_config_file,
            resource=workload_json,
            sidecar_volume_mountpath="/tmp/vineyard_workspace",
            name=sts_name + "-vineyard",
            apply_resources=True,
            owner_references=owner_reference_json,
            sidecar_image=self._vineyard_image,
            sidecar_size=self._vineyard_shared_mem,
            sidecar_cpu=self._vineyard_cpu,
            sidecar_memory=self._vineyard_mem,
            sidecar_service_type=self._service_type,
            output="json",
            capture=True,
        )

        normalized_workload_json = json.loads(new_workload_json)
        final_workload_json = json.loads(normalized_workload_json["workload"])

        fake_kube_response = FakeKubeResponse(final_workload_json)

        new_workload = self._api_client.deserialize(fake_kube_response, type(workload))
        return new_workload

    def _create_engine_stateful_set(self):
        logger.info("Creating engine pods...")

        stateful_set = self._engine_cluster.get_engine_stateful_set()
        if self._vineyard_deployment is not None:
            # schedule engine statefulset to the same node with vineyard deployment
            stateful_set = self._add_pod_affinity_for_vineyard_deployment(
                workload=stateful_set
            )
        else:
            stateful_set = self._inject_vineyard_as_sidecar(stateful_set)

        response = self._apps_api.create_namespaced_stateful_set(
            self._namespace, stateful_set
        )
        self._resource_object.append(response)
        return response

    def _create_frontend_deployment(self, name=None, owner_references=None):
        logger.info("Creating frontend pods...")
        deployment = self._engine_cluster.get_interactive_frontend_deployment()
        if name is not None:
            deployment.metadata.name = name
        deployment.metadata.owner_references = owner_references
        response = self._apps_api.create_namespaced_deployment(
            self._namespace, deployment
        )
        self._resource_object.append(response)

    def _create_frontend_service(self):
        logger.info("Creating frontend service...")
        service = self._engine_cluster.get_interactive_frontend_service(8233, 7687)
        service.metadata.owner_references = self._owner_references
        response = self._core_api.create_namespaced_service(self._namespace, service)
        self._resource_object.append(response)

    def _create_learning_service(self, object_id):
        logger.info("Creating learning service...")
        service = self._engine_cluster.get_learning_service(
            object_id, self._learning_start_port
        )
        service.metadata.owner_references = self._owner_references
        response = self._core_api.create_namespaced_service(self._namespace, service)
        self._graphlearn_services[object_id] = response
        self._resource_object.append(response)

    def get_engine_config(self):
        config = {
            "vineyard_service_name": self._engine_cluster.vineyard_service_name,
            "vineyard_rpc_endpoint": self._vineyard_service_endpoint,
        }
        if self._with_mars:
            config["mars_endpoint"] = self._mars_service_endpoint
        return config

    def _create_services(self):
        self._create_engine_stateful_set()
        if self._with_interactive:
            self._create_frontend_deployment(owner_references=self._owner_references)
            # self._create_frontend_service()
        if self._with_mars:
            # scheduler used by Mars
            self._create_mars_scheduler()

    def _waiting_for_services_ready(self):
        logger.info("Waiting for services ready...")
        selector = ""
        namespace = self._namespace
        start_time = time.time()
        event_messages = []
        while True:
            # TODO: Add label selector to filter out deployments.
            statefulsets = self._apps_api.list_namespaced_stateful_set(namespace)
            service_available = False
            for rs in statefulsets.items:
                if rs.metadata.name == self._engine_cluster.engine_stateful_set_name:
                    # logger.info(
                    #     "Engine pod: %s ready / %s total",
                    #     rs.status.ready_replicas,
                    #     self._num_workers,
                    # )
                    if rs.status.ready_replicas == self._num_workers:
                        # service is ready
                        service_available = True
                        break

                    # check container status
                    labels = rs.spec.selector.match_labels
                    selector = ",".join(f"{k}={v}" for k, v in labels.items())

                    pods = self._core_api.list_namespaced_pod(
                        namespace=namespace, label_selector=selector
                    )

                    for pod in pods.items:
                        pod_name = pod.metadata.name
                        field_selector = "involvedObject.name=" + pod_name
                        stream = kube_watch.Watch().stream(
                            self._core_api.list_namespaced_event,
                            namespace,
                            field_selector=field_selector,
                            timeout_seconds=self._kube_timeout_seconds,
                        )
                        for event in stream:
                            msg = f"[{pod_name}]: {event['object'].message}"
                            if msg not in event_messages:
                                event_messages.append(msg)
                                logger.info(msg)
                                if event["object"].reason == "Failed":
                                    raise RuntimeError("Kubernetes event error: " + msg)

            if service_available:
                break
            if self._timeout_seconds + start_time < time.time():
                raise TimeoutError("GraphScope Engines launching timeout.")
            time.sleep(self._retry_time_seconds)

        self._pod_name_list = []
        self._pod_ip_list = []
        self._pod_host_ip_list = []
        pods = self._core_api.list_namespaced_pod(
            namespace=namespace, label_selector=selector
        )
        for pod in pods.items:
            self._pod_name_list.append(pod.metadata.name)
            self._pod_ip_list.append(pod.status.pod_ip)
            self._pod_host_ip_list.append(pod.status.host_ip)
        assert len(self._pod_ip_list) > 0
        self._analytical_engine_endpoint = (
            f"{self._pod_ip_list[0]}:{self._random_analytical_engine_rpc_port}"
        )
        self._vineyard_service_endpoint = (
            self._engine_cluster.get_vineyard_service_endpoint(self._api_client)
        )
        self._vineyard_internal_endpoint = (
            f"{self._pod_ip_list[0]}:{self._engine_cluster._vineyard_service_port}"
        )

        logger.info("GraphScope engines pod is ready.")
        logger.info("Engines pod name list: %s", self._pod_name_list)
        logger.info("Engines pod ip list: %s", self._pod_ip_list)
        logger.info("Engines pod host ip list: %s", self._pod_host_ip_list)
        logger.info("Vineyard service endpoint: %s", self._vineyard_service_endpoint)
        if self._with_mars:
            self._mars_service_endpoint = self._mars_cluster.get_mars_service_endpoint(
                self._api_client
            )
            logger.info("Mars service endpoint: %s", self._mars_service_endpoint)

    # the function will add the podAffinity to the engine workload so that the workload
    # will be scheduled to the same node with vineyard deployment.
    # e.g. the vineyard deployment is named "vineyard-deployment" and the namespace is "graphscope-system",
    # the podAffinity will be added to the engine workload as below:
    # spec:
    #   affinity:
    #     podAffinity:
    #       requiredDuringSchedulingIgnoredDuringExecution:
    #       - labelSelector:
    #           matchExpressions:
    #           - key: app.kubernetes.io/instance
    #             operator: In
    #             values:
    #             - graphscope-system-vineyard-deployment # [vineyard deployment namespace]-[vineyard deployment name]
    #         topologyKey: kubernetes.io/hostname
    def _add_pod_affinity_for_vineyard_deployment(self, workload):
        import vineyard

        workload_json = json.dumps(
            self._api_client.sanitize_for_serialization(workload)
        )
        new_workload_json = vineyard.deploy.vineyardctl.schedule.workload(
            kubeconfig=self._k8s_config_file,
            resource=workload_json,
            vineyardd_name=self._vineyard_deployment,
            vineyardd_namespace=self._namespace,
            capture=True,
        )

        normalized_workload_json = json.loads(new_workload_json)
        fake_kube_response = FakeKubeResponse(normalized_workload_json)
        new_workload = self._api_client.deserialize(fake_kube_response, type(workload))
        return new_workload

    def _dump_resource_object(self):
        resource = {}
        if self._delete_namespace:
            resource[self._namespace] = "Namespace"
        else:
            # coordinator info
            resource[self._coordinator_name] = "Deployment"
            resource[self._coordinator_service_name] = "Service"
        self._resource_object.dump(extra_resource=resource)

    def _get_analytical_hosts(self):
        pod_name_list = self._pod_name_list
        if self._analytical_pod_name:
            pod_name_list = self._analytical_pod_name
        return pod_name_list

    def _allocate_analytical_engine(self):
        # check the engine flag
        if self._with_analytical and self._with_analytical_java:
            logger.info(
                "Analytical engine and analytical engine(java) cannot be enabled at the same time. "
                "Proceeding to only enable analytical_java."
            )
            self._with_analytical = False
        elif not (self._with_analytical or self._with_analytical_java):
            raise NotImplementedError(
                "Neither analytical engine nor analytical engine(java) is enabled."
            )

        # allocate analytical engine based on the mode
        if self._deploy_mode == "eager":
            return self._pod_name_list, self._pod_ip_list, self._pod_host_ip_list
        else:
            if self._with_analytical:
                return self.deploy_analytical_engine()
            else:
                return self.deploy_analytical_java_engine()

    def _distribute_analytical_process(self, pod_name_list, pod_ip_list):
        # generate and distribute hostfile
        hosts = os.path.join(get_tempdir(), "hosts_of_nodes")
        with open(hosts, "w") as f:
            for i, pod_ip in enumerate(pod_ip_list):
                f.write(f"{pod_ip} {pod_name_list[i]}\n")

        container = self._engine_cluster.analytical_container_name
        for pod in pod_name_list:
            logger.debug(
                run_kube_cp_command(hosts, "/tmp/hosts_of_nodes", pod, container, True)
            )

        # launch engine
        rmcp = ResolveMPICmdPrefix(rsh_agent=True)
        cmd, mpi_env = rmcp.resolve(self._num_workers, ",".join(pod_name_list))

        cmd.append(ANALYTICAL_ENGINE_PATH)
        cmd.extend(["--host", "0.0.0.0"])
        cmd.extend(["--port", str(self._random_analytical_engine_rpc_port)])

        cmd.extend(["-v", str(self._glog_level)])
        mpi_env["GLOG_v"] = str(self._glog_level)

        cmd.extend(["--vineyard_socket", self._engine_cluster.vineyard_ipc_socket])
        logger.info("Analytical engine launching command: %s", " ".join(cmd))

        env = os.environ.copy()
        env["GRAPHSCOPE_HOME"] = GRAPHSCOPE_HOME
        env.update(mpi_env)

        self._analytical_engine_process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="replace",
            universal_newlines=True,
            bufsize=1,
        )

        stdout_watcher = PipeWatcher(
            self._analytical_engine_process.stdout, sys.stdout, drop=True
        )
        stderr_watcher = PipeWatcher(
            self._analytical_engine_process.stderr, sys.stderr, drop=True
        )
        setattr(self._analytical_engine_process, "stdout_watcher", stdout_watcher)
        setattr(self._analytical_engine_process, "stderr_watcher", stderr_watcher)

    def create_analytical_instance(self):
        pod_name_list, pod_ip_list, _ = self._allocate_analytical_engine()
        if not pod_name_list or not pod_ip_list:
            raise RuntimeError("Failed to allocate analytical engine.")
        logger.info(
            "Starting GAE rpc service on %s ...", self._analytical_engine_endpoint
        )

        self._distribute_analytical_process(pod_name_list, pod_ip_list)

    def _delete_dangling_coordinator(self):
        # delete service
        try:
            self._core_api.delete_namespaced_service(
                self._coordinator_service_name, self._namespace
            )
        except K8SApiException as ex:
            if ex.status == 404:
                logger.warning(
                    "coordinator service %s not found", self._coordinator_service_name
                )
            else:
                logger.exception(
                    "Deleting dangling coordinator service %s failed",
                    self._coordinator_service_name,
                )
        try:
            self._apps_api.delete_namespaced_deployment(
                self._coordinator_name, self._namespace
            )
        except K8SApiException as ex:
            if ex.status == 404:
                logger.warning(
                    "coordinator deployment %s not found", self._coordinator_name
                )
            else:
                logger.exception(
                    "Deleting dangling coordinator %s failed", self._coordinator_name
                )

        if self._waiting_for_delete:
            start_time = time.time()
            while True:
                try:
                    self._apps_api.read_namespaced_deployment(
                        self._coordinator_name, self._namespace
                    )
                except K8SApiException as ex:
                    if ex.status != 404:
                        logger.exception(
                            "Deleting dangling coordinator %s failed",
                            self._coordinator_name,
                        )
                    break
                else:
                    if time.time() - start_time > self._timeout_seconds:
                        logger.error(
                            "Deleting dangling coordinator %s timeout",
                            self._coordinator_name,
                        )
                    time.sleep(self._del_retry_time_seconds)

    def _get_owner_reference_as_json(self):
        owner_reference = [
            {
                "apiVersion": self._owner_references[0].api_version,
                "kind": self._owner_references[0].kind,
                "name": self._owner_references[0].name,
                "uid": self._owner_references[0].uid,
            }
        ]
        owner_reference_json = json.dumps(owner_reference)
        return owner_reference_json

    def _check_if_vineyard_deployment_exist(self):
        try:
            self._apps_api.read_namespaced_deployment(
                self._vineyard_deployment, self._namespace
            )
        except K8SApiException:
            logger.info(
                "Vineyard deployment %s/%s not exist",
                self._namespace,
                self._vineyard_deployment,
            )
            return False
        return True

    def _deploy_vineyard_deployment_if_not_exist(self):
        if not self._check_if_vineyard_deployment_exist():
            self._deploy_vineyard_deployment()
        else:
            logger.info(
                "The external vineyard deployment %s is ready."
                "Please make sure the type of the vineyard rpc service is the same as %s.",
                self._vineyard_deployment,
                self._service_type,
            )

    def _deploy_vineyard_deployment(self):
        import vineyard

        owner_reference_json = self._get_owner_reference_as_json()
        vineyard.deploy.vineyardctl.deploy.vineyard_deployment(
            kubeconfig=self._k8s_config_file,
            name=self._vineyard_deployment,
            namespace=self._namespace,
            replicas=self._num_workers,
            etcd_replicas=self._num_workers,
            vineyardd_image=self._vineyard_image,
            vineyardd_memory=self._vineyard_mem,
            vineyardd_cpu=self._vineyard_cpu,
            vineyardd_size=self._vineyard_shared_mem,
            vineyardd_service_type=self._service_type,
            owner_references=owner_reference_json,
        )
        vineyard_pods = self._core_api.list_namespaced_pod(
            self._namespace,
            label_selector="app.kubernetes.io/instance="
            + self._namespace
            + "-"
            + self._vineyard_deployment,
        )
        for pod in vineyard_pods.items:
            self._vineyard_pod_name_list.append(pod.metadata.name)

    def start(self):
        if self._serving:
            return True
        try:
            if self._deploy_mode == "eager":
                self._create_services()
                self._waiting_for_services_ready()
                self._dump_resource_object()
                self._serving = True
        except Exception:  # pylint: disable=broad-except
            time.sleep(1)
            logger.exception("Error when launching GraphScope on kubernetes cluster")
            self.stop()
            return False
        return True

    def stop(self, is_dangling=False):
        if self._serving:
            logger.info("Cleaning up kubernetes resources")
            for target in self._resource_object:
                delete_kubernetes_object(
                    api_client=self._api_client,
                    target=target,
                    wait=self._waiting_for_delete,
                    timeout_seconds=self._timeout_seconds,
                )
            self._resource_object.clear()

            if is_dangling:
                logger.info("Dangling coordinator detected, cleaning up...")
                # delete everything inside namespace of graphscope instance
                if self._delete_namespace:
                    # delete namespace created by graphscope
                    self._core_api.delete_namespace(self._namespace)
                    if self._waiting_for_delete:
                        start_time = time.time()
                        while True:
                            try:
                                self._core_api.read_namespace(self._namespace)
                            except K8SApiException as ex:
                                if ex.status != 404:
                                    logger.exception(
                                        "Deleting dangling namespace %s failed",
                                        self._namespace,
                                    )
                                break
                            else:
                                if time.time() - start_time > self._timeout_seconds:
                                    logger.error(
                                        "Deleting namespace %s timeout", self._namespace
                                    )
                                time.sleep(self._del_retry_time_seconds)

                else:
                    # delete coordinator deployment and service
                    self._delete_dangling_coordinator()
            self._serving = False
            logger.info("Kubernetes launcher stopped")

    def _allocate_learning_engine(self, object_id):
        # check the learning engine flag
        if not self._with_learning:
            raise NotImplementedError("Learning engine not enabled")

        # allocate learning engine based on the mode
        if self._deploy_mode == "eager":
            return self._pod_name_list, self._pod_ip_list, self._pod_host_ip_list
        return self.deploy_learning_engine(object_id)

    def _distribute_learning_process(
        self, pod_name_list, pod_host_ip_list, object_id, handle, config
    ):
        # allocate service for ports
        # prepare arguments
        handle = json.loads(
            base64.b64decode(handle.encode("utf-8", errors="ignore")).decode(
                "utf-8", errors="ignore"
            )
        )
        hosts = ",".join(
            [
                f"{pod_name}:{port}"
                for pod_name, port in zip(
                    pod_name_list,
                    self._engine_cluster.get_learning_ports(self._learning_start_port),
                )
            ]
        )
        handle["server"] = hosts
        handle = base64.b64encode(
            json.dumps(handle).encode("utf-8", errors="ignore")
        ).decode("utf-8", errors="ignore")

        # launch the server
        self._learning_instance_processes[object_id] = []
        for pod_index, pod in enumerate(self._pod_name_list):
            container = self._engine_cluster.learning_container_name
            sub_cmd = f"python3 -m gscoordinator.learning {handle} {config} {pod_index}"
            cmd = f"kubectl -n {self._namespace} exec -it -c {container} {pod} -- {sub_cmd}"
            logger.debug("launching learning server: %s", " ".join(cmd))
            proc = subprocess.Popen(
                shlex.split(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                encoding="utf-8",
                errors="replace",
                universal_newlines=True,
                bufsize=1,
            )
            stdout_watcher = PipeWatcher(
                proc.stdout,
                sys.stdout,
                drop=True,
                suppressed=(not logger.isEnabledFor(logging.DEBUG)),
            )
            setattr(proc, "stdout_watcher", stdout_watcher)
            self._learning_instance_processes[object_id].append(proc)

        # Create Service
        self._create_learning_service(object_id)
        # update the port usage record
        self._learning_start_port += len(pod_name_list)
        # parse the service hosts and ports
        return self._engine_cluster.get_graphlearn_service_endpoint(
            self._api_client, object_id, pod_host_ip_list
        )

    def create_learning_instance(self, object_id, handle, config):
        pod_name_list, _, pod_host_ip_list = self._allocate_learning_engine(object_id)
        if not pod_name_list or not pod_host_ip_list:
            raise RuntimeError("Failed to allocate learning engine")
        return self._distribute_learning_process(
            pod_name_list, pod_host_ip_list, object_id, handle, config
        )

    def close_learning_instance(self, object_id):
        if self._deploy_mode == "lazy":
            self.delete_learning_engine(object_id)
            return
        if object_id not in self._learning_instance_processes:
            return
        # delete the services
        target = self._graphlearn_services[object_id]
        try:
            delete_kubernetes_object(
                api_client=self._api_client,
                target=target,
                wait=self._waiting_for_delete,
                timeout_seconds=self._timeout_seconds,
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to delete graphlearn service for %s", object_id)

        # terminate the process
        for proc in self._learning_instance_processes[object_id]:
            try:
                proc.terminate()
                proc.wait(1)
            except Exception:  # pylint: disable=broad-except
                logger.exception("Failed to terminate graphlearn server")
        self._learning_instance_processes[object_id].clear()


class ResourceManager(object):
    """A class to manager kubernetes object.

    Object managed by this class will dump meta info to disk file
    for pod preStop lifecycle management.

    meta info format:

        {
            "my-deployment": "Deployment",
            "my-service": "Service"
        }
    """

    _resource_object_path = os.path.join(get_tempdir(), "resource_object")  # fixed

    def __init__(self, api_client):
        self._api_client = api_client
        self._resource_object = []
        self._meta_info = {}

    def append(self, target):
        self._resource_object.append(target)
        self._meta_info.update(
            get_kubernetes_object_info(api_client=self._api_client, target=target)
        )
        self.dump()

    def extend(self, targets):
        self._resource_object.extend(targets)
        for target in targets:
            self._meta_info.update(
                get_kubernetes_object_info(api_client=self._api_client, target=target)
            )
        self.dump()

    def clear(self):
        self._resource_object.clear()
        self._meta_info.clear()

    def __str__(self):
        return str(self._meta_info)

    def __getitem__(self, index):
        return self._resource_object[index]

    def dump(self, extra_resource=None):
        """Dump meta info to disk file.
        Args:
            extra_resource (dict): extra resource to dump.
                A typical scenario is dumping meta info of namespace
                for coordinator dangling processing.
        """
        if extra_resource is not None:
            rlt = copy.deepcopy(self._meta_info)
            rlt.update(extra_resource)
        else:
            rlt = self._meta_info
        with open(self._resource_object_path, "w") as f:
            json.dump(rlt, f)
