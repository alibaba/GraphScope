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
from gscoordinator.utils import run_command
from gscoordinator.version import __version__

logger = logging.getLogger("graphscope")


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
        vineyard_cpu=None,
        vineyard_daemonset=None,
        vineyard_image=None,
        vineyard_mem=None,
        vineyard_shared_mem=None,
        volumes=None,
        waiting_for_delete=None,
        with_mars=False,
        with_analytical=True,
        with_analytical_java=False,
        with_interactive=True,
        with_learning=True,
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

        self._image_registry = image_registry
        self._image_repository = image_repository
        self._image_tag = image_tag

        image_pull_secrets = image_pull_secrets.split(",") if image_pull_secrets else []

        self._glog_level = parse_as_glog_level(log_level)

        self._num_workers = num_workers

        self._vineyard_daemonset = vineyard_daemonset
        if vineyard_daemonset is not None:
            try:
                self._apps_api.read_namespaced_daemon_set(
                    vineyard_daemonset, self._namespace
                )
            except K8SApiException:
                logger.error(f"Vineyard daemonset {vineyard_daemonset} not found")
                self._vineyard_daemonset = None

        self._engine_cpu = engine_cpu
        self._engine_mem = engine_mem
        self._vineyard_shared_mem = vineyard_shared_mem

        self._with_dataset = with_dataset
        self._preemptive = preemptive
        self._service_type = service_type

        assert timeout_seconds is not None
        self._timeout_seconds = timeout_seconds

        self._waiting_for_delete = waiting_for_delete

        self._with_analytical = with_analytical
        self._with_analytical_java = with_analytical_java
        self._with_interactive = with_interactive
        self._with_learning = with_learning
        self._with_mars = with_mars
        self._mars_scheduler_cpu = mars_scheduler_cpu
        self._mars_scheduler_mem = mars_scheduler_mem
        self._mars_worker_cpu = mars_worker_cpu
        self._mars_worker_mem = mars_worker_mem

        self._pod_name_list = []
        self._pod_ip_list = None
        self._pod_host_ip_list = None

        self._analytical_engine_endpoint = None
        self._mars_service_endpoint = None

        self._serving = False

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

        self._engine_cluster = EngineCluster(
            engine_cpu=engine_cpu,
            engine_mem=engine_mem,
            engine_pod_node_selector=engine_pod_node_selector,
            glog_level=self._glog_level,
            image_pull_policy=image_pull_policy,
            image_pull_secrets=image_pull_secrets,
            image_registry=image_registry,
            image_repository=image_repository,
            image_tag=image_tag,
            instance_id=instance_id,
            with_dataset=with_dataset,
            namespace=namespace,
            num_workers=num_workers,
            preemptive=preemptive,
            service_type=service_type,
            vineyard_cpu=vineyard_cpu,
            vineyard_daemonset=vineyard_daemonset,
            vineyard_image=vineyard_image,
            vineyard_mem=vineyard_mem,
            vineyard_shared_mem=vineyard_shared_mem,
            volumes=volumes,
            with_mars=with_mars,
            with_analytical=with_analytical,
            with_analytical_java=with_analytical_java,
            with_interactive=with_interactive,
            with_learning=with_learning,
        )

        self._vineyard_service_endpoint = None
        self.vineyard_internal_service_endpoint = None
        self._mars_service_endpoint = None
        if self._with_mars:
            self._mars_cluster = MarsCluster(
                self._instance_id, self._namespace, self._service_type
            )

    def __del__(self):
        self.stop()

    def type(self):
        return types_pb2.K8S

    def waiting_for_delete(self):
        return self._waiting_for_delete

    def get_namespace(self):
        return self._namespace

    def get_vineyard_stream_info(self):
        hosts = [f"{self._namespace}:{host}" for host in self._pod_name_list]
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
        return self._pod_name_list

    def distribute_file(self, path):
        for pod in self._pod_name_list:
            container = self._engine_cluster.analytical_container_name
            try:
                # The library may exists in the analytical pod.
                test_cmd = f"test -f {path}"
                logger.debug(delegate_command_to_pod(test_cmd, pod, container))
                logger.info("Library exists, skip distribute")
            except RuntimeError:
                cmd = f"mkdir -p {os.path.dirname(path)}"
                logger.debug(delegate_command_to_pod(cmd, pod, container))
                cmd = f"kubectl cp {path} {pod}:{path} -c {container}"
                logger.debug(run_command(cmd))

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

    def create_interactive_instance(self, object_id: int, schema_path: str):
        if not self._with_interactive:
            raise NotImplementedError("Interactive engine not enabled")
        """
        Args:
            object_id (int): object id of the graph.
            schema_path (str): path of the schema file.
        """
        env = os.environ.copy()
        env["GRAPHSCOPE_HOME"] = GRAPHSCOPE_HOME
        container = self._engine_cluster.interactive_executor_container_name
        cmd = [
            INTERACTIVE_ENGINE_SCRIPT,
            "create_gremlin_instance_on_k8s",
            self._session_workspace,
            str(object_id),
            schema_path,
            self.hosts,
            container,
            str(self._interactive_port),  # executor port
            str(self._interactive_port + 1),  # executor rpc port
            str(self._interactive_port + 2),  # frontend port
            self._coordinator_name,
        ]
        self._interactive_port += 3
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

    def close_interactive_instance(self, object_id):
        env = os.environ.copy()
        env["GRAPHSCOPE_HOME"] = GRAPHSCOPE_HOME
        container = self._engine_cluster.interactive_executor_container_name
        cmd = [
            INTERACTIVE_ENGINE_SCRIPT,
            "close_gremlin_instance_on_k8s",
            self._session_workspace,
            str(object_id),
            self.hosts,
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
        response = self._apps_api.create_namespaced_deployment(
            self._namespace, deployment
        )
        self._resource_object.append(response)

    def _create_engine_stateful_set(self):
        logger.info("Create engine headless services...")
        service = self._engine_cluster.get_engine_headless_service()
        response = self._core_api.create_namespaced_service(self._namespace, service)
        self._resource_object.append(response)
        logger.info("Creating engine pods...")
        stateful_set = self._engine_cluster.get_engine_stateful_set()
        response = self._apps_api.create_namespaced_stateful_set(
            self._namespace, stateful_set
        )
        self._resource_object.append(response)

    def _create_frontend_deployment(self):
        logger.info("Creating frontend pods...")
        deployment = self._engine_cluster.get_interactive_frontend_deployment()
        response = self._apps_api.create_namespaced_deployment(
            self._namespace, deployment
        )
        self._resource_object.append(response)

    def _create_frontend_service(self):
        logger.info("Creating frontend service...")
        service = self._engine_cluster.get_interactive_frontend_service(8233)
        response = self._core_api.create_namespaced_service(self._namespace, service)
        self._resource_object.append(response)

    def _create_vineyard_service(self):
        logger.info("Creating vineyard service...")
        service = self._engine_cluster.get_vineyard_service()
        response = self._core_api.create_namespaced_service(self._namespace, service)
        self._resource_object.append(response)

    def _create_learning_service(self, object_id):
        logger.info("Creating learning service...")
        service = self._engine_cluster.get_learning_service(
            object_id, self._learning_start_port
        )
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
            self._create_frontend_deployment()
            # self._create_frontend_service()
        if self._with_mars:
            # scheduler used by Mars
            self._create_mars_scheduler()
        if self._vineyard_daemonset is None:
            self._create_vineyard_service()

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
                            timeout_seconds=1,
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
            time.sleep(2)

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
        self.vineyard_internal_endpoint = (
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

    def _dump_resource_object(self):
        resource = {}
        if self._delete_namespace:
            resource[self._namespace] = "Namespace"
        else:
            # coordinator info
            resource[self._coordinator_name] = "Deployment"
            resource[self._coordinator_service_name] = "Service"
        self._resource_object.dump(extra_resource=resource)

    def create_analytical_instance(self):
        if not (self._with_analytical or self._with_analytical_java):
            raise NotImplementedError("Analytical engine not enabled")
        logger.info(
            "Starting GAE rpc service on %s ...", self._analytical_engine_endpoint
        )

        # generate and distribute hostfile
        kube_hosts_path = os.path.join(get_tempdir(), "kube_hosts")
        with open(kube_hosts_path, "w") as f:
            for i, pod_ip in enumerate(self._pod_ip_list):
                f.write(f"{pod_ip} {self._pod_name_list[i]}\n")

        for pod in self._pod_name_list:
            container = self._engine_cluster.analytical_container_name
            cmd = f"kubectl -n {self._namespace} cp {kube_hosts_path} {pod}:/tmp/hosts_of_nodes -c {container}"
            cmd = shlex.split(cmd)
            subprocess.check_call(cmd)

        # launch engine
        rmcp = ResolveMPICmdPrefix(rsh_agent=True)
        cmd, mpi_env = rmcp.resolve(self._num_workers, ",".join(self._pod_name_list))

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
                    time.sleep(1)
                    if time.time() - start_time > self._timeout_seconds:
                        logger.error(
                            "Deleting dangling coordinator %s timeout",
                            self._coordinator_name,
                        )

    def start(self):
        if self._serving:
            return True
        try:
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
                                time.sleep(1)
                                if time.time() - start_time > self._timeout_seconds:
                                    logger.error(
                                        "Deleting namespace %s timeout", self._namespace
                                    )
                else:
                    # delete coordinator deployment and service
                    self._delete_dangling_coordinator()
            self._serving = False
            logger.info("Kubernetes launcher stopped")

    def create_learning_instance(self, object_id, handle, config):
        if not self._with_learning:
            raise NotImplementedError("Learning engine not enabled")
        # allocate service for ports
        # prepare arguments
        handle = json.loads(base64.b64decode(handle.encode("utf-8")).decode("utf-8"))
        hosts = ",".join(
            [
                f"{pod_name}:{port}"
                for pod_name, port in zip(
                    self._pod_name_list,
                    self._engine_cluster.get_learning_ports(self._learning_start_port),
                )
            ]
        )
        handle["server"] = hosts
        handle = base64.b64encode(json.dumps(handle).encode("utf-8")).decode("utf-8")

        # launch the server
        self._learning_instance_processes[object_id] = []
        for pod_index, pod in enumerate(self._pod_name_list):
            container = self._engine_cluster.learning_container_name
            sub_cmd = f"/opt/rh/rh-python38/root/usr/bin/python3 -m gscoordinator.learning {handle} {config} {pod_index}"
            cmd = f"kubectl -n {self._namespace} exec -it -c {container} {pod} -- {sub_cmd}"
            logging.debug("launching learning server: %s", " ".join(cmd))
            cmd = shlex.split(cmd)
            proc = subprocess.Popen(
                cmd,
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

        # update the port usage record
        self._learning_start_port += len(self._pod_name_list)
        # Create Service
        self._create_learning_service(object_id)

        # parse the service hosts and ports
        return self._engine_cluster.get_graphlearn_service_endpoint(
            self._api_client, object_id, self._pod_host_ip_list
        )

    def close_learning_instance(self, object_id):
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
