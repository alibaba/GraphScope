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
import json
import logging
import os
import random
import subprocess
import sys
import time
import uuid

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

from graphscope.deploy.kubernetes.resource_builder import EmptyDirVolumeBuilder
from graphscope.deploy.kubernetes.resource_builder import GSEngineBuilder
from graphscope.deploy.kubernetes.resource_builder import GSEtcdBuilder
from graphscope.deploy.kubernetes.resource_builder import GSGraphManagerBuilder
from graphscope.deploy.kubernetes.resource_builder import HostPathVolumeBuilder
from graphscope.deploy.kubernetes.resource_builder import ServiceBuilder
from graphscope.deploy.kubernetes.utils import delete_kubernetes_object
from graphscope.deploy.kubernetes.utils import get_kubernetes_object_info
from graphscope.deploy.kubernetes.utils import get_service_endpoints
from graphscope.framework.utils import random_string
from graphscope.proto import types_pb2

from gscoordinator.io_utils import PipeWatcher
from gscoordinator.launcher import Launcher
from gscoordinator.utils import ResolveMPICmdPrefix
from gscoordinator.utils import parse_as_glog_level

logger = logging.getLogger("graphscope")


class KubernetesClusterLauncher(Launcher):
    _gs_etcd_builder_cls = GSEtcdBuilder
    _gs_engine_builder_cls = GSEngineBuilder
    _gs_graph_manager_builder_cls = GSGraphManagerBuilder

    _etcd_name_prefix = "gs-etcd-"
    _etcd_service_name_prefix = "gs-etcd-service-"
    _engine_name_prefix = "gs-engine-"
    _vineyard_service_name_prefix = "gs-vineyard-service-"
    _gie_graph_manager_name_prefix = "gs-graphmanager-"
    _gie_graph_manager_service_name_prefix = "gs-graphmanager-service-"
    _gle_service_name_prefix = "gs-graphlearn-service-"

    _analytical_engine_exec = "grape_engine"
    _vineyard_container_name = "vineyard"  # fixed
    _etcd_container_name = "etcd"
    _engine_container_name = "engine"  # fixed
    _gie_manager_container_name = "manager"
    _gie_zookeeper_container_name = "zookeeper"

    _interactive_engine_manager_port = 8080  # fixed
    _zookeeper_port = 2181  # fixed
    _random_analytical_engine_rpc_port = random.randint(56001, 57000)
    _random_etcd_listen_peer_service_port = random.randint(57001, 58000)
    _random_etcd_listen_client_service_port = random.randint(58001, 59000)

    _vineyard_service_port = 9600  # fixed

    _resource_object_path = "/tmp/resource_object"  # fixed

    def __init__(
        self,
        namespace=None,
        service_type=None,
        gs_image=None,
        etcd_image=None,
        zookeeper_image=None,
        gie_graph_manager_image=None,
        coordinator_name=None,
        coordinator_service_name=None,
        engine_cpu=None,
        engine_mem=None,
        vineyard_cpu=None,
        vineyard_mem=None,
        vineyard_shared_mem=None,
        image_pull_policy=None,
        image_pull_secrets=None,
        num_workers=None,
        log_level=None,
        timeout_seconds=None,
        waiting_for_delete=None,
        delete_namespace=None,
        **kwargs
    ):
        try:
            kube_config.load_incluster_config()
        except:  # noqa: E722
            kube_config.load_kube_config()
        self._api_client = kube_client.ApiClient()
        self._core_api = kube_client.CoreV1Api(self._api_client)
        self._app_api = kube_client.AppsV1Api(self._api_client)

        # random for multiple k8s cluster in the same namespace
        self._engine_name = self._engine_name_prefix + random_string(6)
        self._etcd_name = self._etcd_name_prefix + random_string(6)
        self._etcd_service_name = self._etcd_service_name_prefix + random_string(6)

        self._gie_graph_manager_name = (
            self._gie_graph_manager_name_prefix + random_string(6)
        )
        self._gie_graph_manager_service_name = (
            self._gie_graph_manager_service_name_prefix + random_string(6)
        )
        self._vineyard_service_name = (
            self._vineyard_service_name_prefix + random_string(6)
        )

        self._namespace = namespace
        self._service_type = service_type
        self._num_workers = num_workers

        self._coordinator_name = coordinator_name
        self._coordinator_service_name = coordinator_service_name

        self._resource_object = []

        # engine container info
        self._gs_image = gs_image
        self._engine_cpu = engine_cpu
        self._engine_mem = engine_mem

        # vineyard container info
        self._vineyard_cpu = vineyard_cpu
        self._vineyard_mem = vineyard_mem
        self._vineyard_shared_mem = vineyard_shared_mem

        # etcd pod info
        self._etcd_image = etcd_image

        # zookeeper pod info
        self._zookeeper_image = zookeeper_image

        # interactive engine graph manager info
        self._gie_graph_manager_image = gie_graph_manager_image

        self._image_pull_policy = image_pull_policy

        # image pull secrets
        self._etcd_endpoint = None
        if image_pull_secrets is not None:
            self._image_pull_secrets = image_pull_secrets.split(",")
        else:
            self._image_pull_secrets = []

        self._host0 = None
        self._pod_name_list = None
        self._pod_ip_list = None
        self._pod_host_ip_list = None

        self._analytical_engine_endpoint = None
        self._vineyard_service_endpoint = None

        self._closed = False
        self._glog_level = parse_as_glog_level(log_level)
        self._timeout_seconds = timeout_seconds
        self._waiting_for_delete = waiting_for_delete
        self._delete_namespace = delete_namespace

        self._analytical_engine_process = None

        # 8000 ~ 9000 is exposed
        self._learning_engine_ports_usage = 8000
        self._graphlearn_services = dict()
        self._learning_instance_processes = {}

    def __del__(self):
        self.stop()

    def type(self):
        return types_pb2.K8S

    def get_vineyard_service_name(self):
        return self._vineyard_service_name

    def get_vineyard_rpc_endpoint(self):
        return self._vineyard_service_endpoint

    def get_pods_list(self):
        return self._pod_name_list

    def get_namespace(self):
        return self._namespace

    def get_gie_graph_manager_service_name(self):
        return self._gie_graph_manager_service_name

    def _create_engine_replicaset(self):
        logger.info("Launching GraphScope engines pod ...")
        targets = []
        labels = {"name": self._engine_name}
        # create engine replicaset
        engine_builder = self._gs_engine_builder_cls(
            name=self._engine_name,
            labels=labels,
            num_workers=self._num_workers,
            image_pull_policy=self._image_pull_policy,
        )
        # volume1 is for vineyard ipc socket
        # MaxGraph: /home/maxgraph/data/vineyard
        engine_builder.add_volume(
            EmptyDirVolumeBuilder(
                name="vineyard-ipc-volume",
                mount_path=["/tmp/vineyard_workspace", "/home/maxgraph/data/vineyard"],
            )
        )
        # volume2 is for shared memory
        engine_builder.add_volume(
            EmptyDirVolumeBuilder(
                name="host-shm", mount_path=["/dev/shm"], use_memory=True
            )
        )
        # volume3 is for CI test, mount ${GS_TEST_DIR} to /testingdata
        if "GS_TEST_DIR" in os.environ:
            engine_builder.add_volume(
                HostPathVolumeBuilder(
                    name="gstest",
                    mount_path=["/testingdata"],
                    host_path=os.environ["GS_TEST_DIR"],
                    volume_type="Directory",
                )
            )
        # add env
        engine_builder.add_simple_envs({"GLOG_v": str(self._glog_level)})
        # add vineyard container
        engine_builder.add_vineyard_container(
            self._vineyard_container_name,
            self._gs_image,
            self._vineyard_cpu,
            self._vineyard_mem,
            self._vineyard_shared_mem,
            self._etcd_endpoint,
            self._vineyard_service_port,
        )
        # add engine container
        engine_builder.add_engine_container(
            self._engine_container_name,
            self._gs_image,
            self._engine_cpu,
            self._engine_mem,
        )
        for name in self._image_pull_secrets:
            engine_builder.add_image_pull_secret(name)
        targets.append(
            self._app_api.create_namespaced_replica_set(
                self._namespace, engine_builder.build()
            )
        )

        self._resource_object.extend(targets)

    def _create_etcd(self):
        logger.info("Launching etcd ...")
        targets = []
        labels = {"name": self._etcd_name}
        # should create service first
        service_builder = ServiceBuilder(
            self._etcd_service_name,
            service_type="ClusterIP",
            port=self._random_etcd_listen_client_service_port,
            selector=labels,
        )
        targets.append(
            self._core_api.create_namespaced_service(
                self._namespace, service_builder.build()
            )
        )

        time.sleep(1)

        # create etcd deployment
        etcd_builder = self._gs_etcd_builder_cls(
            name=self._etcd_name,
            labels=labels,
            replicas=1,
            image_pull_policy=self._image_pull_policy,
        )

        for name in self._image_pull_secrets:
            etcd_builder.add_image_pull_secret(name)

        etcd_builder.add_etcd_container(
            name=self._etcd_container_name,
            service_name=self._etcd_service_name,
            image=self._etcd_image,
            listen_peer_service_port=self._random_etcd_listen_peer_service_port,
            listen_client_service_port=self._random_etcd_listen_client_service_port,
        )
        targets.append(
            self._app_api.create_namespaced_deployment(
                self._namespace, etcd_builder.build()
            )
        )

        self._resource_object.extend(targets)

    def _create_vineyard_service(self):
        targets = []
        labels = {"name": self._engine_name}  # vineyard in engine pod
        service_builder = ServiceBuilder(
            self._vineyard_service_name,
            service_type=self._service_type,
            port=self._vineyard_service_port,
            selector=labels,
        )
        targets.append(
            self._core_api.create_namespaced_service(
                self._namespace, service_builder.build()
            )
        )

        self._resource_object.extend(targets)

    def _get_vineyard_service_endpoint(self):
        # Always len(endpoints) >= 1
        endpoints = get_service_endpoints(
            api_client=self._api_client,
            namespace=self._namespace,
            name=self._vineyard_service_name,
            type=self._service_type,
        )
        return endpoints[0]

    def _create_graphlearn_service(self, object_id, start_port, num_workers):
        targets = []
        labels = {"name": self._engine_name}
        service_builder = ServiceBuilder(
            self._gle_service_name_prefix + str(object_id),
            service_type=self._service_type,
            port=list(range(start_port, start_port + num_workers)),
            selector=labels,
            external_traffic_policy="Local",
        )
        targets.append(
            self._core_api.create_namespaced_service(
                self._namespace, service_builder.build()
            )
        )
        self._graphlearn_services[object_id] = targets
        self._resource_object.extend(targets)

    def _parse_graphlearn_service_endpoint(self, object_id):
        if self._service_type == "NodePort":
            services = self._core_api.list_namespaced_service(self._namespace)
            for svc in services.items:
                if svc.metadata.name == self._gle_service_name_prefix + str(object_id):
                    endpoints = []
                    for ip, port_spec in zip(self._pod_host_ip_list, svc.spec.ports):
                        endpoints.append(
                            (
                                "%s:%s" % (ip, port_spec.node_port),
                                int(port_spec.name.split("-")[-1]),
                            )
                        )
                    endpoints.sort(key=lambda ep: ep[1])
                    return [ep[0] for ep in endpoints]
        elif self._service_type == "LoadBalancer":
            endpoints = get_service_endpoints(
                api_client=self._api_client,
                namespace=self._namespace,
                name=self._gle_service_name_prefix + str(object_id),
                type=self._service_type,
            )
            return endpoints
        raise RuntimeError("Get graphlearn service endpoint failed.")

    def _create_interactive_engine_service(self):
        logger.info("Launching GIE graph manager ...")
        targets = []
        labels = {"app": self._gie_graph_manager_name}
        service_builder = ServiceBuilder(
            name=self._gie_graph_manager_service_name,
            service_type="ClusterIP",
            port=self._interactive_engine_manager_port,
            selector=labels,
        )
        targets.append(
            self._core_api.create_namespaced_service(
                self._namespace, service_builder.build()
            )
        )

        time.sleep(1)

        # create graph manager deployment
        graph_manager_builder = self._gs_graph_manager_builder_cls(
            name=self._gie_graph_manager_name,
            labels=labels,
            replicas=1,
            image_pull_policy=self._image_pull_policy,
        )
        for name in self._image_pull_secrets:
            graph_manager_builder.add_image_pull_secret(name)

        envs = {
            "GREMLIN_IMAGE": self._gie_graph_manager_image,
            "ENGINE_NAMESPACE": self._namespace,
            "COORDINATOR_IMAGE": self._gie_graph_manager_image,
        }
        graph_manager_builder.add_simple_envs(envs)

        # add manager container
        graph_manager_builder.add_manager_container(
            name=self._gie_manager_container_name,
            image=self._gie_graph_manager_image,
            port=self._interactive_engine_manager_port,
        )

        # add zookeeper container
        graph_manager_builder.add_zookeeper_container(
            name=self._gie_zookeeper_container_name,
            image=self._zookeeper_image,
            port=self._zookeeper_port,
        )

        targets.append(
            self._app_api.create_namespaced_deployment(
                self._namespace, graph_manager_builder.build()
            )
        )

        self._resource_object.extend(targets)

    def _waiting_interactive_engine_service_ready(self):
        start_time = time.time()
        event_messages = []
        while True:
            deployments = self._app_api.list_namespaced_deployment(self._namespace)
            service_available = False
            for deployment in deployments.items:
                if deployment.metadata.name == self._gie_graph_manager_name:
                    # replicas is 1
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

            if service_available:
                break
            if (
                self._timeout_seconds
                and self._timeout_seconds + start_time < time.time()
            ):
                raise TimeoutError("Waiting GIE graph manager start timeout.")
            time.sleep(2)
        logger.info("GIE graph manager service is ready.")

    def _create_services(self):
        # create interactive engine service
        self._create_interactive_engine_service()
        self._waiting_interactive_engine_service_ready()

        # etcd used by vineyard
        self._create_etcd()
        self._etcd_endpoint = self._get_etcd_service_endpoint()
        logger.info("Etcd is ready, endpoint is {}".format(self._etcd_endpoint))

        self._create_engine_replicaset()
        self._create_vineyard_service()

    def _waiting_for_services_ready(self):
        start_time = time.time()
        event_messages = []
        while True:
            replicasets = self._app_api.list_namespaced_replica_set(
                namespace=self._namespace
            )
            service_available = False
            for rs in replicasets.items:
                if rs.metadata.name == self._engine_name:
                    # logger.info(
                    # "Engine pod: {} ready / {} total".format(
                    # str(rs.status.ready_replicas), self._num_workers
                    # )
                    # )
                    if rs.status.ready_replicas == self._num_workers:
                        # service is ready
                        service_available = True
                        break

                    # check container status
                    selector = ""
                    for k, v in rs.spec.selector.match_labels.items():
                        selector += k + "=" + v + ","
                    selector = selector[:-1]

                    pods = self._core_api.list_namespaced_pod(
                        namespace=self._namespace, label_selector=selector
                    )

                    for pod in pods.items:
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

            if service_available:
                break
            if (
                self._timeout_seconds
                and self._timeout_seconds + start_time < time.time()
            ):
                raise TimeoutError("GraphScope Engines launching timeout.")
            time.sleep(2)

        self._pod_name_list = []
        self._pod_ip_list = []
        self._pod_host_ip_list = []
        pods = self._core_api.list_namespaced_pod(
            namespace=self._namespace,
            label_selector="name=%s" % self._engine_name,
        )
        for pod in pods.items:
            self._pod_name_list.append(pod.metadata.name)
            self._pod_ip_list.append(pod.status.pod_ip)
            self._pod_host_ip_list.append(pod.status.host_ip)
        assert len(self._pod_ip_list) >= 1
        self._host0 = self._pod_ip_list[0]
        self._analytical_engine_endpoint = "{}:{}".format(
            self._host0, self._random_analytical_engine_rpc_port
        )

        # get vineyard service endpoint
        self._vineyard_service_endpoint = self._get_vineyard_service_endpoint()
        logger.info("GraphScope engines pod is ready.")

    def _dump_cluster_logs(self):
        log_dict = dict()
        pod_items = self._core_api.list_namespaced_pod(self._namespace).to_dict()
        for item in pod_items["items"]:
            log_dict[item["metadata"]["name"]] = self._core_api.read_namespaced_pod_log(
                name=item["metadata"]["name"], namespace=self._namespace
            )
        return log_dict

    def _dump_resource_object(self):
        rlt = {}
        for target in self._resource_object:
            rlt.update(
                get_kubernetes_object_info(api_client=self._api_client, target=target)
            )
        if self._delete_namespace:
            rlt[self._namespace] = "Namespace"
        else:
            # coordinator info
            rlt[self._coordinator_name] = "Deployment"
            rlt[self._coordinator_service_name] = "Service"

        with open(self._resource_object_path, "w") as f:
            json.dump(rlt, f)

    def _get_etcd_service_endpoint(self):
        # Always len(endpoints) >= 1
        endpoints = get_service_endpoints(
            api_client=self._api_client,
            namespace=self._namespace,
            name=self._etcd_service_name,
            type="ClusterIP",
        )
        return endpoints[0]

    def _launch_analytical_engine_locally(self):
        logger.info(
            "Starting GAE rpc service on {} ...".format(
                str(self._analytical_engine_endpoint)
            )
        )

        # generate and distribute hostfile
        with open("/tmp/kube_hosts", "w") as f:
            for i in range(len(self._pod_ip_list)):
                f.write("{} {}\n".format(self._pod_ip_list[i], self._pod_name_list[i]))

        for pod in self._pod_name_list:
            subprocess.check_call(
                [
                    "kubectl",
                    "-n",
                    self._namespace,
                    "cp",
                    "/tmp/kube_hosts",
                    "{}:/etc/hosts_of_nodes".format(pod),
                    "-c",
                    self._engine_container_name,
                ]
            )

        # launch engine
        rmcp = ResolveMPICmdPrefix(rsh_agent=True)
        cmd, mpi_env = rmcp.resolve(self._num_workers, ",".join(self._pod_name_list))

        cmd.append(self._analytical_engine_exec)
        cmd.extend(["--host", "0.0.0.0"])
        cmd.extend(["--port", str(self._random_analytical_engine_rpc_port)])

        if rmcp.openmpi():
            cmd.extend(["-v", str(self._glog_level)])
        else:
            mpi_env["GLOG_v"] = str(self._glog_level)

        cmd.extend(["--vineyard_socket", "/tmp/vineyard_workspace/vineyard.sock"])
        logger.debug("Analytical engine launching command: {}".format(" ".join(cmd)))

        env = os.environ.copy()
        env.update(mpi_env)

        self._analytical_engine_process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
        )

        stdout_watcher = PipeWatcher(
            self._analytical_engine_process.stdout, sys.stdout, drop=True
        )
        setattr(self._analytical_engine_process, "stdout_watcher", stdout_watcher)

    def _delete_dangling_coordinator(self):
        # delete service
        self._core_api.delete_namespaced_service(
            name=self._coordinator_service_name, namespace=self._namespace
        )
        self._app_api.delete_namespaced_deployment(
            name=self._coordinator_name, namespace=self._namespace
        )
        if self._waiting_for_delete:
            start_time = time.time()
            while True:
                try:
                    self._app_api.read_namespaced_deployment(
                        name=self._coordinator_name, namespace=self._namespace
                    )
                except K8SApiException as ex:
                    if ex.status != 404:
                        logger.error(
                            "Deleting dangling coordinator {} failed: {}".format(
                                self._coordinator_name, str(ex)
                            )
                        )
                    break
                else:
                    time.sleep(1)
                    if time.time() - start_time > self._timeout_seconds:
                        logger.error(
                            "Deleting dangling coordinator {} timeout".format(
                                self._coordinator_name
                            )
                        )

    def start(self):
        try:
            self._create_services()
            self._waiting_for_services_ready()
            self._dump_resource_object()
            logger.info("Engines pod name list: {}".format(self._pod_name_list))
            logger.info("Engines pod ip list: {}".format(self._pod_ip_list))
            logger.info("Engines pod host ip list: {}".format(self._pod_host_ip_list))
            logger.info(
                "Vineyard service endpoint: {}".format(self._vineyard_service_endpoint)
            )
            self._launch_analytical_engine_locally()
        except Exception as e:
            time.sleep(1)
            logger.error(
                "Error when launching GraphScope on kubernetes cluster: %s",
                str(e),
            )
            self.stop()
            return False
        return True

    def stop(self, is_dangling=False):
        if not self._closed:
            for target in self._resource_object:
                delete_kubernetes_object(
                    api_client=self._api_client,
                    target=target,
                    wait=self._waiting_for_delete,
                    timeout_seconds=self._timeout_seconds,
                )
            self._resource_object = []

            if is_dangling:
                logger.info("Dangling coordinator detected, clean up soon.")
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
                                    logger.error(
                                        "Deleting dangling namespace {} failed: {}".format(
                                            self._namespace, str(ex)
                                        )
                                    )
                                break
                            else:
                                time.sleep(1)
                                if time.time() - start_time > self._timeout_seconds:
                                    logger.error(
                                        "Deleting namespace %s timeout"
                                        % self._namespace
                                    )
                else:
                    # delete coordinator deployment and service
                    self._delete_dangling_coordinator()
            self._closed = True

    def poll(self):
        if self._analytical_engine_process:
            return self._analytical_engine_process.poll()
        return -1

    def create_learning_instance(self, object_id, handle, config):
        # allocate service for ports
        self._create_graphlearn_service(
            object_id, self._learning_engine_ports_usage, len(self._pod_name_list)
        )

        # prepare arguments
        handle = json.loads(base64.b64decode(handle.encode("utf-8")).decode("utf-8"))
        hosts = ",".join(
            [
                "%s:%s" % (pod_name, port)
                for pod_name, port in zip(
                    self._pod_name_list,
                    range(
                        self._learning_engine_ports_usage,
                        self._learning_engine_ports_usage + len(self._pod_name_list),
                    ),
                )
            ]
        )
        handle["server"] = hosts
        handle = base64.b64encode(json.dumps(handle).encode("utf-8")).decode("utf-8")

        # launch the server
        self._learning_instance_processes[object_id] = []
        for pod_index, pod in enumerate(self._pod_name_list):
            cmd = [
                "kubectl",
                "-n",
                self._namespace,
                "exec",
                "-it",
                "-c",
                self._engine_container_name,
                pod,
                "--",
                "python3",
                "-m" "gscoordinator.learning",
                handle,
                config,
                str(pod_index),
            ]
            logging.info("launching learning server: %s", " ".join(cmd))
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            stdout_watcher = PipeWatcher(proc.stdout, sys.stdout, drop=True)
            setattr(proc, "stdout_watcher", stdout_watcher)
            self._learning_instance_processes[object_id].append(proc)

        # update the port usage record
        self._learning_engine_ports_usage += len(self._pod_name_list)

        # parse the service hosts and ports
        return self._parse_graphlearn_service_endpoint(object_id)

    def close_learning_instance(self, object_id):
        if object_id not in self._learning_instance_processes:
            return

        # delete the services
        for target in self._graphlearn_services[object_id]:
            try:
                delete_kubernetes_object(
                    api_client=self._api_client,
                    target=target,
                    wait=self._waiting_for_delete,
                    timeout_seconds=self._timeout_seconds,
                )
            except Exception as e:
                logger.error(
                    "Failed to delete graphlearn service for %s, %s", object_id, e
                )

        # terminate the process
        for proc in self._learning_instance_processes[object_id]:
            try:
                proc.terminate()
                proc.wait(1)
            except Exception as e:
                logger.error("Failed to terminate graphlearn server: %s", e)
        self._learning_instance_processes[object_id].clear()
