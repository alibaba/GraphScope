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
import shutil
import socket
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

from graphscope.deploy.kubernetes.resource_builder import GSEngineBuilder
from graphscope.deploy.kubernetes.resource_builder import GSEtcdBuilder
from graphscope.deploy.kubernetes.resource_builder import ServiceBuilder
from graphscope.deploy.kubernetes.resource_builder import VolumeBuilder
from graphscope.deploy.kubernetes.resource_builder import resolve_volume_builder
from graphscope.deploy.kubernetes.utils import delete_kubernetes_object
from graphscope.deploy.kubernetes.utils import get_kubernetes_object_info
from graphscope.deploy.kubernetes.utils import get_service_endpoints
from graphscope.deploy.kubernetes.utils import try_to_resolve_api_client
from graphscope.framework.utils import PipeWatcher
from graphscope.framework.utils import is_free_port
from graphscope.proto import types_pb2

from gscoordinator.launcher import Launcher
from gscoordinator.utils import ANALYTICAL_ENGINE_PATH
from gscoordinator.utils import GRAPHSCOPE_HOME
from gscoordinator.utils import INTERACTIVE_ENGINE_SCRIPT
from gscoordinator.utils import WORKSPACE
from gscoordinator.utils import ResolveMPICmdPrefix
from gscoordinator.utils import parse_as_glog_level
from gscoordinator.version import __version__

logger = logging.getLogger("graphscope")


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

    _resource_object_path = "/tmp/resource_object"  # fixed

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

    def dump(self):
        with open(self._resource_object_path, "w") as f:
            json.dump(self._meta_info, f)

    def dump_with_extra_resource(self, resource):
        """Also dump with extra resources. A typical scenario is
        dump meta info of namespace for coordinator dangling processing.
        """
        rlt = copy.deepcopy(self._meta_info)
        rlt.update(resource)
        with open(self._resource_object_path, "w") as f:
            json.dump(rlt, f)


class KubernetesClusterLauncher(Launcher):
    _gs_etcd_builder_cls = GSEtcdBuilder
    _gs_engine_builder_cls = GSEngineBuilder
    _gs_mars_scheduler_builder_cls = GSEngineBuilder

    _etcd_name_prefix = "gs-etcd-"
    _etcd_service_name_prefix = "gs-etcd-service-"
    _engine_name_prefix = "gs-engine-"
    _vineyard_service_name_prefix = "gs-vineyard-service-"
    _gle_service_name_prefix = "gs-graphlearn-service-"

    _vineyard_container_name = "vineyard"  # fixed
    _etcd_container_name = "etcd"
    _engine_container_name = "engine"  # fixed

    _mars_scheduler_container_name = "marsscheduler"  # fixed
    _mars_worker_container_name = "marsworker"  # fixed
    _mars_scheduler_name_prefix = "marsscheduler-"
    _mars_service_name_prefix = "mars-"

    _zookeeper_port = 2181  # fixed
    _random_analytical_engine_rpc_port = random.randint(56001, 57000)
    _random_etcd_listen_peer_service_port = random.randint(57001, 58000)
    _random_etcd_listen_client_service_port = random.randint(58001, 59000)

    _vineyard_service_port = 9600  # fixed
    _mars_scheduler_port = 7103  # fixed
    _mars_worker_port = 7104  # fixed

    def __init__(
        self,
        namespace=None,
        service_type=None,
        gs_image=None,
        etcd_image=None,
        dataset_image=None,
        coordinator_name=None,
        coordinator_service_name=None,
        etcd_num_pods=None,
        etcd_cpu=None,
        etcd_mem=None,
        engine_cpu=None,
        engine_mem=None,
        vineyard_daemonset=None,
        vineyard_cpu=None,
        vineyard_mem=None,
        vineyard_shared_mem=None,
        mars_worker_cpu=None,
        mars_worker_mem=None,
        mars_scheduler_cpu=None,
        mars_scheduler_mem=None,
        with_mars=False,
        image_pull_policy=None,
        image_pull_secrets=None,
        volumes=None,
        mount_dataset=None,
        num_workers=None,
        preemptive=None,
        instance_id=None,
        log_level=None,
        timeout_seconds=None,
        waiting_for_delete=None,
        delete_namespace=None,
        **kwargs
    ):

        super().__init__()
        self._api_client = try_to_resolve_api_client()
        self._core_api = kube_client.CoreV1Api(self._api_client)
        self._app_api = kube_client.AppsV1Api(self._api_client)

        self._saved_locals = locals()
        self._num_workers = self._saved_locals["num_workers"]
        self._instance_id = self._saved_locals["instance_id"]

        # random for multiple k8s cluster in the same namespace
        self._engine_name = self._engine_name_prefix + self._saved_locals["instance_id"]
        self._etcd_name = self._etcd_name_prefix + self._saved_locals["instance_id"]
        self._etcd_service_name = (
            self._etcd_service_name_prefix + self._saved_locals["instance_id"]
        )
        self._mars_scheduler_name = (
            self._mars_scheduler_name_prefix + self._saved_locals["instance_id"]
        )

        self._coordinator_name = coordinator_name
        self._coordinator_service_name = coordinator_service_name

        self._resource_object = ResourceManager(self._api_client)

        # etcd pod info
        self._etcd_num_pods = max(1, self._saved_locals["etcd_num_pods"])
        self._etcd_endpoint = None

        # image pull secrets
        if image_pull_secrets is not None:
            self._image_pull_secrets = image_pull_secrets.split(",")
        else:
            self._image_pull_secrets = []

        self._volumes = json.loads(volumes)

        self._host0 = None
        self._pod_name_list = None
        self._pod_ip_list = None
        self._pod_host_ip_list = None

        self._analytical_engine_endpoint = None
        self._vineyard_service_endpoint = None
        self._mars_service_endpoint = None

        self._closed = False
        self._glog_level = parse_as_glog_level(log_level)

        self._analytical_engine_process = None
        self._zetcd_process = None

        # 8000 ~ 9000 is exposed
        self._learning_engine_ports_usage = 8000
        self._graphlearn_services = dict()
        self._learning_instance_processes = {}

        # workspace
        self._instance_workspace = os.path.join(
            WORKSPACE, self._saved_locals["instance_id"]
        )
        os.makedirs(self._instance_workspace, exist_ok=True)
        self._session_workspace = None

        # component service name
        if self._exists_vineyard_daemonset(self._saved_locals["vineyard_daemonset"]):
            self._vineyard_service_name = (
                self._saved_locals["vineyard_daemonset"] + "-rpc"
            )
        else:
            self._vineyard_service_name = (
                self._vineyard_service_name_prefix + self._saved_locals["instance_id"]
            )
        self._mars_service_name = (
            self._mars_service_name_prefix + self._saved_locals["instance_id"]
        )

    def __del__(self):
        self.stop()

    def type(self):
        return types_pb2.K8S

    def get_vineyard_service_name(self):
        return self._vineyard_service_name

    def get_vineyard_rpc_endpoint(self):
        return self._vineyard_service_endpoint

    def get_mars_scheduler_endpoint(self):
        return self._mars_service_endpoint

    def get_pods_list(self):
        return self._pod_name_list

    def waiting_for_delete(self):
        return self._saved_locals["waiting_for_delete"]

    def get_namespace(self):
        return self._saved_locals["namespace"]

    def get_vineyard_stream_info(self):
        hosts = [
            "%s:%s" % (self._saved_locals["namespace"], host)
            for host in self._pod_name_list
        ]
        return "kubernetes", hosts

    def set_session_workspace(self, session_id):
        self._session_workspace = os.path.join(self._instance_workspace, session_id)
        os.makedirs(self._session_workspace, exist_ok=True)

    @property
    def preemptive(self):
        return self._saved_locals["preemptive"]

    @property
    def hosts(self):
        """String of a list of pod name, comma separated."""
        return ",".join(self._pod_name_list)

    def distribute_file(self, path):
        dir = os.path.dirname(path)
        for pod in self._pod_name_list:
            subprocess.check_call(
                [
                    "kubectl",
                    "exec",
                    pod,
                    "-c",
                    "engine",
                    "--",
                    "mkdir",
                    "-p",
                    dir,
                ]
            )
            subprocess.check_call(
                [
                    "kubectl",
                    "cp",
                    path,
                    "{}:{}".format(pod, path),
                    "-c",
                    "engine",
                ]
            )

    def create_interactive_instance(self, config: dict):
        """
        Args:
            config (dict): dict of op_def_pb2.OpDef.attr
        """
        object_id = config[types_pb2.VINEYARD_ID].i
        schema_path = config[types_pb2.SCHEMA_PATH].s.decode()
        # engine params format:
        #   k1:v1;k2:v2;k3:v3
        engine_params = {}
        if types_pb2.GIE_GREMLIN_ENGINE_PARAMS in config:
            engine_params = json.loads(
                config[types_pb2.GIE_GREMLIN_ENGINE_PARAMS].s.decode()
            )
        engine_params = [
            "{}:{}".format(key, value) for key, value in engine_params.items()
        ]
        env = os.environ.copy()
        env.update({"GRAPHSCOPE_HOME": GRAPHSCOPE_HOME})
        cmd = [
            INTERACTIVE_ENGINE_SCRIPT,
            "create_gremlin_instance_on_k8s",
            self._session_workspace,
            str(object_id),
            schema_path,
            self.hosts,
            self._engine_container_name,
            "{}".format(";".join(engine_params)),
            self._coordinator_name,
        ]
        logger.info("Create GIE instance with command: {0}".format(" ".join(cmd)))
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
        env.update({"GRAPHSCOPE_HOME": GRAPHSCOPE_HOME})
        cmd = [
            INTERACTIVE_ENGINE_SCRIPT,
            "close_gremlin_instance_on_k8s",
            self._session_workspace,
            str(object_id),
            self.hosts,
            self._engine_container_name,
        ]
        logger.info("Close GIE instance with command: {0}".format(" ".join(cmd)))
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

        labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/external": "mars",
        }

        # create mars service
        service_builder = ServiceBuilder(
            self._mars_service_name,
            service_type=self._saved_locals["service_type"],
            port=self._mars_scheduler_port,
            selector=labels,
        )
        self._resource_object.append(
            self._core_api.create_namespaced_service(
                self._saved_locals["namespace"], service_builder.build()
            )
        )

        # create engine replicaset
        scheduler_builder = self._gs_mars_scheduler_builder_cls(
            name=self._mars_scheduler_name,
            labels=labels,
            num_workers=1,
            image_pull_policy=self._saved_locals["image_pull_policy"],
        )
        # volume1 is for vineyard ipc socket
        if self._exists_vineyard_daemonset(self._saved_locals["vineyard_daemonset"]):
            vineyard_socket_volume_type = "hostPath"
            vineyard_socket_volume_fields = {
                "type": "Directory",
                "path": "/var/run/vineyard-%s-%s"
                % (
                    self._saved_locals["namespace"],
                    self._saved_locals["vineyard_daemonset"],
                ),
            }
        else:
            vineyard_socket_volume_type = "emptyDir"
            vineyard_socket_volume_fields = {}
        scheduler_builder.add_volume(
            VolumeBuilder(
                name="vineyard-ipc-volume",
                type=vineyard_socket_volume_type,
                field=vineyard_socket_volume_fields,
                mounts_list=[
                    {"mountPath": "/tmp/vineyard_workspace"},
                ],
            )
        )
        # volume2 is for shared memory
        scheduler_builder.add_volume(
            VolumeBuilder(
                name="host-shm",
                type="emptyDir",
                field={"medium": "Memory"},
                mounts_list=[{"mountPath": "/dev/shm"}],
            )
        )
        # add env
        scheduler_builder.add_simple_envs(
            {
                "GLOG_v": str(self._glog_level),
                "VINEYARD_IPC_SOCKET": "/tmp/vineyard_workspace/vineyard.sock",
                "WITH_VINEYARD": "ON",
            }
        )

        # add vineyard container
        if not self._exists_vineyard_daemonset(
            self._saved_locals["vineyard_daemonset"]
        ):
            port = self._random_etcd_listen_client_service_port
            etcd_endpoints = ["http://%s:%s" % (self._etcd_service_name, port)]
            for i in range(self._etcd_num_pods):
                etcd_endpoints.append("http://%s-%d:%s" % (self._etcd_name, i, port))
            scheduler_builder.add_vineyard_container(
                name=self._vineyard_container_name,
                image=self._saved_locals["gs_image"],
                cpu=self._saved_locals["vineyard_cpu"],
                mem=self._saved_locals["vineyard_mem"],
                shared_mem=self._saved_locals["vineyard_shared_mem"],
                preemptive=self._saved_locals["preemptive"],
                etcd_endpoints=etcd_endpoints,
                port=self._vineyard_service_port,
            )

        # add mars scheduler container
        if self._saved_locals["with_mars"]:
            scheduler_builder.add_mars_scheduler_container(
                name=self._mars_scheduler_container_name,
                image=self._saved_locals["gs_image"],
                cpu=self._saved_locals["mars_scheduler_cpu"],
                mem=self._saved_locals["mars_scheduler_mem"],
                preemptive=self._saved_locals["preemptive"],
                port=self._mars_scheduler_port,
            )
        for name in self._image_pull_secrets:
            scheduler_builder.add_image_pull_secret(name)

        self._resource_object.append(
            self._app_api.create_namespaced_replica_set(
                self._saved_locals["namespace"], scheduler_builder.build()
            )
        )

    def _create_engine_replicaset(self):
        logger.info("Launching GraphScope engines pod ...")

        labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "engine",
        }

        # create engine replicaset
        engine_builder = self._gs_engine_builder_cls(
            name=self._engine_name,
            labels=labels,
            num_workers=self._num_workers,
            image_pull_policy=self._saved_locals["image_pull_policy"],
        )
        # volume1 is for vineyard ipc socket
        # MaxGraph: /home/maxgraph/data/vineyard
        if self._exists_vineyard_daemonset(self._saved_locals["vineyard_daemonset"]):
            vineyard_socket_volume_type = "hostPath"
            vineyard_socket_volume_fields = {
                "type": "Directory",
                "path": "/var/run/vineyard-%s-%s"
                % (
                    self._saved_locals["namespace"],
                    self._saved_locals["vineyard_daemonset"],
                ),
            }
        else:
            vineyard_socket_volume_type = "emptyDir"
            vineyard_socket_volume_fields = {}
        engine_builder.add_volume(
            VolumeBuilder(
                name="vineyard-ipc-volume",
                type=vineyard_socket_volume_type,
                field=vineyard_socket_volume_fields,
                mounts_list=[
                    {"mountPath": "/tmp/vineyard_workspace"},
                ],
            )
        )
        # volume2 is for shared memory
        engine_builder.add_volume(
            VolumeBuilder(
                name="host-shm",
                type="emptyDir",
                field={"medium": "Memory"},
                mounts_list=[{"mountPath": "/dev/shm"}],
            )
        )

        # Mount aliyun demo dataset bucket
        if self._saved_locals["mount_dataset"] is not None:
            self._volumes["dataset"] = {
                "type": "emptyDir",
                "field": {},
                "mounts": {
                    "mountPath": self._saved_locals["mount_dataset"],
                    "readOnly": True,
                    "mountPropagation": "HostToContainer",
                },
            }

        # Mount user specified volumes
        for name, volume in self._volumes.items():
            volume_builder = resolve_volume_builder(name, volume)
            if volume_builder is not None:
                engine_builder.add_volume(volume_builder)

        # add env
        env = {
            "GLOG_v": str(self._glog_level),
            "VINEYARD_IPC_SOCKET": "/tmp/vineyard_workspace/vineyard.sock",
            "WITH_VINEYARD": "ON",
            "PATH": os.environ["PATH"],
            "LD_LIBRARY_PATH": os.environ["LD_LIBRARY_PATH"],
            "DYLD_LIBRARY_PATH": os.environ["DYLD_LIBRARY_PATH"],
        }
        if "OPAL_PREFIX" in os.environ:
            env.update({"OPAL_PREFIX": os.environ["OPAL_PREFIX"]})
        if "OPAL_BINDIR" in os.environ:
            env.update({"OPAL_BINDIR": os.environ["OPAL_BINDIR"]})

        engine_builder.add_simple_envs(env)

        # add engine container
        engine_builder.add_engine_container(
            cmd=["tail", "-f", "/dev/null"],
            name=self._engine_container_name,
            image=self._saved_locals["gs_image"],
            cpu=self._saved_locals["engine_cpu"],
            mem=self._saved_locals["engine_mem"],
            preemptive=self._saved_locals["preemptive"],
        )

        # add vineyard container
        if not self._exists_vineyard_daemonset(
            self._saved_locals["vineyard_daemonset"]
        ):
            port = self._random_etcd_listen_client_service_port
            etcd_endpoints = ["http://%s:%s" % (self._etcd_service_name, port)]
            for i in range(self._etcd_num_pods):
                etcd_endpoints.append("http://%s-%d:%s" % (self._etcd_name, i, port))
            engine_builder.add_vineyard_container(
                name=self._vineyard_container_name,
                image=self._saved_locals["gs_image"],
                cpu=self._saved_locals["vineyard_cpu"],
                mem=self._saved_locals["vineyard_mem"],
                shared_mem=self._saved_locals["vineyard_shared_mem"],
                preemptive=self._saved_locals["preemptive"],
                etcd_endpoints=etcd_endpoints,
                port=self._vineyard_service_port,
            )

        # add mars worker container
        if self._saved_locals["with_mars"]:
            engine_builder.add_mars_worker_container(
                name=self._mars_worker_container_name,
                image=self._saved_locals["gs_image"],
                cpu=self._saved_locals["mars_worker_cpu"],
                mem=self._saved_locals["mars_worker_mem"],
                preemptive=self._saved_locals["preemptive"],
                port=self._mars_worker_port,
                scheduler_endpoint="%s:%s"
                % (self._mars_service_name, self._mars_scheduler_port),
            )

        if self._saved_locals["mount_dataset"]:
            engine_builder.add_container(
                {
                    "name": "dataset",
                    "image": self._saved_locals["dataset_image"],
                    "imagePullPolicy": self._saved_locals["image_pull_policy"],
                    "volumeMounts": [
                        {
                            "name": "dataset",
                            "mountPath": "/dataset",
                            "mountPropagation": "Bidirectional",
                        }
                    ],
                    "securityContext": {"privileged": True},
                }
            )
        for name in self._image_pull_secrets:
            engine_builder.add_image_pull_secret(name)

        self._resource_object.append(
            self._app_api.create_namespaced_replica_set(
                self._saved_locals["namespace"], engine_builder.build()
            )
        )

    def _create_etcd(self):
        logger.info("Launching etcd ...")

        labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "etcd",
        }

        # should create service first
        service_builder = ServiceBuilder(
            self._etcd_service_name,
            service_type="ClusterIP",
            port=self._random_etcd_listen_client_service_port,
            selector=labels,
        )
        self._resource_object.append(
            self._core_api.create_namespaced_service(
                self._saved_locals["namespace"], service_builder.build()
            )
        )

        time.sleep(1)

        # create etcd cluster
        etcd_builder = self._gs_etcd_builder_cls(
            name_prefix=self._etcd_name,
            container_name=self._etcd_container_name,
            service_name=self._etcd_service_name,
            image=self._saved_locals["etcd_image"],
            cpu=self._saved_locals["etcd_cpu"],
            mem=self._saved_locals["etcd_mem"],
            preemptive=self._saved_locals["preemptive"],
            labels=labels,
            image_pull_policy=self._saved_locals["image_pull_policy"],
            num_pods=self._etcd_num_pods,
            restart_policy="Always",
            image_pull_secrets=self._image_pull_secrets,
            listen_peer_service_port=self._random_etcd_listen_peer_service_port,
            listen_client_service_port=self._random_etcd_listen_client_service_port,
        )

        pods, services = etcd_builder.build()
        for svc in services:
            self._resource_object.append(
                self._core_api.create_namespaced_service(
                    self._saved_locals["namespace"], svc.build()
                )
            )
        for pod in pods:
            self._resource_object.append(
                self._core_api.create_namespaced_pod(
                    self._saved_locals["namespace"], pod.build()
                )
            )

    def _create_vineyard_service(self):
        # vineyard in engine pod
        labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "engine",
        }

        service_builder = ServiceBuilder(
            self._vineyard_service_name,
            service_type=self._saved_locals["service_type"],
            port=self._vineyard_service_port,
            selector=labels,
        )
        self._resource_object.append(
            self._core_api.create_namespaced_service(
                self._saved_locals["namespace"], service_builder.build()
            )
        )

    def _get_vineyard_service_endpoint(self):
        # Always len(endpoints) >= 1
        endpoints = get_service_endpoints(
            api_client=self._api_client,
            namespace=self._saved_locals["namespace"],
            name=self._vineyard_service_name,
            type=self._saved_locals["service_type"],
        )
        return endpoints[0]

    def _get_mars_scheduler_service_endpoint(self):
        # Always len(endpoints) >= 1
        endpoints = get_service_endpoints(
            api_client=self._api_client,
            namespace=self._saved_locals["namespace"],
            name=self._mars_service_name,
            type=self._saved_locals["service_type"],
        )
        return endpoints[0]

    def _create_graphlearn_service(self, object_id, start_port, num_workers):
        targets = []

        labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "engine",
        }

        service_builder = ServiceBuilder(
            self._gle_service_name_prefix + str(object_id),
            service_type=self._saved_locals["service_type"],
            port=list(range(start_port, start_port + num_workers)),
            selector=labels,
            external_traffic_policy="Local",
        )
        targets.append(
            self._core_api.create_namespaced_service(
                self._saved_locals["namespace"], service_builder.build()
            )
        )
        self._graphlearn_services[object_id] = targets
        self._resource_object.extend(targets)

    def _parse_graphlearn_service_endpoint(self, object_id):
        if self._saved_locals["service_type"] == "NodePort":
            services = self._core_api.list_namespaced_service(
                self._saved_locals["namespace"]
            )
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
        elif self._saved_locals["service_type"] == "LoadBalancer":
            endpoints = get_service_endpoints(
                api_client=self._api_client,
                namespace=self._saved_locals["namespace"],
                name=self._gle_service_name_prefix + str(object_id),
                type=self._saved_locals["service_type"],
            )
            return endpoints
        raise RuntimeError("Get graphlearn service endpoint failed.")

    def get_engine_config(self):
        config = {
            "vineyard_service_name": self.get_vineyard_service_name(),
            "vineyard_rpc_endpoint": self.get_vineyard_rpc_endpoint(),
            "mars_endpoint": self.get_mars_scheduler_endpoint(),
        }
        return config

    def _create_interactive_engine_service(self):
        # launch zetcd proxy
        logger.info("Launching zetcd proxy service ...")
        zetcd_exec = shutil.which("zetcd")
        if not zetcd_exec:
            raise RuntimeError("zetcd command not found.")
        port = self._random_etcd_listen_client_service_port
        etcd_endpoints = ["http://%s:%s" % (self._etcd_service_name, port)]
        for i in range(self._etcd_num_pods):
            etcd_endpoints.append("http://%s-%d:%s" % (self._etcd_name, i, port))
        cmd = [
            zetcd_exec,
            "--zkaddr",
            "0.0.0.0:{}".format(self._zookeeper_port),
            "--endpoints",
            "{}".format(",".join(etcd_endpoints)),
        ]
        logger.info("zetcd cmd {}".format(" ".join(cmd)))

        self._zetcd_process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=os.getcwd(),
            env=os.environ.copy(),
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
        )
        stdout_watcher = PipeWatcher(self._zetcd_process.stdout, sys.stdout, drop=True)
        setattr(self._zetcd_process, "stdout_watcher", stdout_watcher)

        start_time = time.time()
        while is_free_port(
            self._zookeeper_port,
            socket.gethostbyname(socket.gethostname()),
        ):
            time.sleep(1)
            if (
                self._saved_locals["timeout_seconds"]
                and self._saved_locals["timeout_seconds"] + start_time < time.time()
            ):
                raise RuntimeError("Launch zetcd service failed.")
        logger.info(
            "ZEtcd is ready, endpoint is {0}:{1}".format(
                socket.gethostbyname(socket.gethostname()), self._zookeeper_port
            )
        )

    def _create_services(self):
        self._create_etcd()
        self._etcd_endpoint = self._get_etcd_service_endpoint()
        logger.info("Etcd is ready, endpoint is {}".format(self._etcd_endpoint))

        # create interactive engine service
        logger.info("Creating interactive engine service...")
        self._create_interactive_engine_service()

        if self._saved_locals["with_mars"]:
            # scheduler used by mars
            self._create_mars_scheduler()

        logger.info("Creating engine replicaset...")
        self._create_engine_replicaset()
        if not self._exists_vineyard_daemonset(
            self._saved_locals["vineyard_daemonset"]
        ):
            self._create_vineyard_service()

    def _waiting_for_services_ready(self):
        start_time = time.time()
        event_messages = []
        engine_pod_selector = ""
        while True:
            replicasets = self._app_api.list_namespaced_replica_set(
                namespace=self._saved_locals["namespace"]
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
                    engine_pod_selector = selector

                    pods = self._core_api.list_namespaced_pod(
                        namespace=self._saved_locals["namespace"],
                        label_selector=selector,
                    )

                    for pod in pods.items:
                        pod_name = pod.metadata.name
                        field_selector = "involvedObject.name=" + pod_name
                        stream = kube_watch.Watch().stream(
                            self._core_api.list_namespaced_event,
                            self._saved_locals["namespace"],
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
                self._saved_locals["timeout_seconds"]
                and self._saved_locals["timeout_seconds"] + start_time < time.time()
            ):
                raise TimeoutError("GraphScope Engines launching timeout.")
            time.sleep(2)

        self._pod_name_list = []
        self._pod_ip_list = []
        self._pod_host_ip_list = []
        pods = self._core_api.list_namespaced_pod(
            namespace=self._saved_locals["namespace"],
            label_selector=engine_pod_selector,
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
        logger.debug("vineyard rpc runs on %s", self._vineyard_service_endpoint)
        if self._saved_locals["with_mars"]:
            self._mars_service_endpoint = self._get_mars_scheduler_service_endpoint()
            logger.debug("mars scheduler runs on %s", self._mars_service_endpoint)
        logger.info("GraphScope engines pod is ready.")

    def _dump_resource_object(self):
        resource = {}
        if self._saved_locals["delete_namespace"]:
            resource[self._saved_locals["namespace"]] = "Namespace"
        else:
            # coordinator info
            resource[self._coordinator_name] = "Deployment"
            resource[self._coordinator_service_name] = "Service"

        self._resource_object.dump_with_extra_resource(resource)

    def _get_etcd_service_endpoint(self):
        # Always len(endpoints) >= 1
        endpoints = get_service_endpoints(
            api_client=self._api_client,
            namespace=self._saved_locals["namespace"],
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
                    self._saved_locals["namespace"],
                    "cp",
                    "/tmp/kube_hosts",
                    "{}:/tmp/hosts_of_nodes".format(pod),
                    "-c",
                    self._engine_container_name,
                ]
            )

        # launch engine
        rmcp = ResolveMPICmdPrefix(rsh_agent=True)
        cmd, mpi_env = rmcp.resolve(self._num_workers, ",".join(self._pod_name_list))

        cmd.append(ANALYTICAL_ENGINE_PATH)
        cmd.extend(["--host", "0.0.0.0"])
        cmd.extend(["--port", str(self._random_analytical_engine_rpc_port)])
        cmd.extend(["--vineyard_shared_mem", self._saved_locals["vineyard_shared_mem"]])

        if rmcp.openmpi():
            cmd.extend(["-v", str(self._glog_level)])
        else:
            mpi_env["GLOG_v"] = str(self._glog_level)

        cmd.extend(["--vineyard_socket", "/tmp/vineyard_workspace/vineyard.sock"])
        logger.info("Analytical engine launching command: {}".format(" ".join(cmd)))

        env = os.environ.copy()
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
        self._core_api.delete_namespaced_service(
            name=self._coordinator_service_name,
            namespace=self._saved_locals["namespace"],
        )
        self._app_api.delete_namespaced_deployment(
            name=self._coordinator_name, namespace=self._saved_locals["namespace"]
        )
        if self._saved_locals["waiting_for_delete"]:
            start_time = time.time()
            while True:
                try:
                    self._app_api.read_namespaced_deployment(
                        name=self._coordinator_name,
                        namespace=self._saved_locals["namespace"],
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
                    if time.time() - start_time > self._saved_locals["timeout_seconds"]:
                        logger.error(
                            "Deleting dangling coordinator {} timeout".format(
                                self._coordinator_name
                            )
                        )

    def _exists_vineyard_daemonset(self, release):
        # check if vineyard daemonset exists.
        if not release:
            return False
        try:
            self._app_api.read_namespaced_daemon_set(
                release, self._saved_locals["namespace"]
            )
        except K8SApiException:
            return False
        else:
            return True

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
            if self._saved_locals["with_mars"]:
                logger.info(
                    "Mars service endpoint: {}".format(self._mars_service_endpoint)
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
                    wait=self._saved_locals["waiting_for_delete"],
                    timeout_seconds=self._saved_locals["timeout_seconds"],
                )
            self._resource_object = []

            if is_dangling:
                logger.info("Dangling coordinator detected, cleaning up...")
                # delete everything inside namespace of graphscope instance
                if self._saved_locals["delete_namespace"]:
                    # delete namespace created by graphscope
                    self._core_api.delete_namespace(self._saved_locals["namespace"])
                    if self._saved_locals["waiting_for_delete"]:
                        start_time = time.time()
                        while True:
                            try:
                                self._core_api.read_namespace(
                                    self._saved_locals["namespace"]
                                )
                            except K8SApiException as ex:
                                if ex.status != 404:
                                    logger.error(
                                        "Deleting dangling namespace {} failed: {}".format(
                                            self._saved_locals["namespace"], str(ex)
                                        )
                                    )
                                break
                            else:
                                time.sleep(1)
                                if (
                                    time.time() - start_time
                                    > self._saved_locals["timeout_seconds"]
                                ):
                                    logger.error(
                                        "Deleting namespace %s timeout"
                                        % self._saved_locals["namespace"]
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
                self._saved_locals["namespace"],
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
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                encoding="utf-8",
                errors="replace",
                universal_newlines=True,
                bufsize=1,
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
                    wait=self._saved_locals["waiting_for_delete"],
                    timeout_seconds=self._saved_locals["timeout_seconds"],
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
