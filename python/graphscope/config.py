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

""" GraphScope default configuration.
"""

import base64
import json
from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Union

from simple_parsing import ArgumentParser
from simple_parsing.helpers import Serializable
from simple_parsing.helpers import list_field

from graphscope.version import __version__

registry = "registry.cn-hongkong.aliyuncs.com"


@dataclass
class ResourceSpec:
    """Resource requirements for a container in kubernetes."""

    cpu: Union[str, float, None] = None  # CPU cores of container.
    # Memory of container, suffix with ['Mi', 'Gi', 'Ti'].
    memory: Union[str, None] = None

    def as_dict(self):
        ret = {}
        if self.cpu is not None:
            ret["cpu"] = self.cpu
        if self.memory is not None:
            ret["memory"] = self.memory
        return ret


@dataclass
class ResourceConfig:
    """Resource spec for a container in kubernetes."""

    requests: ResourceSpec = None  # Resource requests of container.
    limits: ResourceSpec = None  # Resource limits of container.

    def get_requests(self):
        return self.requests.as_dict() if self.requests is not None else None

    def get_limits(self):
        return self.limits.as_dict() if self.limits is not None else None

    def set_cpu_request(self, cpu):
        self.requests.cpu = cpu
        # self.limits.cpu = cpu

    def set_mem_request(self, memory):
        self.requests.memory = memory
        # self.limits.memory = memory

    @staticmethod
    def make_burstable(cpu, memory):
        """Get default resource config for a container in kubernetes."""
        return ResourceConfig(
            requests=ResourceSpec(cpu=cpu, memory=memory),
            # limits=ResourceSpec(cpu=cpu, memory=memory),
        )


@dataclass
class ImageConfig:
    """Image related stuffs."""

    # k8s image registry.
    registry: Union[str, None] = "registry.cn-hongkong.aliyuncs.com"
    repository: str = "graphscope"  # k8s image repository.
    tag: str = __version__  # k8s image tag.
    # A list of secrets to pull image.
    pull_secrets: List[str] = field(default_factory=list)
    pull_policy: str = "IfNotPresent"  # Kubernetes image pull policy.


@dataclass
class MarsConfig:
    """Mars configuration"""

    enable: bool = False  # Enable Mars or not.
    worker_resource: ResourceConfig = field(
        default_factory=lambda: ResourceConfig.make_burstable(0.2, "4Mi")
    )
    scheduler_resource: ResourceConfig = field(
        default_factory=lambda: ResourceConfig.make_burstable(0.2, "4Mi")
    )


@dataclass
class DatasetConfig:
    """A Dataset container could be shipped with GraphScope in kubernetes."""

    enable: bool = False  # Mount the aliyun dataset bucket as a volume by ossfs.
    # A json string specifies the dataset proxy info. Available options of proxy: http_proxy, https_proxy, no_proxy.
    proxy: Union[str, None] = None


@dataclass
class EngineConfig:
    """Engine configuration"""

    enabled_engines: str = "gae,gie,gle"  # A set of engines to enable.
    # Node selector for engine pods, default is None.
    node_selector: Union[str, None] = None

    enable_gae: bool = True  # Enable or disable analytical engine.
    # Enable or disable analytical engine with java support.
    enable_gae_java: bool = False
    enable_gie: bool = True  # Enable or disable interactive engine.
    enable_gle: bool = True  # Enable or disable learning engine.

    preemptive: bool = True

    # Resource for analytical pod
    gae_resource: ResourceConfig = field(
        default_factory=lambda: ResourceConfig.make_burstable(1, "4Gi")
    )

    # Resource for interactive executor pod
    gie_executor_resource: ResourceConfig = field(
        default_factory=lambda: ResourceConfig.make_burstable(1, "2Gi")
    )

    # Resource for interactive frontend pod
    gie_frontend_resource: ResourceConfig = field(
        default_factory=lambda: ResourceConfig.make_burstable(0.5, "1Gi")
    )

    # Resource for learning pod
    gle_resource: ResourceConfig = field(
        default_factory=lambda: ResourceConfig.make_burstable(0.2, "1Gi")
    )

    def post_setup(self):
        valid_engines = set(
            "analytical,analytical-java,interactive,learning,gae,gae-java,gie,gle".split(
                ","
            )
        )
        for item in [item.strip() for item in self.enabled_engines.split(",")]:
            if item not in valid_engines and item != "":
                print(f"Not a valid engine name: {item}")
            if item == "analytical" or item == "gae":
                self.enable_gae = True
            if item == "interactive" or item == "gie":
                self.enable_gie = True
            if item == "learning" or item == "gle":
                self.enable_gle = True
            if item == "analytical-java" or item == "gae-java":
                self.enable_gae_java = True

        if self.preemptive:
            self.gae_resource.requests = None
            self.gle_resource.requests = None
            self.gie_executor_resource.requests = None
            self.gie_frontend_resource.requests = None


@dataclass
class EtcdConfig:
    """Etcd configuration."""

    endpoint: Union[str, None] = None
    """The address of external etcd cluster, with formats like 'etcd01:port,etcd02:port,etcd03:port'.
    If address is set, all other etcd configurations are ignored.
    """

    # The port that etcd server will bind to for accepting client connections. Defaults to 2379.
    listening_client_port: int = 2379

    # The port that etcd server will bind to for accepting peer connections. Defaults to 2380.
    listening_peer_port: int = 2380

    # Kubernetes related config
    replicas: int = 1


@dataclass
class VineyardConfig:
    """Vineyard configuration"""

    # Vineyard IPC socket path, a socket suffixed by timestamp will be created in '/tmp' if not given.
    socket: Union[str, None] = None
    rpc_port: int = 9600  # Vineyard RPC port.

    # Kubernetes related config

    # The name of vineyard deployment, it should exist as expected.
    deployment_name: Union[str, None] = None

    image: str = "vineyardcloudnative/vineyardd:latest"  # Image for vineyard container.

    # Resource for vineyard sidecar container
    resource: ResourceConfig = field(
        default_factory=lambda: ResourceConfig.make_burstable(0.2, "256Mi")
    )


@dataclass
class CoordinatorConfig:
    endpoint: Union[str, None] = None
    """The address of existed coordinator service, with formats like 'ip:port'.
    If address is set, all other coordinator configurations are ignored.
    """
    service_port: int = 63800  # Coordinator service port that will be listening on.

    monitor: bool = False  # Enable or disable prometheus exporter.
    monitor_port: int = 9090  # Coordinator prometheus exporter service port.

    # Kubernetes related config

    # Name of the coordinator deployment and service.
    deployment_name: Union[str, None] = None
    # Node selector for coordinator pod in kubernetes
    node_selector: Union[str, None] = None
    # Resource configuration of coordinator.
    resource: ResourceConfig = field(
        default_factory=lambda: ResourceConfig.make_burstable(0.5, "512Mi")
    )

    # For GraphScope operator
    # Launch coordinator only, do not let coordinator launch resources or delete resources.
    # It would try to find existing resources and connect to it.
    operator_mode: bool = False


@dataclass
class HostsLauncherConfig:
    """Local cluster configuration."""

    # list of hostnames of graphscope engine workers.
    hosts: List[str] = list_field("localhost")
    # Etcd configuration. Only local session needs to configure etcd.
    etcd: EtcdConfig = field(default_factory=EtcdConfig)

    # The number of retries when downloading dataset from internet.
    dataset_download_retries: int = 3


@dataclass
class KubernetesLauncherConfig:
    """Kubernetes cluster configuration."""

    # The namespace to create all resource, which must exist in advance.
    namespace: Union[str, None] = None
    delete_namespace: bool = False  # Delete the namespace that created by graphscope.

    config_file: Union[str, None] = None  # kube config file path

    # The deployment mode of engines on the kubernetes cluster, choose from 'eager' or 'lazy'.
    deployment_mode: str = "eager"

    # Service type, choose from 'NodePort' or 'LoadBalancer'.
    service_type: str = "NodePort"

    # A base64 encoded json string specifies the kubernetes volumes to mount.
    volumes: Union[str, None] = None

    # Wait until the graphscope instance has been deleted successfully.
    waiting_for_delete: bool = False

    image: ImageConfig = field(default_factory=ImageConfig)  # Image configuration.

    engine: EngineConfig = field(default_factory=EngineConfig)  # Engine configuration.

    # Dataset configuration.
    dataset: DatasetConfig = field(default_factory=DatasetConfig)

    mars: MarsConfig = field(default_factory=MarsConfig)  # Mars configuration.


@dataclass
class OperatorLauncherConfig:
    namespace: str = "default"
    gae_endpoint: str = ""
    hosts: List[str] = list_field()


@dataclass
class SessionConfig:
    """Session configuration"""

    num_workers: int = 2  # The number of graphscope engine workers.
    # The number of graphscope engine workers when launch local workers.
    default_local_num_workers: int = 1

    reconnect: bool = False  # Connect to an existed GraphScope Cluster
    instance_id: Union[str, None] = None  # Unique id for each GraphScope instance.

    show_log: bool = False  # Show log or not.
    log_level: str = "info"  # Log level, choose from 'info' or 'debug'.

    # The length of time to wait before giving up launching graphscope.z
    timeout_seconds: int = 600
    # The length of time to wait starting from client disconnected before killing the graphscope instance.
    dangling_timeout_seconds: int = 600

    # The length of time to wait before retrying to launch graphscope.
    retry_time_seconds: int = 1

    execution_mode: str = "eager"  # The deploying mode of graphscope, eager or lazy.


@dataclass
class Config(Serializable):
    # Launcher type, choose from 'hosts', 'k8s' or 'operator'.
    launcher_type: str = "k8s"

    session: SessionConfig = field(default_factory=SessionConfig)

    # Coordinator configuration.
    coordinator: CoordinatorConfig = field(default_factory=CoordinatorConfig)
    # Vineyard configuration.
    vineyard: VineyardConfig = field(default_factory=VineyardConfig)

    # Local cluster configuration.
    hosts_launcher: HostsLauncherConfig = field(default_factory=HostsLauncherConfig)

    # Kubernetes cluster configuration.
    kubernetes_launcher: KubernetesLauncherConfig = field(
        default_factory=KubernetesLauncherConfig
    )

    # Launcher used in operator mode.
    operator_launcher: OperatorLauncherConfig = field(
        default_factory=OperatorLauncherConfig
    )

    def set_option(self, key, value):  # noqa: C901
        """Forward set_option target to actual config fields"""
        if key == "addr":
            self.coordinator.endpoint = value
        elif key == "mode":
            self.session.execution_mode = value
        elif key == "cluster_type":
            self.launcher_type = value
        elif key == "k8s_namespace":
            self.kubernetes_launcher.namespace = value
        elif key == "k8s_image_registry":
            self.kubernetes_launcher.image.registry = value
        elif key == "k8s_image_repository":
            self.kubernetes_launcher.image.repository = value
        elif key == "k8s_image_tag":
            self.kubernetes_launcher.image.tag = value
        elif key == "k8s_image_pull_policy":
            self.kubernetes_launcher.image.pull_policy = value
        elif key == "k8s_image_secrets":
            self.kubernetes_launcher.image.pull_secrets = value
        elif key == "k8s_coordinator_cpu":
            self.coordinator.resource.set_cpu_request(value)
        elif key == "k8s_coordinator_mem":
            self.coordinator.resource.set_mem_request(value)
        elif key == "etcd_addrs":
            self.hosts_launcher.etcd.endpoint = value
        elif key == "etcd_listening_client_port":
            self.hosts_launcher.etcd.listening_client_port = value
        elif key == "etcd_listening_peer_port":
            self.hosts_launcher.etcd.listening_peer_port = value
        elif key == "k8s_vineyard_image":
            self.kubernetes_launcher.image.vineyard_image = value
        elif key == "k8s_vineyard_deployment":
            self.vineyard.deployment_name = value
        elif key == "k8s_vineyard_cpu":
            self.vineyard.resource.set_cpu_request(value)
        elif key == "k8s_vineyard_mem":
            self.vineyard.resource.set_mem_request(value)
        elif key == "k8s_engine_cpu":
            self.kubernetes_launcher.engine.gae_resource.set_cpu_request(value)
        elif key == "k8s_engine_mem":
            self.kubernetes_launcher.engine.gae_resource.set_mem_request(value)
        elif key == "mars_worker_cpu":
            self.kubernetes_launcher.mars.worker_resource.set_cpu_request(value)
        elif key == "mars_worker_mem":
            self.kubernetes_launcher.mars.worker_resource.set_mem_request(value)
        elif key == "mars_scheduler_cpu":
            self.kubernetes_launcher.mars.scheduler_resource.set_cpu_request(value)
        elif key == "mars_scheduler_mem":
            self.kubernetes_launcher.mars.scheduler_resource.set_mem_request(value)
        elif key == "k8s_coordinator_pod_node_selector":
            self.coordinator.node_selector = base64_encode(json.dumps(value))
        elif key == "k8s_engine_pod_node_selector":
            self.kubernetes_launcher.engine.node_selector = base64_encode(
                json.dumps(value)
            )
        elif key == "enabled_engines":
            self.kubernetes_launcher.engine.enabled_engines = value
        elif key == "with_mars":
            self.kubernetes_launcher.mars.enable = value
        elif key == "with_dataset":
            self.kubernetes_launcher.dataset.enable = value
        elif key == "k8s_volumes":
            self.kubernetes_launcher.volumes = base64_encode(json.dumps(value))
        elif key == "k8s_service_type":
            self.kubernetes_launcher.service_type = value
        elif key == "preemptive":
            self.kubernetes_launcher.engine.preemptive = value
        elif key == "k8s_deploy_mode":
            self.kubernetes_launcher.deployment_mode = value
        elif key == "k8s_waiting_for_delete":
            self.kubernetes_launcher.waiting_for_delete = value
        elif key == "num_workers":
            self.session.num_workers = value
            self.session.default_local_num_workers = value
        elif key == "show_log":
            self.session.show_log = value
        elif key == "log_level":
            self.session.log_level = value
        elif key == "timeout_seconds":
            self.session.timeout_seconds = value
        elif key == "dangling_timeout_seconds":
            self.session.dangling_timeout_seconds = value
        elif key == "dataset_download_retries":
            self.hosts_launcher.dataset_download_retries = value
        elif key == "k8s_client_config":
            self.kubernetes_launcher.config_file = value
        elif key == "reconnect":
            self.session.reconnect = value
        elif key == "vineyard_shared_mem":
            pass
        else:
            raise ValueError("Key not recognized: " + key)


def base64_encode(string):
    return base64.b64encode(string.encode("utf-8")).decode("utf-8", errors="ignore")


gs_config = Config()


if __name__ == "__main__":
    config = Config()
    config.coordinator.resource.requests = None
    config.kubernetes_launcher.image.registry = ""
    print(config.dumps_yaml())
    # print(config.dumps_json())
    s = config.dumps_yaml()
    config3 = Config.loads_yaml(s)
    print(config3)

    parser = ArgumentParser()
    parser.add_arguments(Config, dest="gs")
    args = parser.parse_args()
