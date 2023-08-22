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

from dataclasses import dataclass, field
from typing import Union

from simple_parsing import ArgumentParser, choice, subgroups
from simple_parsing.helpers import choice, list_field
from simple_parsing.helpers import Serializable
from simple_parsing import ConflictResolution

# from graphscope.version import __is_prerelease__
# from graphscope.version import __version__

__is_prerelease__ = False
__version__ = "0.3.0"

registry = "registry.cn-hongkong.aliyuncs.com"


class GSConfig(object):
    # the coordinator endpoint of a pre-launched GraphScope instance.
    addr = None

    # "eager" or "lazy", defaults to "eager"
    mode = "eager"

    # "k8s" or "hosts"
    cluster_type = "k8s"

    k8s_namespace = None

    # k8s image information
    # GraphScope's component has a fixed name, use registry, repository and tag to
    # uniquely identify the image. For example, the coordinator image would be
    # ${registry}/${repository}/coordinator:${tag}
    # The image names of all major components are:
    #   - coordinator: The coordinator of GraphScope instance.
    #   - analytical: The analytical engine of GraphScope instance.
    #   - interactive: The interactive engine of GraphScope instance.
    #   - learning: The learning engine of GraphScope instance.
    # These are utility components for ease of use.
    #   - dataset: A dataset container with example datasets
    #   - jupyter: A jupyter notebook container with GraphScope client installed.
    k8s_image_registry = "registry.cn-hongkong.aliyuncs.com"
    k8s_image_repository = "graphscope"
    k8s_image_tag = __version__

    # image pull configuration
    k8s_image_pull_policy = "IfNotPresent"
    k8s_image_pull_secrets = []

    # coordinator resource configuration
    k8s_coordinator_cpu = 0.5
    k8s_coordinator_mem = "512Mi"

    # etcd resource configuration
    etcd_addrs = None
    etcd_listening_client_port = 2379
    etcd_listening_peer_port = 2380

    # vineyard resource configuration
    # image for vineyard container
    k8s_vineyard_image = "vineyardcloudnative/vineyardd:latest"
    k8s_vineyard_deployment = None
    k8s_vineyard_cpu = 0.5
    k8s_vineyard_mem = "512Mi"

    vineyard_shared_mem = ""  # dummy

    # engine resource configuration
    k8s_engine_cpu = 0.2
    k8s_engine_mem = "1Gi"

    # mars resource configuration
    mars_worker_cpu = 0.2
    mars_worker_mem = "4Mi"
    mars_scheduler_cpu = 0.2
    mars_scheduler_mem = "2Mi"

    # the node selector can be a dict, see also: https://tinyurl.com/3nx6k7ph
    k8s_coordinator_pod_node_selector = None
    k8s_engine_pod_node_selector = None

    # Enabled engines, default to all 3 engines
    # Available options: analytical, analytical-java, interactive, learning
    enabled_engines = "analytical,interactive,learning"

    # launch graphscope with Mars
    with_mars = False
    # Demo dataset related
    with_dataset = False

    k8s_volumes = {}

    k8s_service_type = "NodePort"

    # support resource preemption or resource guarantee
    preemptive = True

    # the deployment mode of engines on the kubernetes cluster, default to eager.
    # eager: create all engine pods at once
    # lazy: create engine pods when called
    k8s_deploy_mode = "eager"

    k8s_waiting_for_delete = False
    num_workers = 2
    show_log = False
    log_level = "INFO"

    timeout_seconds = 600

    # kill GraphScope instance after seconds of client disconnect
    # disable dangling check by setting -1.
    dangling_timeout_seconds = 600

    # download_retries
    dataset_download_retries = 3

@dataclass
class ResourceSpec():
    """Resource requirements for a container in kubernetes."""
    cpu: Union[str, float] = None # CPU cores of container.
    memory: str = None  # Memory of container, suffix with ['Mi', 'Gi', 'Ti'].

    def as_dict(self):
        ret = {}
        if self.cpu is not None:
            ret['cpu'] = self.cpu
        if self.memory is not None:
            ret['memory'] = self.memory
        return ret

@dataclass
class ResourceConfig():
    """Resource spec for a container in kubernetes."""
    requests: ResourceSpec = None  # Resource requests of container.
    limits: ResourceSpec = None  # Resource limits of container.

    def get_requests(self):
        return self.requests.as_dict() if self.requests is not None else None
    def get_limits(self):
        return self.limits.as_dict() if self.limits is not None else None

    def set_cpu_guarantee(self, cpu):
        self.requests.cpu = cpu
        self.limits.cpu = cpu

    def set_mem_guarantee(self, memory):
        self.requests.memory = memory
        self.limits.memory = memory

def _get_guarantee_resource_config(cpu, memory):
    """Get default resource config for a container in kubernetes."""
    return ResourceConfig(
        requests=ResourceSpec(cpu=cpu, memory=memory),
        limits=ResourceSpec(cpu=cpu, memory=memory),
    )

@dataclass
class ContainerConfig():
    """Container configuration."""
    resource: ResourceConfig = ResourceConfig()  # Resource configuration of container.

@dataclass
class EtcdConfig():
    """Etcd configuration."""
    address: str = None
    """The address of external etcd cluster, with formats like 'etcd01:port,etcd02:port,etcd03:port'.
    If address is set, all other etcd configurations are ignored.
    """
    # replicas: int = 1

    listening_client_port: int = 2379  # The port that etcd server will bind to for accepting client connections. Defaults to 2379.
    listening_peer_port: int = 2380  # The port that etcd server will bind to for accepting peer connections. Defaults to 2380.


@dataclass
class ImageConfig():
    """Image related stuffs."""
    registry: str = "registry.cn-hongkong.aliyuncs.com"  # k8s image registry.
    repository: str = "graphscope"  # k8s image repository.
    tag: str = __version__  # k8s image tag.
    pull_secrets: list[str] = field(default_factory=list)  # A list of secrets to pull image.
    pull_policy: str = "IfNotPresent"  # Kubernetes image pull policy.
    
    vineyard_image: str = "vineyardcloudnative/vineyardd:latest"  # Image for vineyard container.


@dataclass
class MarsConfig():
    """Mars configuration"""
    enable: bool = False  # Enable Mars or not.
    worker: ContainerConfig = ContainerConfig(resource=_get_guarantee_resource_config(0.2, "4Mi"))
    scheduler: ContainerConfig = ContainerConfig(resource=_get_guarantee_resource_config(0.2, "2Mi"))

@dataclass
class DatasetConfig():
    """A Dataset container could be shipped with GraphScope in kubernetes."""
    enable: bool = False  # Mount the aliyun dataset bucket as a volume by ossfs.
    proxy: str = None  # A json string specifies the dataset proxy info. Available options of proxy: http_proxy, https_proxy, no_proxy.


@dataclass
class EngineConfig():
    """Engine configuration"""
    enabled_engines: str = "gae,gie,gle"  # A set of engines to enable.
    node_selector: str = None  # Node selector for engine pods, default is None.

    enable_gae: bool = True  # Enable or disable analytical engine.
    enable_gae_java: bool = False # Enable or disable analytical engine with java support.
    enable_gie: bool = True  # Enable or disable interactive engine.
    enable_gle: bool = True  # Enable or disable learning engine.

    # gae: ContainerConfig = ContainerConfig(resource=_get_resource_config(0.2, "1Gi"))
    gae: ContainerConfig = ContainerConfig()
    # Resource for analytical pod

    executor: ContainerConfig = ContainerConfig(resource=_get_guarantee_resource_config(1, "1Gi"))
    # Resource for interactive executor pod

    frontend: ContainerConfig = ContainerConfig(resource=_get_guarantee_resource_config(0.2, "512Mi"))
    # Resource for interactive frontend pod

    gle: ContainerConfig = ContainerConfig(resource=_get_guarantee_resource_config(0.2, "1Gi"))
    # Resource for learning pod

    vineyard: ContainerConfig = ContainerConfig(resource=_get_guarantee_resource_config(0.2, "256Mi"))
    # Resource for vineyard sidecar container

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

@dataclass
class VineyardConfig():
    """Vineyard configuration"""
    socket: str = None  # Vineyard IPC socket path, a socket suffixed by timestamp will be created in '/tmp' if not given.
    rpc_port: int = 9600  # Vineyard RPC port.

    deployment_name: str = None  # The name of vineyard deployment, it should exist as expected.

@dataclass
class CoordinatorDeploymentConfig():
    deployment_name: str = None  # Name of the coordinator deployment and service.
    node_selector: str = None  # Node selector for coordinator pod in kubernetes
    coordinator: ContainerConfig = ContainerConfig(resource=_get_guarantee_resource_config(0.5, "512Mi"))  # Resource configuration of coordinator.

@dataclass
class CoordinatorConfig():
    address: str = None
    """The address of existed coordinator service, with formats like 'ip:port'.
    If address is set, all other coordinator configurations are ignored.
    """
    service_port: int = 63800  # Coordinator service port that will be listening on.

    monitor: bool = False  # Enable or disable prometheus exporter.
    monitor_port: int = 9090  # Coordinator prometheus exporter service port.

    coordinator: CoordinatorDeploymentConfig = CoordinatorDeploymentConfig()  # Coordinator deployment configuration.



@dataclass
class HostsConfig():
    """Local cluster configuration."""
    hosts: list[str] = list_field("localhost")  # list of comma separated hostname of graphscope engine workers.
    etcd: EtcdConfig = EtcdConfig()  # Etcd configuration. Only local session needs to configure etcd.

    dataset_download_retries: int = 3  # The number of retries when downloading dataset from internet.

@dataclass
class KubernetesConfig():
    """Kubernetes cluster configuration."""
    namespace: str = "graphscope"  # The namespace to create all resource, which must exist in advance.
    delete_namespace: bool = True  # Delete the namespace that created by graphscope.

    config_file: str = None  # kube config file path
    
    deployment_mode = "eager" # The deployment mode of engines on the kubernetes cluster, choose from 'eager' or 'lazy'.

    service_type: str = "NodePort"  # Service type, choose from 'NodePort' or 'LoadBalancer'.

    volumes: str = None  # A base64 encoded json string specifies the kubernetes volumes to mount.

    preemptive: bool = True  # Support resource preemption or resource guarantee.

    waiting_for_delete: bool = False  # Wait until the graphscope instance has been deleted successfully.

    image: ImageConfig = ImageConfig()  # Image configuration.



    engine: EngineConfig = EngineConfig()  # Engine configuration.

    dataset: DatasetConfig = DatasetConfig()  # Dataset configuration.

    mars: MarsConfig = MarsConfig()  # Mars configuration.


@dataclass
class SessionConfig():
    """Session configuration"""
    num_workers: int = 2  # The number of graphscope engine workers.

    instance_id: str = None  # Unique id for each GraphScope instance.
    
    show_log: bool = False  # Show log or not.
    log_level: str = "info"  # Log level, choose from 'info' or 'debug'.
          
    timeout_seconds: int = 600  # The length of time to wait before giving up launching graphscope.
    dangling_timeout_seconds: int = 600  # The length of time to wait starting from client disconnected before killing the graphscope instance.
    
    retry_time_seconds: int = 1  # The length of time to wait before retrying to launch graphscope.

    execution_mode: str = "eager"  # The deploying mode of graphscope, eager or lazy.


@dataclass
class Config(Serializable):
    session: SessionConfig = SessionConfig()

    coordinator: CoordinatorConfig = CoordinatorConfig()  # Coordinator configuration.
    vineyard: VineyardConfig = VineyardConfig()  # Vineyard configuration.

    launcher_type: str = "hosts"  # Launcher type, choose from 'k8s' or 'hosts'.
    hosts_launcher: HostsConfig = HostsConfig()  # Local cluster configuration.
    kubernetes_launcher: KubernetesConfig = KubernetesConfig()  # Kubernetes cluster configuration.

    def post_setup(self):
        self.kubernetes_launcher.engine.post_setup()

    def set_option(self, key, value):
        if key == "addr":
            self.coordinator.address = value
        if key == "mode":
            self.session.execution_mode = value
        if key == "cluster_type":
            self.launcher_type = value
        if key == "k8s_namespace":
            self.kubernetes_launcher.namespace = value
        if key == "k8s_image_registry":
            self.kubernetes_launcher.image.registry = value
        if key == "k8s_image_repository":
            self.kubernetes_launcher.image.repository = value
        if key == "k8s_image_tag":
            self.kubernetes_launcher.image.tag = value
        if key == "k8s_image_pull_policy":
            self.kubernetes_launcher.image.pull_policy = value
        if key == "k8s_image_secrets":
            self.kubernetes_launcher.image.pull_secrets = value
        if key == "k8s_coordinator_cpu":
            self.kubernetes_launcher.coordinator.coordinator.resource.set_cpu_guarantee(value)
        if key == "k8s_coordinator_mem":
            self.kubernetes_launcher.coordinator.coordinator.resource.set_mem_guarantee(value)
        if key == "etcd_addrs":
            self.hosts_launcher.etcd.address = value
        if key == "etcd_listening_client_port":
            self.hosts_launcher.etcd.listening_client_port = value
        if key == "etcd_listening_peer_port":
            self.hosts_launcher.etcd.listening_peer_port = value
        if key == "k8s_vineyard_image":
            self.kubernetes_launcher.image.vineyard_image = value
        if key == "k8s_vineyard_deployment":
            self.vineyard.deployment_name = value
        if key == "k8s_vineyard_cpu":
            self.kubernetes_launcher.engine.vineyard.resource.set_cpu_guarantee(value)
        if key == "k8s_vineyard_mem":
            self.kubernetes_launcher.engine.vineyard.resource.set_mem_guarantee(value)
        if key == "k8s_engine_cpu":
            self.kubernetes_launcher.engine.gae.resource.set_cpu_guarantee(value)
        if key == "k8s_engine_mem":
            self.kubernetes_launcher.engine.gae.resource.set_mem_guarantee(value)
        if key == "mars_worker_cpu":
            self.kubernetes_launcher.mars.worker.resource.set_cpu_guarantee(value)
        if key == "mars_worker_mem":
            self.kubernetes_launcher.mars.worker.resource.set_mem_guarantee(value)
        if key == "mars_scheduler_cpu":
            self.kubernetes_launcher.mars.scheduler.resource.set_cpu_guarantee(value)
        if key == "mars_scheduler_mem":
            self.kubernetes_launcher.mars.scheduler.resource.set_mem_guarantee(value)
        if key == "k8s_coordinator_pod_node_selector":
            self.kubernetes_launcher.coordinator.node_selector = value
        if key == "k8s_engine_pod_node_selector":
            self.kubernetes_launcher.engine.node_selector = value
        if key == "enabled_engines":
            self.kubernetes_launcher.engine.enabled_engines = value
        if key == "with_mars":
            self.kubernetes_launcher.mars.enable = value
        if key == "with_dataset":
            self.kubernetes_launcher.dataset.enable = value
        if key == "k8s_volumes":
            self.kubernetes_launcher.volumes = value
        if key == "k8s_service_type":
            self.kubernetes_launcher.service_type = value
        if key == "preemptive":
            raise NotImplementedError()
        if key == "k8s_deploy_mode":
            self.kubernetes_launcher.deployment_mode = value
        if key == "k8s_waiting_for_delete":
            self.kubernetes_launcher.waiting_for_delete = value
        if key == "num_workers":
            self.session.num_workers = value
        if key == "show_log":
            self.session.show_log = value
        if key == "log_level":
            self.session.log_level = value
        if key == "timeout_seconds":
            self.session.timeout_seconds = value
        if key == "dangling_timeout_seconds":
            self.session.dangling_timeout_seconds = value
        if key == "dataset_download_retries":
            self.hosts_launcher.dataset_download_retries = value

if __name__ == '__main__':
    config = Config()
    print(config.dumps_yaml())
    print(config.kubernetes_launcher.engine.gae.resource.as_dict())
    # config2 = Config()
    # config2.loads_yaml(config.dumps_yaml())
    # print(config2)

    parser = ArgumentParser()
    parser.add_arguments(Config, dest="gs")
    args = parser.parse_args()
