#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020-2023 Alibaba Group Holding Limited.
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

try:
    from kubernetes import client as kube_client
except ImportError:
    kube_client = None

from graphscope.config import Config
from graphscope.config import KubernetesLauncherConfig
from graphscope.deploy.kubernetes.resource_builder import ResourceBuilder
from graphscope.deploy.kubernetes.utils import get_service_endpoints

from gscoordinator.constants import ANALYTICAL_CONTAINER_NAME
from gscoordinator.constants import DATASET_CONTAINER_NAME
from gscoordinator.constants import GRAPHLEARN_CONTAINER_NAME
from gscoordinator.constants import GRAPHLEARN_TORCH_CONTAINER_NAME
from gscoordinator.constants import INTERACTIVE_EXECUTOR_CONTAINER_NAME
from gscoordinator.constants import INTERACTIVE_FRONTEND_CONTAINER_NAME
from gscoordinator.utils import parse_as_glog_level
from gscoordinator.version import __version__

logger = logging.getLogger("graphscope")

BASE_MACHINE_ENVS = {
    "MY_NODE_NAME": "spec.nodeName",
    "MY_POD_NAME": "metadata.name",
    "MY_POD_NAMESPACE": "metadata.namespace",
    "MY_POD_IP": "status.podIP",
    "MY_HOST_NAME": "status.podIP",
}

_annotations = {
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-type": "tcp",
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-connect-timeout": "8",
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-healthy-threshold": "2",
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-unhealthy-threshold": "2",
    "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-interval": "1",
}


class EngineCluster:
    def __init__(
        self,
        config: Config,
        engine_pod_prefix,
        graphlearn_start_port,
        graphlearn_torch_start_port,
    ):
        self._instance_id = config.session.instance_id
        self._glog_level = parse_as_glog_level(config.log_level)
        self._num_workers = config.session.num_workers

        launcher_config: KubernetesLauncherConfig = config.kubernetes_launcher

        self._namespace = launcher_config.namespace
        self._service_type = launcher_config.service_type

        self._engine_resources = launcher_config.engine

        self._with_dataset = launcher_config.dataset.enable

        self._with_analytical = launcher_config.engine.enable_gae
        self._with_analytical_java = launcher_config.engine.enable_gae_java
        self._with_interactive = launcher_config.engine.enable_gie
        self._with_graphlearn = launcher_config.engine.enable_gle
        self._with_graphlearn_torch = launcher_config.engine.enable_glt
        self._with_mars = launcher_config.mars.enable

        def load_base64_json(string):
            if string is None:
                return None
            json_str = base64.b64decode(string).decode("utf-8", errors="ignore")
            return json.loads(json_str)

        self._node_selector = load_base64_json(launcher_config.engine.node_selector)

        self._volumes = load_base64_json(launcher_config.volumes)

        self._dataset_proxy = load_base64_json(launcher_config.dataset.proxy)

        self._image_pull_policy = launcher_config.image.pull_policy
        self._image_pull_secrets = launcher_config.image.pull_secrets

        registry = launcher_config.image.registry
        repository = launcher_config.image.repository
        tag = launcher_config.image.tag

        image_prefix = f"{registry}/{repository}" if registry else repository
        self._analytical_image = f"{image_prefix}/analytical:{tag}"
        self._analytical_java_image = f"{image_prefix}/analytical-java:{tag}"
        self._interactive_frontend_image = f"{image_prefix}/interactive-frontend:{tag}"
        self._interactive_executor_image = f"{image_prefix}/interactive-executor:{tag}"
        self._graphlearn_image = f"{image_prefix}/graphlearn:{tag}"
        self._graphlearn_torch_image = f"{image_prefix}/graphlearn-torch:{tag}"
        self._dataset_image = f"{image_prefix}/dataset:{tag}"

        self._vineyard_deployment = config.vineyard.deployment_name
        self._vineyard_image = config.vineyard.image
        self._vineyard_service_port = config.vineyard.rpc_port
        self._sock = "/tmp/vineyard_workspace/vineyard.sock"

        self._dataset_requests = {"cpu": "200m", "memory": "64Mi"}

        self._engine_pod_prefix = engine_pod_prefix
        self._analytical_prefix = "gs-analytical-"
        self._interactive_frontend_prefix = "gs-interactive-frontend-"
        self._graphlearn_prefix = "gs-graphlearn-"
        self._graphlearn_torch_prefix = "gs-graphlearn-torch-"
        self._vineyard_prefix = "vineyard-"
        self._mars_scheduler_name_prefix = "mars-scheduler-"
        self._mars_service_name_prefix = "mars-"

        self._graphlearn_start_port = graphlearn_start_port
        self._graphlearn_torch_start_port = graphlearn_torch_start_port

        self._engine_labels = {
            "app.kubernetes.io/name": "graphscope",
            "app.kubernetes.io/instance": self._instance_id,
            "app.kubernetes.io/version": __version__,
            "app.kubernetes.io/component": "engine",
            "app.kubernetes.io/engine_selector": self.engine_stateful_set_name,
        }

        self._frontend_labels = self._engine_labels.copy()
        self._frontend_labels["app.kubernetes.io/component"] = "frontend"

    @property
    def vineyard_ipc_socket(self):
        return self._sock

    def vineyard_deployment_exists(self):
        return self._vineyard_deployment is not None

    def get_common_env(self):
        def put_if_exists(env: dict, key: str):
            if key in os.environ:
                env[key] = os.environ[key]

        env = {
            "GLOG_v": str(self._glog_level),
            "VINEYARD_IPC_SOCKET": self.vineyard_ipc_socket,
            "WITH_VINEYARD": "ON",
        }
        put_if_exists(env, "OPAL_PREFIX")
        put_if_exists(env, "OPAL_BINDIR")
        env = [kube_client.V1EnvVar(name=k, value=v) for k, v in env.items()]
        return env

    def get_dataset_proxy_env(self):
        return [
            kube_client.V1EnvVar(name=k, value=v)
            for k, v in self._dataset_proxy.items()
        ]

    def get_base_machine_env(self):
        env = [
            ResourceBuilder.get_value_from_field_ref(key, value)
            for key, value in BASE_MACHINE_ENVS.items()
        ]
        return env

    def get_shm_volume(self):
        name = "host-shm"
        volume = kube_client.V1Volume(name=name)
        volume.empty_dir = kube_client.V1EmptyDirVolumeSource()
        volume.empty_dir.medium = "Memory"

        source_volume_mount = kube_client.V1VolumeMount(
            name=name, mount_path="/dev/shm"
        )
        destination_volume_mount = source_volume_mount

        return volume, source_volume_mount, destination_volume_mount

    def get_dataset_volume(self):
        name = "dataset"
        volume = kube_client.V1Volume(name=name)
        volume.empty_dir = kube_client.V1EmptyDirVolumeSource()

        source_volume_mount = kube_client.V1VolumeMount(
            name=name, mount_path="/dataset"
        )
        source_volume_mount.mount_propagation = "Bidirectional"

        # volume mount in engine container
        destination_volume_mount = kube_client.V1VolumeMount(
            name=name, mount_path="/dataset"
        )
        destination_volume_mount.read_only = True
        destination_volume_mount.mount_propagation = "HostToContainer"

        return volume, source_volume_mount, destination_volume_mount

    def get_engine_container_helper(self, name, image, args, volume_mounts, resource):
        container = kube_client.V1Container(
            name=name, image=image, args=args, volume_mounts=volume_mounts
        )
        container.image_pull_policy = self._image_pull_policy
        container.env = self.get_common_env()
        requests, limits = resource.get_requests(), resource.get_limits()
        container.resources = ResourceBuilder.get_resources(requests, limits)
        return container

    def _get_tail_if_exists_cmd(self, fname: str):
        return (
            f"while true; do if [ -e {fname} ]; then tail -f {fname}; fi; sleep 1; done"
        )

    def get_analytical_container(self, volume_mounts, with_java=False):
        name = ANALYTICAL_CONTAINER_NAME
        image = self._analytical_image if not with_java else self._analytical_java_image
        args = ["bash", "-c", self._get_tail_if_exists_cmd("/tmp/grape_engine.INFO")]
        resource = self._engine_resources.gae_resource
        container = self.get_engine_container_helper(
            name, image, args, volume_mounts, resource
        )

        readiness_probe = kube_client.V1Probe()
        command = ["/bin/bash", "-c", f"ls {self._sock} 2>/dev/null"]
        readiness_probe._exec = kube_client.V1ExecAction(command=command)
        readiness_probe.initial_delay_seconds = 5
        readiness_probe.period_seconds = 2
        readiness_probe.failure_threshold = 3
        container.readiness_probe = readiness_probe

        # container.lifecycle = self.get_lifecycle()
        return container

    def get_interactive_executor_container(self, volume_mounts):
        name = INTERACTIVE_EXECUTOR_CONTAINER_NAME
        image = self._interactive_executor_image
        args = [
            "bash",
            "-c",
            self._get_tail_if_exists_cmd("/var/log/graphscope/current/executor.*.log"),
        ]
        resource = self._engine_resources.gie_executor_resource
        container = self.get_engine_container_helper(
            name, image, args, volume_mounts, resource
        )
        return container

    def get_graphlearn_container(self, volume_mounts):
        name = GRAPHLEARN_CONTAINER_NAME
        image = self._graphlearn_image
        args = ["tail", "-f", "/dev/null"]
        resource = self._engine_resources.gle_resource
        container = self.get_engine_container_helper(
            name, image, args, volume_mounts, resource
        )
        container.ports = [
            kube_client.V1ContainerPort(container_port=p)
            for p in range(
                self._graphlearn_start_port, self._graphlearn_start_port + 1000
            )
        ]
        return container

    def get_graphlearn_torch_container(self, volume_mounts):
        name = GRAPHLEARN_TORCH_CONTAINER_NAME
        image = self._graphlearn_torch_image
        args = ["tail", "-f", "/dev/null"]
        resource = self._engine_resources.glt_resource
        container = self.get_engine_container_helper(
            name, image, args, volume_mounts, resource
        )
        container.ports = [
            kube_client.V1ContainerPort(container_port=p)
            for p in range(
                self._graphlearn_torch_start_port,
                self._graphlearn_torch_start_port + 1000,
            )
        ]
        return container

    def get_mars_container(self):
        pass

    def get_dataset_container(self, volume_mounts):
        name = DATASET_CONTAINER_NAME
        container = kube_client.V1Container(name=name)
        container.image = self._dataset_image
        container.image_pull_policy = self._image_pull_policy

        container.resources = ResourceBuilder.get_resources(
            self._dataset_requests, self._dataset_requests
        )

        container.volume_mounts = volume_mounts
        if self._dataset_proxy and self._dataset_proxy is not None:
            container.env = self.get_dataset_proxy_env()

        container.security_context = kube_client.V1SecurityContext(privileged=True)
        return container

    def get_vineyard_socket_volume(self):
        name = "vineyard-ipc-socket"

        # Notice, the path must be same as the one in vineyardd_types.go
        # https://github.com/v6d-io/v6d/blob/main/k8s/apis/k8s/v1alpha1/vineyardd_types.go#L125
        path = f"/var/run/vineyard-kubernetes/{self._namespace}/{self._vineyard_deployment}"
        host_path = kube_client.V1HostPathVolumeSource(path=path)
        host_path.type = "Directory"
        volume = kube_client.V1Volume(name=name, host_path=host_path)
        volume_mount = kube_client.V1VolumeMount(
            name=name, mount_path="/tmp/vineyard_workspace"
        )
        return volume, volume_mount

    def get_engine_pod_spec(self):
        containers = []

        volume, _, volume_mount = self.get_shm_volume()
        volumes = [volume]
        engine_volume_mounts = [volume_mount]

        if self.vineyard_deployment_exists():
            volume, volume_mount = self.get_vineyard_socket_volume()
            volumes.append(volume)
            engine_volume_mounts.append(volume_mount)

        if self._volumes and self._volumes is not None:
            volume, _, volume_mount = ResourceBuilder.get_user_defined_volumes(
                self._volumes
            )
            volumes.extend(volume)
            engine_volume_mounts.extend(volume_mount)

        if self._with_analytical:
            containers.append(
                self.get_analytical_container(volume_mounts=engine_volume_mounts)
            )
        if self._with_analytical_java:
            containers.append(
                self.get_analytical_container(
                    volume_mounts=engine_volume_mounts, with_java=True
                )
            )
        if self._with_interactive:
            containers.append(
                self.get_interactive_executor_container(
                    volume_mounts=engine_volume_mounts
                )
            )
        if self._with_graphlearn:
            containers.append(
                self.get_graphlearn_container(volume_mounts=engine_volume_mounts)
            )
        if self._with_graphlearn_torch:
            containers.append(
                self.get_graphlearn_torch_container(volume_mounts=engine_volume_mounts)
            )

        if self._with_dataset:
            volume, src_volume_mount, dst_volume_mount = self.get_dataset_volume()
            volumes.append(volume)
            containers.append(
                self.get_dataset_container(volume_mounts=[src_volume_mount])
            )
            engine_volume_mounts.append(dst_volume_mount)

        # if self._with_mars:
        #     containers.append(self.get_mars_container())

        return ResourceBuilder.get_pod_spec(
            containers=containers,
            image_pull_secrets=self._image_pull_secrets,
            node_selector=self._node_selector,
            volumes=volumes,
        )

    def get_engine_pod_template_spec(self):
        spec = self.get_engine_pod_spec()
        if self._with_analytical or self._with_analytical_java:
            default_container = ANALYTICAL_CONTAINER_NAME
        else:
            default_container = None
        return ResourceBuilder.get_pod_template_spec(
            spec, self._engine_labels, default_container=default_container
        )

    def get_engine_stateful_set(self):
        name = self.engine_stateful_set_name
        template = self.get_engine_pod_template_spec()
        replicas = self._num_workers
        service_name = name + "-headless"
        spec = ResourceBuilder.get_stateful_set_spec(
            template, replicas, self._engine_labels, service_name
        )
        return ResourceBuilder.get_stateful_set(
            self._namespace, name, spec, self._engine_labels
        )

    def get_engine_headless_service(self):
        name = self.engine_stateful_set_name + "-headless"
        ports = [kube_client.V1ServicePort(name="etcd", port=2379)]
        service_spec = ResourceBuilder.get_service_spec(
            "ClusterIP", ports, self._engine_labels, None
        )

        # Necessary, create a headless service for statefulsets
        service_spec.cluster_ip = "None"
        service = ResourceBuilder.get_service(
            self._namespace, name, service_spec, self._engine_labels
        )
        return service

    def get_graphlearn_service(self, object_id, start_port):
        service_type = self._service_type
        num_workers = self._num_workers
        name = self.get_graphlearn_service_name(object_id)
        ports = []
        for i in range(start_port, start_port + num_workers):
            port = kube_client.V1ServicePort(name=f"{name}-{i}", port=i, protocol="TCP")
            ports.append(port)
        service_spec = ResourceBuilder.get_service_spec(
            service_type, ports, self._engine_labels, "Local"
        )
        service = ResourceBuilder.get_service(
            self._namespace, name, service_spec, self._engine_labels
        )
        return service

    def get_graphlearn_torch_service(self, object_id, start_port):
        service_type = self._service_type
        num_workers = self._num_workers
        name = self.get_graphlearn_torch_service_name(object_id)
        ports = []
        for i in range(start_port, start_port + num_workers):
            port = kube_client.V1ServicePort(name=f"{name}-{i}", port=i, protocol="TCP")
            ports.append(port)
        service_spec = ResourceBuilder.get_service_spec(
            service_type, ports, self._engine_labels, "Local"
        )
        service = ResourceBuilder.get_service(
            self._namespace, name, service_spec, self._engine_labels
        )
        return service

    def get_graphlearn_ports(self, start_port):
        num_workers = self._num_workers
        return [i for i in range(start_port, start_port + num_workers)]

    def get_graphlearn_torch_ports(self, start_port):
        num_loaders = 4
        return [i for i in range(start_port, start_port + num_loaders)]

    @property
    def engine_stateful_set_name(self):
        return f"{self._engine_pod_prefix}{self._instance_id}"

    @property
    def frontend_deployment_name(self):
        return f"{self._interactive_frontend_prefix}{self._instance_id}"

    @property
    def vineyard_service_name(self):
        return f"{self.engine_stateful_set_name}-{self._instance_id}-vineyard-rpc"

    def get_vineyard_service_endpoint(self, api_client):
        # return f"{self.vineyard_service_name}:{self._vineyard_service_port}"
        if self.vineyard_deployment_exists():
            service_name = self._vineyard_deployment + "-rpc"
        else:
            service_name = self.vineyard_service_name
        endpoints = get_service_endpoints(
            api_client=api_client,
            namespace=self._namespace,
            name=service_name,
            service_type=self._service_type,
        )
        assert len(endpoints) > 0
        return endpoints[0]

    def get_graphlearn_service_name(self, object_id):
        return f"{self._graphlearn_prefix}{object_id}"

    def get_graphlearn_torch_service_name(self, object_id):
        return f"{self._graphlearn_torch_prefix}{object_id}"

    def get_graphlearn_service_endpoint(self, api_client, object_id, pod_host_ip_list):
        service_name = self.get_graphlearn_service_name(object_id)
        service_type = self._service_type
        core_api = kube_client.CoreV1Api(api_client)
        if service_type == "NodePort":
            # TODO: add label_selector to filter the service
            services = core_api.list_namespaced_service(self._namespace)
            for svc in services.items:
                if svc.metadata.name == service_name:
                    endpoints = []
                    for ip, port_spec in zip(pod_host_ip_list, svc.spec.ports):
                        endpoints.append(
                            (
                                f"{ip}:{port_spec.node_port}",
                                int(port_spec.name.split("-")[-1]),
                            )
                        )
                    endpoints.sort(key=lambda ep: ep[1])
                    return [ep[0] for ep in endpoints]
        elif service_type == "LoadBalancer":
            endpoints = get_service_endpoints(
                api_client=api_client,
                namespace=self._namespace,
                name=service_name,
                service_type=service_type,
            )
            return endpoints
        raise RuntimeError("Get graphlearn service endpoint failed.")

    def get_interactive_frontend_container(self):
        name = INTERACTIVE_FRONTEND_CONTAINER_NAME
        image = self._interactive_frontend_image
        args = [
            "bash",
            "-c",
            self._get_tail_if_exists_cmd("/var/log/graphscope/current/frontend.log"),
        ]
        container = kube_client.V1Container(name=name, image=image, args=args)
        container.image_pull_policy = self._image_pull_policy
        resource = self._engine_resources.gie_frontend_resource
        requests, limits = resource.get_requests(), resource.get_limits()
        container.resources = ResourceBuilder.get_resources(requests, limits)
        return container

    def get_interactive_frontend_deployment(self, replicas=1):
        name = self.frontend_deployment_name

        container = self.get_interactive_frontend_container()
        pod_spec = ResourceBuilder.get_pod_spec(
            containers=[container],
        )
        template_spec = ResourceBuilder.get_pod_template_spec(
            pod_spec, self._frontend_labels
        )
        deployment_spec = ResourceBuilder.get_deployment_spec(
            template_spec, replicas, self._frontend_labels
        )
        return ResourceBuilder.get_deployment(
            self._namespace, name, deployment_spec, self._frontend_labels
        )

    def get_interactive_frontend_service(self, gremlin_port, cypher_port):
        name = self.frontend_deployment_name
        service_type = self._service_type
        ports = [
            kube_client.V1ServicePort(name="gremlin", port=gremlin_port),
            kube_client.V1ServicePort(name="cypher", port=cypher_port),
        ]
        service_spec = ResourceBuilder.get_service_spec(
            service_type, ports, self._frontend_labels, None
        )
        service = ResourceBuilder.get_service(
            self._namespace, name, service_spec, self._frontend_labels, _annotations
        )
        return service


class MarsCluster:
    def __init__(self, instance_id, namespace, service_type):
        self._mars_prefix = "mars-"
        self._mars_scheduler_port = 7103  # fixed
        self._mars_scheduler_web_port = 7104  # fixed
        self._mars_worker_port = 7105  # fixed
        self._instance_id = instance_id
        self._namespace = namespace
        self._service_type = service_type

        self._mars_worker_requests = None
        self._mars_scheduler_requests = None

    def get_mars_deployment(self):
        pass

    def get_mars_service(self):
        pass

    @property
    def mars_scheduler_service_name(self):
        return f"{self._mars_prefix}{self._instance_id}"

    @property
    def mars_scheduler_web_port(self):
        return self._mars_scheduler_web_port

    def get_mars_service_endpoint(self, api_client):
        # Always len(endpoints) >= 1
        service_name = self.mars_scheduler_service_name
        service_type = self._service_type
        web_port = self.mars_scheduler_web_port
        endpoints = get_service_endpoints(
            api_client=api_client,
            namespace=self._namespace,
            name=service_name,
            service_type=service_type,
            query_port=web_port,
        )
        assert len(endpoints) > 0
        return f"http://{endpoints[0]}"
