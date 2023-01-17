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


import logging
import sys

from kubernetes import client as kube_client

from graphscope.deploy.kubernetes.utils import parse_readable_memory
from graphscope.framework.utils import get_tempdir

logger = logging.getLogger("graphscope")


class ResourceBuilder:
    @staticmethod
    def get_configmap(name, kvs):
        metadata = kube_client.V1ObjectMeta(name=name)
        configmap = kube_client.V1ConfigMap(metadata=metadata, data=kvs)
        return configmap

    @staticmethod
    def get_role(name, namespace, api_groups, resources, verbs, labels):
        metadata = kube_client.V1ObjectMeta(name=name, namespace=namespace)
        metadata.labels = labels
        rule = kube_client.V1PolicyRule(
            api_groups=api_groups.split(','),
            resources=resources.split(','),
            verbs=verbs.split(','),
        )
        role = kube_client.V1Role(metadata=metadata, rules=[rule])
        return role

    @staticmethod
    def get_cluster_role(name, api_groups, resources, verbs, labels):
        metadata = kube_client.V1ObjectMeta(name=name, labels=labels)
        rule = kube_client.V1PolicyRule(
            api_groups=api_groups.split(','),
            resources=resources.split(','),
            verbs=verbs.split(','),
        )
        role = kube_client.V1ClusterRole(metadata=metadata, rules=[rule])
        return role

    @staticmethod
    def get_role_binding(name, namespace, role_name, service_account_name, labels):
        metadata = kube_client.V1ObjectMeta(name=name, namespace=namespace)
        metadata.labels = labels
        role_ref = kube_client.V1RoleRef(
            kind="Role", name=role_name, api_group="rbac.authorization.k8s.io"
        )
        subject = kube_client.V1Subject(
            kind="ServiceAccount", name=service_account_name, namespace=namespace
        )
        role_binding = kube_client.V1RoleBinding(
            metadata=metadata, role_ref=role_ref, subjects=[subject]
        )
        return role_binding

    @staticmethod
    def get_cluster_role_binding(name, namespace, role_name, service_account_name, labels):
        metadata = kube_client.V1ObjectMeta(name=name, labels=labels)
        role_ref = kube_client.V1RoleRef(
            kind="ClusterRole", name=role_name, api_group="rbac.authorization.k8s.io"
        )
        subject = kube_client.V1Subject(
            kind="ServiceAccount", name=service_account_name, namespace=namespace
        )
        role_binding = kube_client.V1ClusterRoleBinding(
            metadata=metadata, role_ref=role_ref, subjects=[subject]
        )
        return role_binding

    @staticmethod
    def get_tcp_probe(port, timeout=15, period=10, failure_threshold=8):
        return kube_client.V1Probe(
            tcp_socket=kube_client.V1TCPSocketAction(port=port),
            timeout_seconds=timeout,
            period_seconds=period,
            failure_threshold=failure_threshold,
        )

    @staticmethod
    def get_exec_action(command):
        return kube_client.V1ExecAction(command=command)

    @staticmethod
    def get_lifecycle_handler(_exec=None, http_get=None, tcp_socket=None):
        handler = kube_client.V1LifecycleHandler(_exec=_exec, http_get=http_get, tcp_socket=tcp_socket)
        return handler

    @staticmethod
    def get_lifecycle(post_start=None, pre_stop=None):
        return kube_client.V1Lifecycle(post_start=post_start, pre_stop=pre_stop)

    @staticmethod
    def get_image_pull_secrets(image_pull_secrets):
        """
        for name in self._image_pull_secrets:
            engine_builder.add_image_pull_secret(name)
        """
        local_object_refs = []
        for name in image_pull_secrets:
            local_object_refs.append(kube_client.V1LocalObjectReference(name=name))
        return local_object_refs

    @staticmethod
    def get_node_selector(node_selector):
        return node_selector

    @staticmethod
    def get_user_defined_volumes(udf_volumes):
        """
            {
                name: {
                    "type": "",
                    "field": {},  # the keys are subject to volume type
                    "mounts": [ {"mountPath": "", "subPath": ""}, ... ]
                    }
            }
        """
        if not udf_volumes:
            return [], [], []
        volumes, source_volume_mounts = [], []
        for name, value in udf_volumes.items():
            volume = kube_client.V1Volume(name=name)
            field = value.get("field", {})
            if value['type'] == 'hostPath':
                volume.host_path = kube_client.V1HostPathVolumeSource(path=field['path'])
                if 'type' in field:
                    volume.host_path.type = field['type']
            elif value['type'] == 'emptyDir':
                volume.empty_dir = kube_client.V1EmptyDirVolumeSource()
                if 'medium' in field:
                    volume.empty_dir.medium = field['medium']
                if 'sizeLimit' in field:
                    volume.empty_dir.size_limit = field['sizeLimit']
            elif value['type'] == 'persistentVolumeClaim':
                pvc = kube_client.V1PersistentVolumeClaimVolumeSource(claim_name=field['claimName'])
                volume.persistent_volume_claim = pvc
                if 'readOnly' in field:
                    volume.persistent_volume_claim.read_only = field['readOnly']
            elif value['type'] == 'configMap':
                volume.config_map = kube_client.V1ConfigMapVolumeSource(name=field['name'])
            elif value['type'] == 'secret':
                volume.secret = kube_client.V1SecretVolumeSource(secret_name=field['name'])
            else:
                raise ValueError(f"Unsupported volume type: {value['type']}")
            volume_mounts = []
            mounts_list = value['mounts']
            if not isinstance(mounts_list, list):
                mounts_list = [value['mounts']]
            for udf_mount in mounts_list:
                volume_mount = kube_client.V1VolumeMount(name=name, mount_path=udf_mount['mountPath'])
                if 'subPath' in udf_mount:
                    volume_mount.sub_path = udf_mount['subPath']
                if 'readOnly' in udf_mount:
                    volume_mount.read_only = udf_mount['readOnly']
                volume_mounts.append(volume_mount)
            volumes.append(volume)
            source_volume_mounts.extend(volume_mounts)
        # Assume destination mounts are the same as source mounts
        destination_volume_mounts = source_volume_mounts
        return volumes, source_volume_mounts, destination_volume_mounts

    @staticmethod
    def get_resources(requests, limits, preemptive=True):
        resource_requirements = kube_client.V1ResourceRequirements()
        if not preemptive and requests is not None:
            resource_requirements.requests = requests
        if limits is not None:
            resource_requirements.limits = limits
        return resource_requirements

    @staticmethod
    def get_pod_spec(containers: [kube_client.V1Container], image_pull_secrets=None, node_selector=None, volumes=None):
        pod_spec = kube_client.V1PodSpec(containers=containers)
        if image_pull_secrets is not None and image_pull_secrets:
            pod_spec.image_pull_secrets = ResourceBuilder.get_image_pull_secrets(image_pull_secrets)
        if node_selector is not None and node_selector:
            pod_spec.node_selector = ResourceBuilder.get_node_selector(node_selector)
        if volumes is not None and volumes:
            pod_spec.volumes = volumes
        return pod_spec

    @staticmethod
    def get_pod_template_spec(spec: kube_client.V1PodSpec, labels: dict, annotations=None, default_container=None):
        pod_template_spec = kube_client.V1PodTemplateSpec()
        pod_template_spec.spec = spec
        if annotations is None:
            annotations = dict()
        if default_container is not None:
            annotations['kubectl.kubernetes.io/default-container'] = default_container
        pod_template_spec.metadata = kube_client.V1ObjectMeta(labels=labels, annotations=annotations)
        return pod_template_spec

    @staticmethod
    def get_deployment_spec(template, replicas, labels):
        selector = kube_client.V1LabelSelector(match_labels=labels)
        spec = kube_client.V1DeploymentSpec(selector=selector, template=template)
        spec.replicas = replicas
        return spec

    @staticmethod
    def get_deployment(namespace, name, spec, labels):
        deployment = kube_client.V1Deployment()
        deployment.api_version = "apps/v1"
        deployment.kind = "Deployment"
        deployment.metadata = kube_client.V1ObjectMeta(name=name, labels=labels, namespace=namespace)
        deployment.spec = spec
        return deployment

    @staticmethod
    def get_stateful_set_spec(template, replicas, labels, service_name):
        selector = kube_client.V1LabelSelector(match_labels=labels)
        spec = kube_client.V1StatefulSetSpec(selector=selector, template=template, service_name=service_name)
        spec.replicas = replicas
        return spec

    @staticmethod
    def get_stateful_set(namespace, name, spec, labels):
        statefulset = kube_client.V1StatefulSet()
        statefulset.api_version = "apps/v1"
        statefulset.kind = "StatefulSet"
        statefulset.metadata = kube_client.V1ObjectMeta(name=name, labels=labels, namespace=namespace)
        statefulset.spec = spec
        return statefulset

    @staticmethod
    def get_value_from_field_ref(name, field_path):
        env = kube_client.V1EnvVar(name=name)
        value_from = kube_client.V1EnvVarSource()
        value_from.field_ref = kube_client.V1ObjectFieldSelector(field_path=field_path)
        env.value_from = value_from
        return env

    @staticmethod
    def get_namespace(name):
        namespace = kube_client.V1Namespace()
        namespace.metadata = kube_client.V1ObjectMeta(name=name)
        namespace.metadata.labels = {"kubernetes.io/metadata.name": name}
        return namespace

    @staticmethod
    def get_service_spec(type, ports, labels, external_traffic_policy):
        service_spec = kube_client.V1ServiceSpec()
        service_spec.type = type
        service_spec.selector = labels
        service_spec.ports = ports
        if external_traffic_policy is not None:
            service_spec.external_traffic_policy = external_traffic_policy
        return service_spec

    @staticmethod
    def get_service(namespace, name, service_spec, labels, annotations=None):
        service = kube_client.V1Service()
        service.api_version = "v1"
        service.kind = "Service"
        service.spec = service_spec
        metadata = kube_client.V1ObjectMeta(namespace=namespace, name=name, labels=labels, annotations=annotations)
        service.metadata = metadata
        return service


class CoordinatorDeployment:
    def __init__(self, namespace, name, image, args, labels, image_pull_secret,
                 image_pull_policy, node_selector, env, host_network, port=None):
        self._replicas = 1
        self._namespace = namespace
        self._name = name
        self._image = image
        self._args = args
        self._labels = labels
        self._image_pull_policy = image_pull_policy
        self._image_pull_secret = image_pull_secret
        self._env: dict = env
        self._port = port
        self._host_network = host_network
        self._node_selector = node_selector
        self._requests = {"cpu": 0.5, "memory": "512Mi"}
        self._limits = {"cpu": 0.5, "memory": "512Mi"}

    def get_lifecycle(self):
        pre_stop = ["/opt/rh/rh-python38/root/usr/bin/python3", "-m", "gscoordinator.hook.prestop"]
        _exec = ResourceBuilder.get_exec_action(pre_stop)
        lifecycle_handler = ResourceBuilder.get_lifecycle_handler(_exec)
        lifecycle = ResourceBuilder.get_lifecycle(pre_stop=lifecycle_handler)
        return lifecycle

    def get_coordinator_container(self):
        resources = ResourceBuilder.get_resources(self._requests, self._limits)
        lifecycle = self.get_lifecycle()
        env = [kube_client.V1EnvVar(name=key, value=value) for key, value in self._env.items()]
        container = kube_client.V1Container(
            name="coordinator",
            image=self._image,
            image_pull_policy=self._image_pull_policy,
            args=self._args,
            resources=resources,
            lifecycle=lifecycle,
            env=env,
        )

        if self._port is not None:
            container_ports = [kube_client.V1ContainerPort(container_port=self._port)]
            container_ports.append(kube_client.V1ContainerPort(container_port=8000))
            container.ports = container_ports
            container.readiness_probe = ResourceBuilder.get_tcp_probe(port=self._port,
                                                                      timeout=15,
                                                                      period=1,
                                                                      failure_threshold=20)
        return container

    def get_coordinator_pod_spec(self):
        container = self.get_coordinator_container()
        pod_spec = ResourceBuilder.get_pod_spec(containers=[container],
                                                image_pull_secrets=self._image_pull_secret,
                                                node_selector=self._node_selector)
        pod_spec.host_network = self._host_network
        return pod_spec

    def get_coordinator_pod_template_spec(self):
        spec = self.get_coordinator_pod_spec()
        return ResourceBuilder.get_pod_template_spec(spec, self._labels, default_container='coordinator')

    def get_coordinator_deployment_spec(self, replicas):
        template = self.get_coordinator_pod_template_spec()
        spec = ResourceBuilder.get_deployment_spec(template, replicas, self._labels)
        return spec

    def get_coordinator_deployment(self):
        spec = self.get_coordinator_deployment_spec(self._replicas)
        return ResourceBuilder.get_deployment(self._namespace, self._name, spec, self._labels)

    def get_coordinator_service(self, service_type, port):
        ports = [kube_client.V1ServicePort(name="coordinator", port=port)]
        ports.append(kube_client.V1ServicePort(name="debug", port=8000))
        service_spec = ResourceBuilder.get_service_spec(service_type, ports, self._labels, None)
        service = ResourceBuilder.get_service(self._namespace, self._name, service_spec, self._labels)
        return service
