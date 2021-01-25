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
import shutil

from graphscope.deploy.kubernetes.utils import parse_readable_memory

logger = logging.getLogger("graphscope")


def _remove_nones(o):
    return dict((k, v) for k, v in o.items() if v is not None)


def resolve_volume_builder(name, value):
    """Resolve specified volume with value dict."""
    supported_type = ["hostPath"]
    if "type" not in value or "field" not in value or "mounts" not in value:
        logger.warning("Volume %s must contains 'type' 'field' and 'mounts'", name)
        return None
    type = value["type"]
    if type not in supported_type:
        logger.warning("%s of volume %s is unsupported yet", type, name)
        return None
    field = value["field"]
    mounts_list = value["mounts"]
    if type == "hostPath":
        return HostPathVolumeBuilder(name=name, field=field, mounts_list=mounts_list)
    return None


class NamespaceBuilder(object):
    """Builder for k8s namespace."""

    def __init__(self, name):
        self._name = name

    def build(self):
        return {
            "kind": "Namespace",
            "metadata": {
                "name": self._name,
                "labels": {
                    "name": self._name,
                },
            },
        }


class LocalObjectRefBuilder(object):
    """Builder for k8s LocalObjectReference."""

    def __init__(self, name):
        self._name = name

    def build(self):
        return {"name": self._name}


class RoleBuilder(object):
    """Builder for k8s RBAC roles."""

    def __init__(self, name, namespace, api_groups, resources, verbs):
        self._name = name
        self._namespace = namespace
        self._api_groups = api_groups.split(",")
        self._resources = resources.split(",")
        self._verbs = verbs.split(",")

    def build(self):
        return {
            "kind": "Role",
            "metadata": {"name": self._name, "namespace": self._namespace},
            "rules": [
                {
                    "apiGroups": self._api_groups,
                    "resources": self._resources,
                    "verbs": self._verbs,
                }
            ],
        }


class ClusterRoleBuilder(object):
    """Builder for k8s RBAC roles."""

    def __init__(self, name, api_groups, resources, verbs):
        self._name = name
        self._api_groups = api_groups.split(",")
        self._resources = resources.split(",")
        self._verbs = verbs.split(",")

    def build(self):
        return {
            "kind": "ClusterRole",
            "metadata": {"name": self._name},
            "rules": [
                {
                    "apiGroups": self._api_groups,
                    "resources": self._resources,
                    "verbs": self._verbs,
                }
            ],
        }


class RoleBindingBuilder(object):
    """Builder for k8s RBAC role bindings."""

    def __init__(self, name, namespace, role_name, service_account_name):
        self._name = name
        self._namespace = namespace
        self._role_name = role_name
        self._service_account_name = service_account_name

    def build(self):
        return {
            "kind": "RoleBinding",
            "metadata": {"name": self._name, "namespace": self._namespace},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": self._role_name,
            },
            "subjects": [
                {
                    "kind": "ServiceAccount",
                    "name": self._service_account_name,
                    "namespace": self._namespace,
                }
            ],
        }


class ClusterRoleBindingBuilder(object):
    """Builder for k8s RBAC cluster role bindings."""

    def __init__(self, name, namespace, cluster_role_name, service_account_name):
        self._name = name
        self._namespace = namespace
        self._cluster_role_name = cluster_role_name
        self._service_account_name = service_account_name

    def build(self):
        return {
            "kind": "ClusterRoleBinding",
            "metadata": {"name": self._name},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": self._cluster_role_name,
            },
            "subjects": [
                {
                    "kind": "ServiceAccount",
                    "name": self._service_account_name,
                    "namespace": self._namespace,
                }
            ],
        }


class ServiceBuilder(object):
    """Builder for k8s services."""

    _annotations = {
        "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-type": "tcp",
        "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-connect-timeout": "8",
        "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-healthy-threshold": "2",
        "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-unhealthy-threshold": "2",
        "service.beta.kubernetes.io/alibaba-cloud-loadbalancer-health-check-interval": "1",
    }

    def __init__(
        self,
        name,
        service_type,
        selector,
        port,
        target_port=None,
        node_port=None,
        protocol=None,
        external_traffic_policy=None,
    ):
        self._name = name
        self._type = service_type
        self._protocol = protocol or "TCP"
        self._selector = selector
        self._port = port
        self._target_port = target_port
        self._node_port = node_port
        self._external_traffic_policy = external_traffic_policy

    def build(self):
        if isinstance(self._port, (range, list, tuple)):
            ports = []
            for idx, port in enumerate(self._port):
                ports.append(
                    _remove_nones(
                        {
                            "name": "%s-%d" % (self._name, idx),
                            "protocol": self._protocol,
                            "port": port,
                        }
                    )
                )
        else:
            ports = [
                _remove_nones(
                    {
                        "protocol": self._protocol,
                        "port": self._port,
                        "targetPort": self._target_port,
                        "nodePort": self._node_port,
                    }
                ),
            ]

        return {
            "kind": "Service",
            "metadata": {
                "annotations": self._annotations,
                "name": self._name,
            },
            "spec": _remove_nones(
                {
                    "type": self._type,
                    "selector": self._selector,
                    "ports": ports,
                    "externalTrafficPolicy": self._external_traffic_policy,
                }
            ),
        }


class ContainerEnvBuilder(object):
    """Builder for k8s container environments."""

    def __init__(self, name, value):
        self._name = name
        self._value = value

    def build(self):
        result = dict(name=self._name)
        result["value"] = str(self._value)
        return result


class PortBuilder(object):
    """Builder for k8s container port definition."""

    def __init__(self, container_port):
        self._container_port = int(container_port)

    def build(self):
        return {
            "containerPort": self._container_port,
        }


class ResourceBuilder(object):
    """Builder for k8s computation resources."""

    def __init__(self, cpu, memory):
        self._cpu = cpu
        self._memory = parse_readable_memory(memory)

    def build(self):
        return {
            "cpu": float(self._cpu),
            "memory": str(self._memory),
        }


class VolumeBuilder(object):
    """Builder for k8s volumes."""

    def __init__(self, name, mounts_list):
        self._name = name
        self._mounts_list = mounts_list
        if not isinstance(self._mounts_list, list):
            self._mounts_list = [self._mounts_list]

        for mount in self._mounts_list:
            mount["name"] = self._name

    def build(self):
        raise NotImplementedError

    def build_mount(self):
        return self._mounts_list


class HostPathVolumeBuilder(VolumeBuilder):
    """Builder for k8s host volume."""

    def __init__(self, name, field, mounts_list):
        super().__init__(name, mounts_list)
        self._field = field

    def build(self):
        return {"name": self._name, "hostPath": self._field}


class EmptyDirVolumeBuilder(VolumeBuilder):
    """Builder for k8s empty-dir volumes."""

    def __init__(self, name, field, mounts_list):
        super().__init__(name, mounts_list)
        self._field = field

    def build(self):
        return {"name": self._name, "emptyDir": self._field}


class HttpHeaderBuilder(object):
    """Builder for k8s http header."""

    def __init__(self, name, value):
        self._name = name
        self._value = value

    def build(self):
        result = dict(name=self._name)
        result["value"] = str(self._value)
        return result


class ProbeBuilder(object):
    """Builder for k8s liveness and readiness probes."""

    def __init__(
        self,
        initial_delay=10,
        period=2,
        timeout=1,
        success_thresh=None,
        failure_thresh=None,
    ):
        self._initial_delay = initial_delay
        self._period = period
        self._timeout = timeout
        self._success_thresh = success_thresh
        self._failure_thresh = failure_thresh

    def build(self):
        return _remove_nones(
            {
                "initialDelaySeconds": self._initial_delay,
                "periodSeconds": self._period,
                "timeoutSeconds": self._timeout,
                "successThreshold": self._success_thresh,
                "failureThreshold": self._failure_thresh,
            }
        )


class ExecProbeBuilder(ProbeBuilder):
    """Builder for k8s executing probes."""

    def __init__(self, command, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._command = command

    def build(self):
        result = {"exec": {"command": self._command}}
        result.update(super().build())
        return result


class TcpProbeBuilder(ProbeBuilder):
    """Builder for k8s tcp probes."""

    def __init__(self, port, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._port = port

    def build(self):
        result = {"tcpSocket": {"port": self._port}}
        result.update(super().build())
        return result


class HttpProbeBuilder(ProbeBuilder):
    """Builder for k8s http probes."""

    def __init__(self, path, port, http_headers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path = path
        self._port = port
        self._http_headers = http_headers

    def build(self):
        result = {
            "httpGet": {
                "path": self._path,
                "port": self._port,
                "httpHeaders": [h.build() for h in self._http_headers],
            }
        }
        result.update(super().build())
        return result


class DeploymentBuilder(object):
    """Base Builder for k8s deployment."""

    def __init__(
        self,
        name,
        labels,
        replicas,
        image_pull_policy,
    ):
        self._name = name
        self._labels = labels
        self._replicas = replicas
        self._image_pull_policy = image_pull_policy

        self._containers = []
        self._volumes = []
        self._envs = dict()

        self._image_pull_secrets = []

    def set_image_pull_policy(self, policy):
        self._image_pull_policy = policy

    def add_env(self, name, value=None):
        self._envs[name] = ContainerEnvBuilder(name, value)

    def add_container(self, ctn):
        self._containers.append(ctn)

    def add_simple_envs(self, envs):
        for k, v in envs.items() or ():
            self.add_env(k, v)

    def add_volume(self, vol):
        self._volumes.append(vol)

    def add_image_pull_secret(self, name):
        self._image_pull_secrets.append(LocalObjectRefBuilder(name))

    def build_template_spec(self):
        result = {
            "containers": [ctn for ctn in self._containers],
            "volumes": [vol.build() for vol in self._volumes] or None,
            "imagePullSecrets": [ips.build() for ips in self._image_pull_secrets]
            or None,
        }
        return dict((k, v) for k, v in result.items() if v)

    def build_selector(self):
        result = {"matchLabels": self._labels}
        return result

    def build(self):
        return {
            "kind": "Deployment",
            "metadata": {
                "name": self._name,
            },
            "spec": {
                "replicas": int(self._replicas),
                "selector": self.build_selector(),
                "template": {
                    "metadata": {
                        "labels": self._labels,
                    },
                    "spec": self.build_template_spec(),
                },
            },
        }


class ReplicaSetBuilder(object):
    """Base Builder for k8s ReplicaSet."""

    def __init__(self, name, labels, replicas, image_pull_policy):
        self._name = name
        self._labels = labels
        self._replicas = replicas
        self._image_pull_policy = image_pull_policy

        self._containers = []
        self._volumes = []
        self._envs = dict()
        self._image_pull_secrets = []

    def set_image_pull_policy(self, policy):
        self._image_pull_policy = policy

    def add_env(self, name, value=None):
        self._envs[name] = ContainerEnvBuilder(name, value)

    def add_container(self, ctn):
        self._containers.append(ctn)

    def add_simple_envs(self, envs):
        for k, v in envs.items() or ():
            self.add_env(k, v)

    def add_volume(self, vol):
        self._volumes.append(vol)

    def add_image_pull_secret(self, name):
        self._image_pull_secrets.append(LocalObjectRefBuilder(name))

    def build_pod_spec(self):
        result = {
            "containers": [ctn for ctn in self._containers],
            "volumes": [vol.build() for vol in self._volumes] or None,
            "imagePullSecrets": [ips.build() for ips in self._image_pull_secrets]
            or None,
        }
        return dict((k, v) for k, v in result.items() if v)

    def build_selector(self):
        result = {"matchLabels": self._labels}
        return result

    def build(self):
        return {
            "kind": "ReplicaSet",
            "metadata": {
                "name": self._name,
            },
            "spec": {
                "replicas": int(self._replicas),
                "selector": self.build_selector(),
                "template": {
                    "metadata": {
                        "labels": self._labels,
                    },
                    "spec": self.build_pod_spec(),
                },
            },
        }


class GSEngineBuilder(ReplicaSetBuilder):
    """Builder for graphscope analytical engine."""

    def __init__(self, name, labels, num_workers, image_pull_policy):
        self._name = name
        self._labels = labels
        self._num_workers = num_workers
        self._image_pull_policy = image_pull_policy
        self._ipc_socket_file = "/tmp/vineyard_workspace/vineyard.sock"
        super().__init__(
            self._name, self._labels, self._num_workers, self._image_pull_policy
        )

    def add_vineyard_container(
        self, name, image, cpu, mem, shared_mem, etcd_endpoint, port, **kwargs
    ):
        vineyard_command = " ".join(
            [
                "vineyardd",
                "--size=%s" % str(shared_mem),
                "--etcd_endpoint=http://%s" % etcd_endpoint,
                "--socket=%s" % self._ipc_socket_file,
                "--etcd_prefix=vineyard",
            ]
        )
        commands = []
        commands.append(
            "while ! curl --output /dev/null --silent --head --connect-timeout 1 %s"
            % etcd_endpoint
        )
        commands.append("do sleep 1 && echo -n .")
        commands.append("done")
        commands.append(vineyard_command)
        cmd = ["bash", "-c", "%s" % ("; ".join(commands),)]

        resources_dict = {
            "requests": ResourceBuilder(cpu, mem).build(),
            "limits": ResourceBuilder(cpu, mem).build(),
        }

        post_start_command = kwargs.pop("post_start_command", None)
        pre_stop_command = kwargs.pop("pre_stop_command", None)
        lifecycle_dict = _remove_nones(
            {
                "postStart": {
                    "exec": {"command": post_start_command},
                }
                if post_start_command
                else None,
                "preStop": {
                    "exec": {"command": pre_stop_command},
                }
                if pre_stop_command
                else None,
            }
        )

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        super().add_container(
            _remove_nones(
                {
                    "command": cmd,
                    "env": [env.build() for env in self._envs.values()] or None,
                    "image": image,
                    "name": name,
                    "imagePullPolicy": self._image_pull_policy,
                    "resources": dict((k, v) for k, v in resources_dict.items() if v)
                    or None,
                    "ports": [PortBuilder(port).build()],
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": None,
                    "readinessProbe": None,
                    "lifecycle": lifecycle_dict or None,
                }
            )
        )

    def add_engine_container(self, name, image, cpu, mem, **kwargs):
        cmd = ["tail", "-f", "/dev/null"]

        resources_dict = {
            "requests": ResourceBuilder(cpu, mem).build(),
            "limits": ResourceBuilder(cpu, mem).build(),
        }

        post_start_command = kwargs.pop("post_start_command", None)
        pre_stop_command = kwargs.pop("pre_stop_command", None)
        lifecycle_dict = _remove_nones(
            {
                "postStart": {
                    "exec": {"command": post_start_command},
                }
                if post_start_command
                else None,
                "preStop": {
                    "exec": {"command": pre_stop_command},
                }
                if pre_stop_command
                else None,
            }
        )

        readiness_cmd = [
            "ls",
            "%s" % self._ipc_socket_file,
        ]
        readiness_probe = ExecProbeBuilder(readiness_cmd)

        ports = [i for i in range(8000, 9000)]

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        super().add_container(
            _remove_nones(
                {
                    "command": cmd,
                    "env": [env.build() for env in self._envs.values()] or None,
                    "image": image,
                    "name": name,
                    "imagePullPolicy": self._image_pull_policy,
                    "resources": dict((k, v) for k, v in resources_dict.items() if v)
                    or None,
                    "ports": [PortBuilder(port).build() for port in ports],
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": None,
                    "readinessProbe": readiness_probe.build(),
                    "lifecycle": lifecycle_dict or None,
                }
            )
        )


class GSEtcdBuilder(DeploymentBuilder):
    """Builder for graphscope etcd."""

    def __init__(self, name, labels, image_pull_policy, replicas=1):
        self._name = name
        self._labels = labels
        self._replicas = replicas
        self._image_pull_policy = image_pull_policy
        super().__init__(
            self._name, self._labels, self._replicas, self._image_pull_policy
        )

    def add_etcd_container(
        self,
        name,
        service_name,
        image,
        cpu,
        mem,
        listen_peer_service_port,
        listen_client_service_port,
        max_txn_ops=1024000,
    ):
        cmd = [
            "etcd",
            "--name",
            service_name,
            "--max-txn-ops=%s" % max_txn_ops,
            "--initial-advertise-peer-urls",
            "http://%s:%s" % (service_name, str(listen_peer_service_port)),
            "--advertise-client-urls=http://%s:%s"
            % (service_name, str(listen_client_service_port)),
            "--data-dir=/var/lib/etcd",
            "--listen-client-urls=http://0.0.0.0:%s" % str(listen_client_service_port),
            "--listen-peer-urls=http://0.0.0.0:%s" % str(listen_peer_service_port),
            "--initial-cluster",
            "%s=http://%s:%s"
            % (
                service_name,
                service_name,
                str(listen_peer_service_port),
            ),
            "--initial-cluster-state",
            "new",
        ]

        resources_dict = {
            "requests": ResourceBuilder(cpu, mem).build(),
            "limits": ResourceBuilder(cpu, mem).build(),
        }

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        super().add_container(
            _remove_nones(
                {
                    "command": cmd,
                    "env": [env.build() for env in self._envs.values()] or None,
                    "image": image,
                    "name": name,
                    "imagePullPolicy": self._image_pull_policy,
                    "resources": dict((k, v) for k, v in resources_dict.items() if v)
                    or None,
                    "ports": [
                        PortBuilder(listen_peer_service_port).build(),
                        PortBuilder(listen_client_service_port).build(),
                    ],
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": self.build_liveness_probe(
                        listen_client_service_port
                    ).build(),
                    "readinessProbe": None,
                    "lifecycle": None,
                }
            )
        )

    def build_liveness_probe(self, listen_client_service_port):
        liveness_cmd = [
            "/bin/sh",
            "-ec",
            "ETCDCTL_API=3 etcdctl --endpoints=http://[127.0.0.1]:%s get foo"
            % str(listen_client_service_port),
        ]
        return ExecProbeBuilder(liveness_cmd, timeout=15, failure_thresh=8)


class GSGraphManagerBuilder(DeploymentBuilder):
    """Builder for graphscope interactive graph manager."""

    def __init__(self, name, labels, image_pull_policy, replicas=1):
        self._name = name
        self._labels = labels
        self._replicas = replicas
        self._image_pull_policy = image_pull_policy
        super().__init__(
            self._name, self._labels, self._replicas, self._image_pull_policy
        )

    def add_manager_container(self, name, image, cpu, mem, port=8080):
        cmd = [
            "/bin/bash",
            "-c",
            "--",
        ]

        resources_dict = {
            "requests": ResourceBuilder(cpu, mem).build(),
            "limits": ResourceBuilder(cpu, mem).build(),
        }

        args = ["/home/maxgraph/manager-entrypoint.sh"]

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        pre_stop_command = ["kill", "-TERM", "`lsof -i:8080 -t`"]
        lifecycle_dict = _remove_nones(
            {
                "preStop": {
                    "exec": {"command": pre_stop_command},
                }
                if pre_stop_command
                else None,
            }
        )
        super().add_container(
            _remove_nones(
                {
                    "command": cmd,
                    "args": args,
                    "env": [env.build() for env in self._envs.values()] or None,
                    "image": image,
                    "name": name,
                    "imagePullPolicy": self._image_pull_policy,
                    "resources": dict((k, v) for k, v in resources_dict.items() if v)
                    or None,
                    "ports": [PortBuilder(port).build()],
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": None,
                    "readinessProbe": None,
                    "lifecycle": lifecycle_dict or None,
                }
            )
        )

    def add_zookeeper_container(self, name, image, cpu, mem, port=2181):
        resources_dict = {
            "requests": ResourceBuilder(cpu, mem).build(),
            "limits": ResourceBuilder(cpu, mem).build(),
        }

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        super().add_container(
            _remove_nones(
                {
                    "command": None,
                    "args": None,
                    "env": None,
                    "image": image,
                    "name": name,
                    "imagePullPolicy": self._image_pull_policy,
                    "resources": dict((k, v) for k, v in resources_dict.items() if v)
                    or None,
                    "ports": [PortBuilder(port).build()],
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": None,
                    "readinessProbe": None,
                    "lifecycle": None,
                }
            )
        )


class GSCoordinatorBuilder(DeploymentBuilder):
    """Builder for graphscope coordinator."""

    def __init__(self, name, labels, image_pull_policy, replicas=1):
        self._name = name
        self._labels = labels
        self._replicas = replicas
        self._image_pull_policy = image_pull_policy
        super().__init__(
            self._name, self._labels, self._replicas, self._image_pull_policy
        )

    def add_coordinator_container(
        self,
        name,
        port,
        num_workers,
        instance_id,
        log_level,
        namespace,
        service_type,
        gs_image,
        etcd_image,
        gie_graph_manager_image,
        zookeeper_image,
        image_pull_policy,
        image_pull_secrets,
        coordinator_name,
        coordinator_cpu,
        coordinator_mem,
        coordinator_service_name,
        etcd_cpu,
        etcd_mem,
        zookeeper_cpu,
        zookeeper_mem,
        gie_graph_manager_cpu,
        gie_graph_manager_mem,
        vineyard_cpu,
        vineyard_mem,
        vineyard_shared_mem,
        engine_cpu,
        engine_mem,
        volumes,
        timeout_seconds,
        waiting_for_delete,
        delete_namespace,
    ):
        self._port = port
        self._num_workers = num_workers
        self._instance_id = instance_id
        self._log_level = log_level
        self._namespace = namespace
        self._service_type = service_type
        self._gs_image = gs_image
        self._etcd_image = etcd_image
        self._gie_graph_manager_image = gie_graph_manager_image
        self._zookeeper_image = zookeeper_image
        self._image_pull_policy = image_pull_policy
        self._image_pull_secrets_str = image_pull_secrets
        self._coordinator_name = coordinator_name
        self._coordinator_cpu = coordinator_cpu
        self._coordinator_mem = coordinator_mem
        self._coordinator_service_name = coordinator_service_name
        self._etcd_cpu = etcd_cpu
        self._etcd_mem = etcd_mem
        self._zookeeper_cpu = zookeeper_cpu
        self._zookeeper_mem = zookeeper_mem
        self._gie_graph_manager_cpu = gie_graph_manager_cpu
        self._gie_graph_manager_mem = gie_graph_manager_mem
        self._vineyard_cpu = vineyard_cpu
        self._vineyard_mem = vineyard_mem
        self._vineyard_shared_mem = vineyard_shared_mem
        self._engine_cpu = engine_cpu
        self._engine_mem = engine_mem
        self._volumes_str = json.dumps(volumes)
        self._timeout_seconds = timeout_seconds
        self._waiting_for_delete = waiting_for_delete
        self._delete_namespace = delete_namespace

        cmd = self.build_container_command()

        resources_dict = {
            "requests": ResourceBuilder(
                self._coordinator_cpu, self._coordinator_mem
            ).build(),
            "limits": ResourceBuilder(
                self._coordinator_cpu, self._coordinator_mem
            ).build(),
        }

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        pre_stop_command = ["python3", "/usr/local/bin/pre_stop.py"]
        lifecycle_dict = _remove_nones(
            {
                "preStop": {
                    "exec": {"command": pre_stop_command},
                }
            }
        )

        super().add_container(
            _remove_nones(
                {
                    "command": cmd,
                    "env": [env.build() for env in self._envs.values()] or None,
                    "image": self._gs_image,
                    "name": name,
                    "imagePullPolicy": self._image_pull_policy,
                    "resources": dict((k, v) for k, v in resources_dict.items() if v)
                    or None,
                    "ports": [PortBuilder(self._port).build()],
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": None,
                    "readinessProbe": self.build_readiness_probe().build(),
                    "lifecycle": lifecycle_dict,
                }
            )
        )

    def build_readiness_probe(self):
        return TcpProbeBuilder(port=self._port, timeout=15, period=10, failure_thresh=8)

    def build_container_command(self):
        cmd = [
            "python3",
            "-m",
            "gscoordinator",
            "--enable_k8s",
            "true",
            "--port",
            str(self._port),
            "--num_workers",
            str(self._num_workers),
            "--instance_id",
            self._instance_id,
            "--log_level",
            self._log_level,
            "--k8s_namespace",
            self._namespace,
            "--k8s_service_type",
            self._service_type,
            "--k8s_gs_image",
            self._gs_image,
            "--k8s_etcd_image",
            self._etcd_image,
            "--k8s_gie_graph_manager_image",
            self._gie_graph_manager_image,
            "--k8s_zookeeper_image",
            self._zookeeper_image,
            "--k8s_image_pull_policy",
            self._image_pull_policy,
            "--k8s_image_pull_secrets",
            self._image_pull_secrets_str if self._image_pull_secrets_str else '""',
            "--k8s_coordinator_name",
            self._coordinator_name,
            "--k8s_coordinator_service_name",
            self._coordinator_service_name,
            "--k8s_etcd_cpu",
            str(self._etcd_cpu),
            "--k8s_etcd_mem",
            self._etcd_mem,
            "--k8s_zookeeper_cpu",
            str(self._zookeeper_cpu),
            "--k8s_etcd_mem",
            self._zookeeper_mem,
            "--k8s_gie_graph_manager_cpu",
            str(self._gie_graph_manager_cpu),
            "--k8s_gie_graph_manager_mem",
            self._gie_graph_manager_mem,
            "--k8s_vineyard_cpu",
            str(self._vineyard_cpu),
            "--k8s_vineyard_mem",
            self._vineyard_mem,
            "--k8s_vineyard_shared_mem",
            self._vineyard_shared_mem,
            "--k8s_engine_cpu",
            str(self._engine_cpu),
            "--k8s_engine_mem",
            self._engine_mem,
            "--k8s_volumes",
            self._volumes_str,
            "--timeout_seconds",
            str(self._timeout_seconds),
            "--waiting_for_delete",
            str(self._waiting_for_delete),
            "--k8s_delete_namespace",
            str(self._delete_namespace),
        ]
        return cmd
