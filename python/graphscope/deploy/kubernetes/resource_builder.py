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
import math
import os
import sys

from graphscope.deploy.kubernetes.utils import parse_readable_memory
from graphscope.framework.utils import get_tempdir

logger = logging.getLogger("graphscope")


def _remove_nones(o):
    return dict((k, v) for k, v in o.items() if v is not None)


def resolve_volume_builder(name, value):
    """Resolve specified volume with value dict."""
    if "type" not in value or "field" not in value or "mounts" not in value:
        logger.warning("Volume %s must contains 'type' 'field' and 'mounts'", name)
        return None
    return VolumeBuilder(
        name=name,
        volume_type=value["type"],
        field=value["field"],
        mounts_list=value["mounts"],
    )


class ConfigMapBuilder(object):
    """Builder for k8s ConfigMap"""

    def __init__(self, name):
        self._name = name
        self._kvs = dict()

    def add_kv(self, key, value):
        if value:
            self._kvs[key] = value

    def add_simple_kvs(self, kvs):
        for k, v in kvs.items() or ():
            self.add_kv(k, v)

    def build(self):
        return {
            "kind": "ConfigMap",
            "metadata": {"name": self._name},
            "data": self._kvs,
        }


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
                "labels": self._selector,
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


class ContainerFieldRefEnvBuilder(object):
    """Builder for k8s container environments."""

    def __init__(self, name, field):
        self._name = name
        self._field = field

    def build(self):
        result = dict(name=self._name)
        result["valueFrom"] = {
            "fieldRef": {
                "fieldPath": self._field,
            }
        }
        return result


BASE_MACHINE_ENVS = {
    "MY_NODE_NAME": "spec.nodeName",
    "MY_POD_NAME": "metadata.name",
    "MY_POD_NAMESPACE": "metadata.namespace",
    "MY_POD_IP": "status.podIP",
    "MY_HOST_NAME": "status.podIP",
}


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

    def __init__(self, name, volume_type, field, mounts_list):
        self._name = name
        self._type = volume_type
        self._field = field
        self._mounts_list = mounts_list
        if not isinstance(self._mounts_list, list):
            self._mounts_list = [self._mounts_list]

        for mount in self._mounts_list:
            mount["name"] = self._name

    def build(self):
        return {"name": self._name, self._type: self._field}

    def build_mount(self):
        return self._mounts_list


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
        self._host_network = False
        self._node_selector = dict()

        self.add_field_envs(BASE_MACHINE_ENVS)

    @property
    def host_network(self):
        return self._host_network

    @host_network.setter
    def host_network(self, value):
        self._host_network = value

    def set_image_pull_policy(self, policy):
        self._image_pull_policy = policy

    def add_env(self, name, value=None):
        if value:
            self._envs[name] = ContainerEnvBuilder(name, value)

    def add_field_env(self, name, field=None):
        if field:
            self._envs[name] = ContainerFieldRefEnvBuilder(name, field)

    def add_simple_envs(self, envs):
        for k, v in envs.items() or ():
            self.add_env(k, v)

    def add_field_envs(self, envs):
        for k, v in envs.items() or ():
            self.add_field_env(k, v)

    def add_container(self, ctn):
        self._containers.append(ctn)

    def add_volume(self, vol):
        self._volumes.append(vol)

    def add_image_pull_secret(self, name):
        self._image_pull_secrets.append(LocalObjectRefBuilder(name))

    def add_pod_node_selector(self, node_selector):
        if node_selector:
            for k, v in node_selector.items():
                self._node_selector[k] = v

    def build_template_spec(self):
        result = {
            "hostNetwork": self._host_network,
            "containers": [ctn for ctn in self._containers],
            "volumes": [vol.build() for vol in self._volumes] or None,
            "imagePullSecrets": [ips.build() for ips in self._image_pull_secrets]
            or None,
            "nodeSelector": self._node_selector or None,
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
        self._annotations = dict()
        self._image_pull_secrets = []
        self._host_network = False
        self._node_selector = dict()

        self.add_field_envs(BASE_MACHINE_ENVS)

    @property
    def host_network(self):
        return self._host_network

    @host_network.setter
    def host_network(self, value):
        self._host_network = value

    def set_image_pull_policy(self, policy):
        self._image_pull_policy = policy

    def add_annotation(self, name, value):
        self._annotations[name] = value

    def add_env(self, name, value=None):
        if value:
            self._envs[name] = ContainerEnvBuilder(name, value)

    def add_field_env(self, name, field=None):
        if field:
            self._envs[name] = ContainerFieldRefEnvBuilder(name, field)

    def add_simple_envs(self, envs):
        for k, v in envs.items() or ():
            self.add_env(k, v)

    def add_field_envs(self, envs):
        for k, v in envs.items() or ():
            self.add_field_env(k, v)

    def add_container(self, ctn):
        self._containers.append(ctn)

    def add_volume(self, vol):
        self._volumes.append(vol)

    def add_image_pull_secret(self, name):
        self._image_pull_secrets.append(LocalObjectRefBuilder(name))

    def add_pod_node_selector(self, node_selector):
        if node_selector:
            for k, v in node_selector.items():
                self._node_selector[k] = v

    def build_pod_spec(self):
        result = {
            "hostNetwork": self._host_network,
            "containers": [ctn for ctn in self._containers],
            "volumes": [vol.build() for vol in self._volumes] or None,
            "imagePullSecrets": [ips.build() for ips in self._image_pull_secrets]
            or None,
            "nodeSelector": self._node_selector or None,
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
                        "annotations": self._annotations,
                    },
                    "spec": self.build_pod_spec(),
                },
            },
        }


class GSEngineBuilder(ReplicaSetBuilder):
    """Builder for graphscope analytical engine."""

    _vineyard_requests_cpu = 0.2
    _vineyard_requests_mem = "128Mi"

    _engine_requests_cpu = 0.2
    _engine_requests_mem = "1Gi"

    _mars_worker_requests_cpu = 0.2
    _mars_worker_requests_mem = "1Gi"
    _mars_scheduler_requests_cpu = 0.2
    _mars_scheduler_requests_mem = "1Gi"

    def __init__(self, name, labels, num_workers, image_pull_policy):
        self._name = name
        self._labels = labels
        self._num_workers = num_workers
        self._image_pull_policy = image_pull_policy
        self._ipc_socket_file = os.path.join(
            get_tempdir(), "vineyard_workspace", "vineyard.sock"
        )
        super().__init__(
            self._name, self._labels, self._num_workers, self._image_pull_policy
        )

    def add_vineyard_container(
        self,
        name,
        image,
        cpu,
        mem,
        shared_mem,
        preemptive,
        etcd_endpoints,
        port,
        **kwargs
    ):
        vineyard_command = " ".join(
            [
                sys.executable,
                "-m",
                "vineyard",
                "--size=%s" % str(shared_mem),
                '--etcd_endpoint="%s"' % (";".join(etcd_endpoints),),
                "--socket=%s" % self._ipc_socket_file,
                "--etcd_prefix=vineyard",
            ]
        )
        commands = []
        commands.append(
            "while ! curl --output /dev/null --silent --head --connect-timeout 1 %s"
            % etcd_endpoints[0]
        )
        commands.append("do sleep 1 && echo -n .")
        commands.append("done")
        commands.append(vineyard_command)
        cmd = ["bash", "-c", "%s" % ("; ".join(commands),)]

        resources_dict = {
            "requests": ResourceBuilder(
                self._vineyard_requests_cpu, self._vineyard_requests_mem
            ).build()
            if preemptive
            else ResourceBuilder(cpu, mem).build(),
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

    def add_engine_container(self, name, image, cpu, mem, preemptive, **kwargs):
        cmd = kwargs.pop("cmd", None)
        args = kwargs.pop("args", None)

        resources_dict = {
            "requests": ResourceBuilder(
                self._engine_requests_cpu, self._engine_requests_mem
            ).build()
            if preemptive
            else ResourceBuilder(cpu, mem).build(),
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
            "/bin/bash",
            "-c",
            "ls %s 2>/dev/null" % self._ipc_socket_file,
        ]
        readiness_probe = ExecProbeBuilder(readiness_cmd)

        # ports range in 8000~9000 will be open if `ports ` param missing.
        ports = kwargs.pop("ports", [i for i in range(8000, 9000)])
        if not isinstance(ports, list):
            ports = [ports]

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

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
                    "ports": [PortBuilder(port).build() for port in ports],
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": None,
                    "readinessProbe": readiness_probe.build(),
                    "lifecycle": lifecycle_dict or None,
                }
            )
        )
        super().add_annotation("kubectl.kubernetes.io/default-container", name)

    def add_mars_worker_container(
        self, name, image, cpu, mem, preemptive, port, scheduler_endpoint
    ):
        # compute n cpu, to avoid mars worker launches too many actors
        if isinstance(cpu, str) and cpu[-1] == "m":
            n_cpu = math.ceil(int("200m"[:-1]) / 1000)
        if isinstance(cpu, (int, float)):
            n_cpu = math.ceil(cpu)
        else:
            # by default: 1
            n_cpu = 1

        cmd = [
            "while ! ls $VINEYARD_IPC_SOCKET 2>/dev/null; do sleep 1 && echo -n .; done",
            ";",
            'echo \'"@inherits": "@mars/deploy/oscar/base_config.yml"\' > /tmp/mars-on-vineyard.yml',
            ";",
            'echo "storage:" >> /tmp/mars-on-vineyard.yml',
            ";",
            'echo "  backends: [vineyard]" >> /tmp/mars-on-vineyard.yml',
            ";",
            'echo "  vineyard:" >> /tmp/mars-on-vineyard.yml',
            ";",
            'echo "    vineyard_socket: $VINEYARD_IPC_SOCKET" >> /tmp/mars-on-vineyard.yml',
            ";",
            "cat /tmp/mars-on-vineyard.yml",
            ";",
            "python3",
            "-m",
            "mars.deploy.oscar.worker",
            "--n-cpu=%d" % n_cpu,
            "--endpoint=$MY_POD_IP:%s" % port,
            "--supervisors=%s" % scheduler_endpoint,
            "--log-level=DEBUG",
            "--config-file=/tmp/mars-on-vineyard.yml",
        ]
        cmd = ["bash", "-c", " ".join(cmd)]

        resources_dict = {
            "requests": ResourceBuilder(
                self._mars_worker_requests_cpu, self._mars_worker_requests_mem
            ).build()
            if preemptive
            else ResourceBuilder(cpu, mem).build(),
            "limits": ResourceBuilder(cpu, mem).build(),
        }

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        probe = TcpProbeBuilder(port=port, timeout=15, period=10, failure_thresh=8)

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
                    "readinessProbe": probe.build(),
                }
            )
        )

    def add_mars_scheduler_container(
        self, name, image, cpu, mem, preemptive, port, web_port
    ):
        cmd = [
            "while ! ls $VINEYARD_IPC_SOCKET 2>/dev/null; do sleep 1 && echo -n .; done",
            ";",
            'echo \'"@inherits": "@mars/deploy/oscar/base_config.yml"\' > /tmp/mars-on-vineyard.yml',
            ";",
            'echo "storage:" >> /tmp/mars-on-vineyard.yml',
            ";",
            'echo "  backends: [vineyard]" >> /tmp/mars-on-vineyard.yml',
            ";",
            'echo "  vineyard:" >> /tmp/mars-on-vineyard.yml',
            ";",
            'echo "    vineyard_socket: $VINEYARD_IPC_SOCKET" >> /tmp/mars-on-vineyard.yml',
            ";",
            "cat /tmp/mars-on-vineyard.yml",
            ";",
            "python3",
            "-m",
            "mars.deploy.oscar.supervisor",
            "--endpoint=$MY_POD_IP:%s" % port,
            "--web-port=%s" % web_port,
            "--log-level=DEBUG",
            "--config-file=/tmp/mars-on-vineyard.yml",
        ]
        cmd = ["bash", "-c", " ".join(cmd)]

        resources_dict = {
            "requests": ResourceBuilder(
                self._mars_scheduler_requests_cpu, self._mars_scheduler_requests_mem
            ).build()
            if preemptive
            else ResourceBuilder(cpu, mem).build(),
            "limits": ResourceBuilder(cpu, mem).build(),
        }

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        probe = TcpProbeBuilder(port=port, timeout=15, period=10, failure_thresh=8)

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
                    "ports": [PortBuilder(port).build(), PortBuilder(web_port).build()],
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": None,
                    "readinessProbe": probe.build(),
                }
            )
        )

        super().add_annotation("kubectl.kubernetes.io/default-container", name)

    def add_engine_pod_node_selector(self, node_selector):
        if node_selector:
            super().add_pod_node_selector(node_selector)


class PodBuilder(object):
    """Base builder for k8s pod."""

    def __init__(
        self,
        name,
        labels,
        hostname=None,
        subdomain=None,
        restart_policy="Never",
        node_selector=None,
    ):
        self._name = name
        self._labels = labels
        self._hostname = hostname
        self._subdomain = subdomain
        self._restart_policy = restart_policy

        self._containers = []
        self._image_pull_secrets = []
        self._volumes = []
        if node_selector:
            self._node_selector = node_selector
        else:
            self._node_selector = dict()

    def add_volume(self, vol):
        if isinstance(vol, list):
            self._volumes.extend(vol)
        else:
            self._volumes.append(vol)

    def add_container(self, ctn):
        self._containers.append(ctn)

    def add_image_pull_secret(self, name):
        self._image_pull_secrets.append(LocalObjectRefBuilder(name))

    def build_pod_spec(self):
        return _remove_nones(
            {
                "hostname": self._hostname,
                "subdomain": self._subdomain,
                "containers": [ctn for ctn in self._containers],
                "volumes": [vol.build() for vol in self._volumes] or None,
                "imagePullSecrets": [ips.build() for ips in self._image_pull_secrets]
                or None,
                "restartPolicy": self._restart_policy,
                "nodeSelector": self._node_selector or None,
            }
        )

    def build(self):
        return {
            "kind": "Pod",
            "metadata": {"name": self._name, "labels": self._labels},
            "spec": self.build_pod_spec(),
        }


class GSEtcdBuilder(object):
    """Builder for graphscope etcd."""

    _requests_cpu = 0.2
    _requests_mem = "128Mi"

    def __init__(
        self,
        name_prefix,
        container_name,
        service_name,
        image,
        cpu,
        mem,
        preemptive,
        listen_peer_service_port,
        listen_client_service_port,
        labels,
        image_pull_policy,
        num_pods=3,
        restart_policy="Always",
        image_pull_secrets=None,
        max_txn_ops=1024000,
    ):
        self._name_prefix = name_prefix
        self._container_name = container_name
        self._service_name = service_name
        self._image = image
        self._cpu = cpu
        self._mem = mem
        self._preemptive = preemptive
        self._listen_peer_service_port = listen_peer_service_port
        self._listen_client_service_port = listen_client_service_port
        self._labels = labels
        self._image_pull_policy = image_pull_policy
        self._num_pods = num_pods
        self._restart_policy = restart_policy
        self._image_pull_secrets = image_pull_secrets
        self._max_txn_ops = 1024000
        self._node_selector = dict()

        self._envs = dict()
        self._volumes = []

    def add_volume(self, vol):
        if isinstance(vol, list):
            self._volumes.extend(vol)
        else:
            self._volumes.append(vol)

    def add_env(self, name, value=None):
        self._envs[name] = ContainerEnvBuilder(name, value)

    def add_simple_envs(self, envs):
        for k, v in envs.items() or ():
            self.add_env(k, v)

    def build(self):
        """
        Returns: a list of :class:`PodBuilder`.
        """
        pods_name = []
        initial_cluster = ""
        for i in range(self._num_pods):
            name = "%s-%s" % (self._name_prefix, str(i))
            pods_name.append(name)
            initial_cluster += "%s=http://%s:%s," % (
                name,
                name,
                self._listen_peer_service_port,
            )
        # drop last comma
        initial_cluster = initial_cluster[0:-1]

        pods_builders, svc_builders = [], []
        for _, name in enumerate(pods_name):
            pod_labels = {"etcd_name": name}
            pod_builder = PodBuilder(
                name=name,
                labels={**self._labels, **pod_labels},
                hostname=name,
                subdomain=self._service_name,
                restart_policy=self._restart_policy,
                node_selector=self._node_selector,
            )

            # volumes
            pod_builder.add_volume(self._volumes)

            cmd = [
                "etcd",
                "--name",
                name,
                "--max-txn-ops=%s" % self._max_txn_ops,
                "--initial-advertise-peer-urls",
                "http://%s:%s" % (name, self._listen_peer_service_port),
                "--advertise-client-urls",
                "http://%s:%s" % (name, self._listen_client_service_port),
                "--data-dir=/var/lib/etcd",
                "--listen-client-urls=http://0.0.0.0:%s"
                % self._listen_client_service_port,
                "--listen-peer-urls=http://0.0.0.0:%s" % self._listen_peer_service_port,
                "--initial-cluster",
                initial_cluster,
                "--initial-cluster-state",
                "new",
            ]

            resources_dict = {
                "requests": ResourceBuilder(
                    self._requests_cpu, self._requests_mem
                ).build()
                if self._preemptive
                else ResourceBuilder(self._cpu, self._mem).build(),
                "limits": ResourceBuilder(self._cpu, self._mem).build(),
            }

            volumeMounts = []
            for vol in self._volumes:
                for vol_mount in vol.build_mount():
                    volumeMounts.append(vol_mount)

            pod_builder.add_container(
                _remove_nones(
                    {
                        "command": cmd,
                        "env": [env.build() for env in self._envs.values()] or None,
                        "image": self._image,
                        "name": self._container_name,
                        "imagePullPolicy": self._image_pull_policy,
                        "resources": dict(
                            (k, v) for k, v in resources_dict.items() if v
                        )
                        or None,
                        "ports": [
                            PortBuilder(self._listen_peer_service_port).build(),
                            PortBuilder(self._listen_client_service_port).build(),
                        ],
                        "volumeMounts": volumeMounts or None,
                        "livenessProbe": self.build_liveness_probe().build(),
                        "readinessProbe": self.build_readiness_probe().build(),
                        "lifecycle": None,
                    }
                )
            )
            pods_builders.append(pod_builder)

            service_builder = ServiceBuilder(
                name,
                service_type="ClusterIP",
                port=[
                    self._listen_peer_service_port,
                    self._listen_client_service_port,
                ],
                selector=pod_labels,
            )
            svc_builders.append(service_builder)

        return pods_builders, svc_builders

    def build_liveness_probe(self):
        liveness_cmd = [
            "/bin/sh",
            "-ec",
            "ETCDCTL_API=3 etcdctl --endpoints=http://[127.0.0.1]:%s get foo"
            % str(self._listen_client_service_port),
        ]
        return ExecProbeBuilder(liveness_cmd, timeout=15, period=10, failure_thresh=8)

    def build_readiness_probe(self):
        return TcpProbeBuilder(
            self._listen_peer_service_port, timeout=15, period=10, failure_thresh=8
        )

    def add_etcd_pod_node_selector(self, node_selector):
        if node_selector:
            for k, v in node_selector.items():
                self._node_selector[k] = v


class GSCoordinatorBuilder(DeploymentBuilder):
    """Builder for graphscope coordinator."""

    _requests_cpu = 0.5
    _requests_mem = "512Mi"

    def __init__(self, name, labels, image_pull_policy, replicas=1):
        self._name = name
        self._labels = labels
        self._replicas = replicas
        self._image_pull_policy = image_pull_policy
        super().__init__(
            self._name, self._labels, self._replicas, self._image_pull_policy
        )

    def add_coordinator_container(self, name, image, cpu, mem, preemptive, **kwargs):
        cmd = kwargs.pop("cmd", None)
        args = kwargs.pop("args", None)
        module_name = kwargs.pop("module_name", "gscoordinator")

        resources_dict = {
            "requests": ResourceBuilder(self._requests_cpu, self._requests_mem).build()
            if preemptive
            else ResourceBuilder(cpu, mem).build(),
            "limits": ResourceBuilder(cpu, mem).build(),
        }

        volumeMounts = []
        for vol in self._volumes:
            for vol_mount in vol.build_mount():
                volumeMounts.append(vol_mount)

        pre_stop_command = [
            sys.executable,
            "-m",
            "{0}.hook.prestop".format(module_name),
        ]
        lifecycle_dict = _remove_nones(
            {
                "preStop": {
                    "exec": {"command": pre_stop_command},
                }
            }
        )

        ports = kwargs.pop("ports", None)
        if ports is not None and not isinstance(ports, list):
            ports = [ports]

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
                    "ports": [PortBuilder(port).build() for port in ports]
                    if ports
                    else None,
                    "volumeMounts": volumeMounts or None,
                    "livenessProbe": None,
                    "readinessProbe": self.build_readiness_probe(ports[0]).build(),
                    "lifecycle": lifecycle_dict,
                }
            )
        )

    def add_coordinator_pod_node_selector(self, node_selector):
        if node_selector:
            super().add_pod_node_selector(node_selector)

    def build_readiness_probe(self, port):
        return TcpProbeBuilder(port=port, timeout=15, period=10, failure_thresh=8)
