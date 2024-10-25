#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited.
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

import datetime
import logging
import queue
import socket

import psutil
from dateutil import tz
from kubernetes import client as kube_client
from kubernetes import watch as kube_watch

from gscoordinator.flex.core.config import BATCHSIZE
from gscoordinator.flex.core.config import CLUSTER_TYPE
from gscoordinator.flex.core.config import CREATION_TIME
from gscoordinator.flex.core.config import ENGINE_TYPE
from gscoordinator.flex.core.config import FRONTEND_TYPE
from gscoordinator.flex.core.config import INSTANCE_NAME
from gscoordinator.flex.core.config import MAXSIZE
from gscoordinator.flex.core.config import NAMESPACE
from gscoordinator.flex.core.config import STORAGE_TYPE
from gscoordinator.flex.core.scheduler import schedule
from gscoordinator.flex.core.stoppable_thread import StoppableThread
from gscoordinator.flex.core.utils import encode_datetime
from gscoordinator.flex.core.utils import resolve_api_client
from gscoordinator.version import __version__

logger = logging.getLogger("graphscope")


class KubePodWatcher(object):
    """Class for watching logs of kubernetes pod"""

    def __init__(self, pod):
        self._api_client = resolve_api_client()
        self._core_api = kube_client.CoreV1Api(self._api_client)
        self._watch = kube_watch.Watch()

        self._pod = pod
        self._pod_name = pod.metadata.name
        # multiple containers
        self._containers = []
        # logs
        self._lines = {}
        self._streaming_log_threads = []
        # fetching
        for container in pod.status.container_statuses:
            name = container.name
            self._containers.append(name)
            self._lines[name] = queue.Queue(maxsize=MAXSIZE)

    def _streaming_log_impl(self, container_name: str):
        lines = self._lines[container_name]
        while True:
            try:
                for message in self._watch.stream(
                    self._core_api.read_namespaced_pod_log,
                    name=self._pod_name,
                    namespace=NAMESPACE,
                    container=container_name,
                    since_seconds=1,
                ):
                    try:
                        lines.put_nowait(message)
                    except queue.Full:  # noqa: E722, B110
                        pass
            except:  # noqa: E722, B110
                pass

    def start(self):
        for container_name in self._containers:
            t = StoppableThread(target=self._streaming_log_impl, args=(container_name,))
            t.daemon = True
            t.start()
            self._streaming_log_threads.append(t)

    def fetch_log(self, from_cache: bool):
        containers_log = {}
        if from_cache:
            # fetch the logs that not consumed from cache queue.
            for container_name in self._containers:
                logs = []
                q = self._lines[container_name]
                for i in range(BATCHSIZE):
                    try:
                        line = q.get(block=False, timeout=None)
                    except queue.Empty:  # noqa: E722, B110
                        break
                    logs.append(line)
                containers_log[container_name] = "\n".join(logs)
        else:
            # fetch the latest logs since 1 hours
            for container_name in self._containers:
                logs = self._core_api.read_namespaced_pod_log(
                    name=self._pod_name,
                    namespace=NAMESPACE,
                    container=container_name,
                    since_seconds=3600,
                )
                containers_log[container_name] = logs
            # clear the cache queue, since the logs were already consumed.
            self._lines[container_name].queue.clear()
        return containers_log


class KubeWatcher(object):
    def __init__(self, components: dict):
        # { "coordinator": { "pod1": KubePodWatcher } }
        self._pods_watcher = {}

        self._api_client = resolve_api_client()
        self._app_api = kube_client.AppsV1Api(self._api_client)
        self._core_api = kube_client.CoreV1Api(self._api_client)
        for k, v in components.items():
            self._pods_watcher[k] = {}
            # only statefulset supported
            if v["kind"] != "StatefulSet":
                continue
            response = self._app_api.read_namespaced_stateful_set(
                namespace=NAMESPACE, name=v["name"]
            )
            match_labels = response.spec.selector.match_labels
            selector = ",".join([f"{k}={v}" for k, v in match_labels.items()])
            pods = self._core_api.list_namespaced_pod(
                namespace=NAMESPACE, label_selector=selector
            )
            for pod in pods.items:
                pod_name = pod.metadata.name
                self._pods_watcher[k][pod_name] = KubePodWatcher(pod)

    def start(self):
        for _, pods in self._pods_watcher.items():
            for _, watcher in pods.items():
                watcher.start()

    def fetch_log(self, component: str, pod_name: str, from_cache: bool):
        pod_watcher = self._pods_watcher[component][pod_name]
        return pod_watcher.fetch_log(from_cache)


class Deployment(object):
    """Base class to derive KubeDeployment and HostDeployment"""

    def __init__(self):
        pass

    def get_deployment_info(self) -> dict:
        return {
            "instance_name": INSTANCE_NAME,
            "cluster_type": CLUSTER_TYPE,
            "version": __version__,
            "frontend": FRONTEND_TYPE,
            "engine": ENGINE_TYPE,
            "storage": STORAGE_TYPE,
            "creation_time": encode_datetime(CREATION_TIME),
        }

    def get_deployment_status(self) -> dict:
        raise NotImplementedError("Method is not supported.")

    def get_resource_usage(self) -> dict:
        raise NotImplementedError("Method is not supported.")

    def fetch_pod_log(self, component: str, pod_name: str, from_cache: bool) -> dict:
        raise NotImplementedError("Method is not supported.")


class HostDeployment(Deployment):
    def __init__(self):
        super().__init__()

    def get_deployment_status(self) -> dict:
        status = {"cluster_type": CLUSTER_TYPE, "nodes": []}
        disk_info = psutil.disk_usage("/")
        status["nodes"].append(
            {
                "name": socket.gethostname(),
                "cpu_usage": psutil.cpu_percent(),
                "memory_usage": psutil.virtual_memory().percent,
                "disk_usage": float(f"{disk_info.used / disk_info.total * 100:.2f}"),
            }
        )
        return status


class KubeDeployment(Deployment):
    """Class for managing instance status deployed in k8s cluster"""

    def __init__(self, components: dict):
        super().__init__()
        self._api_client = resolve_api_client()
        self._core_api = kube_client.CoreV1Api(self._api_client)
        self._custom_objects_api = kube_client.CustomObjectsApi()
        # { "coordinator" : { "kind": "StatefulSet", "name": "statefulset name" } }
        self._components = components
        # kube watcher
        self._kube_watcher = KubeWatcher(components)
        self._kube_watcher.start()
        # monitor resources every 60s and record the usage for half a day
        self._resource_usage = self.initialize_resource_usage()
        self._fetch_resource_usage_job = {
            schedule.every(60)
            .seconds.do(self._fetch_resource_usage_impl)
            .tag("fetch", "resource usage")
        }
        # deployment status
        self._status = {"cluster_type": CLUSTER_TYPE, "pods": {}}
        self._fetch_status_job = (
            schedule.every(30)
            .seconds.do(self._fetch_status_impl)
            .tag("fetch", "deployment status")
        )

    def _get_pod_component(self, pod):
        """Get component that the pod belongs to"""
        if pod.metadata.owner_references is not None:
            owner = pod.metadata.owner_references[0]
            for component, v in self._components.items():
                if owner.kind == v["kind"] and owner.name == v["name"]:
                    return component
        return None

    def _parse_kube_cpu_value(self, value: str):
        """2002304973n -> 2002m"""
        if value.endswith("n"):
            value = int(value[0:-1]) / 1000000
        elif value.endswith("u"):
            value = int(value[0:-1]) / 1000
        elif value.endswith("m"):
            value = int(value[0:-1])
        return float(value)

    def _parse_kube_memory_value(self, value: str):
        """ki -> Mi"""
        if value.endswith("Mi"):
            value = int(value[0:-2])
        elif value.endswith("Ki"):
            value = int(value[0:-2]) / 1000
        elif value.endswith("Gi"):
            value = int(value[0:-2]) * 1000
        elif value.endswith("Ti"):
            value = int(value[0:-2]) * 1000000
        return float(value)

    def _fetch_resource_usage_impl(self):
        try:
            t = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            pods = self._custom_objects_api.list_namespaced_custom_object(
                "metrics.k8s.io", "v1beta1", NAMESPACE, "pods"
            )
            for stats in pods["items"]:
                name = stats["metadata"]["name"]
                pod = self._core_api.read_namespaced_pod(name, NAMESPACE)
                component = self._get_pod_component(pod)
                if component is None:
                    continue
                # accumulate the cpu and memory value from each container
                cpu = 0
                memory = 0
                for container in stats["containers"]:
                    cpu += self._parse_kube_cpu_value(container["usage"]["cpu"])
                    memory += self._parse_kube_memory_value(
                        container["usage"]["memory"]
                    )
                if self._resource_usage["cpu_usage"].full():
                    self._resource_usage["cpu_usage"].get()
                    self._resource_usage["memory_usage"].get()
                self._resource_usage["cpu_usage"].put(
                    {"host": name, "timestamp": t, "usage": round(cpu)}
                )
                self._resource_usage["memory_usage"].put(
                    {"host": name, "timestamp": t, "usage": round(memory)}
                )
        except Exception as e:
            logger.warn("Failed to fetch resource usage %s", str(e))

    def _fetch_status_impl(self):
        try:
            t = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            pods = self._custom_objects_api.list_namespaced_custom_object(
                "metrics.k8s.io", "v1beta1", NAMESPACE, "pods"
            )
            status = {}
            for stats in pods["items"]:
                name = stats["metadata"]["name"]
                pod = self._core_api.read_namespaced_pod(name, NAMESPACE)
                component = self._get_pod_component(pod)
                if component is None:
                    continue
                # pod cpu usage
                cpu = 0
                # pod memory usage
                memory = 0
                # accumulate the cpu and memory value from each container
                for container in stats["containers"]:
                    cpu += self._parse_kube_cpu_value(container["usage"]["cpu"])
                    memory += self._parse_kube_memory_value(
                        container["usage"]["memory"]
                    )
                pod = self._core_api.read_namespaced_pod(name, NAMESPACE)
                # pod labels
                labels = pod.metadata.labels
                # node
                node = pod.spec.node_name
                # pod status
                phase = pod.status.phase
                # pod images of each container
                image = []
                # set to the maximum value of containers
                restart_count = 0
                # pod creation time
                creation_time = pod.status.start_time.astimezone(tz.tzlocal()).strftime(
                    "%Y/%m/%d %H:%M:%S"
                )
                for container in pod.status.container_statuses:
                    image.append(container.image)
                    restart_count = max(restart_count, container.restart_count)
                if component not in status:
                    status[component] = []
                status[component].append(
                    {
                        "name": name,
                        "image": image,
                        "labels": labels,
                        "node": node,
                        "status": phase,
                        "restart_count": restart_count,
                        "cpu_usage": round(cpu),
                        "memory_usage": round(memory),
                        "timestamp": t,
                        "creation_time": creation_time,
                        "component_belong_to": component,
                    }
                )
            # sort
            sorted_status = {}
            for key in sorted(status):
                status[key].sort(key=lambda x: x["name"])
                sorted_status[key] = status[key]
            self._status["pods"] = sorted_status
        except Exception as e:
            logger.warn("Failed to fetch deployment status %s", str(e))

    def initialize_resource_usage(self):
        try:
            pod_num = 0
            pods = self._custom_objects_api.list_namespaced_custom_object(
                "metrics.k8s.io", "v1beta1", NAMESPACE, "pods"
            )
            for stats in pods["items"]:
                name = stats["metadata"]["name"]
                pod = self._core_api.read_namespaced_pod(name, NAMESPACE)
                component = self._get_pod_component(pod)
                if component is None:
                    continue
                pod_num += 1
            # resources usage
            return {
                "cpu_usage": queue.Queue(maxsize=720 * pod_num),
                "memory_usage": queue.Queue(maxsize=720 * pod_num),
            }
        except Exception as e:
            logger.warn("Failed to fetch resource usage %s", str(e))
            return {"cpu_usage": [], "memory_usage": []}

    def get_resource_usage(self) -> dict:
        rlt = {}
        for k, q in self._resource_usage.items():
            rlt[k] = list(q.queue)
        return rlt

    def get_deployment_status(self) -> dict:
        return self._status

    def fetch_pod_log(self, component: str, pod_name: str, from_cache: bool) -> dict:
        return self._kube_watcher.fetch_log(component, pod_name, from_cache)


def get_kube_deployment():
    api_client = resolve_api_client()
    app_api = kube_client.AppsV1Api(api_client)
    # component
    components = {}
    response = app_api.list_namespaced_stateful_set(NAMESPACE)
    for statefulset in response.items:
        name = statefulset.metadata.name
        labels = statefulset.metadata.labels
        if (
            "app.kubernetes.io/instance" not in labels
            or labels["app.kubernetes.io/instance"] != INSTANCE_NAME
        ):
            continue
        component = labels.get("app.kubernetes.io/component", None)
        if component is not None:
            components[component] = {"kind": "StatefulSet", "name": name}
    if not components:
        raise RuntimeError(
            f"Failed to find instance {INSTANCE_NAME} in kubernetes cluster"
        )
    return KubeDeployment(components)


def initialize_deployemnt():
    if CLUSTER_TYPE == "HOSTS":
        return HostDeployment()
    elif CLUSTER_TYPE == "KUBERNETES":
        return get_kube_deployment()
    else:
        raise RuntimeError(
            f"Failed to initialize deployemnt with cluster type {CLUSTER_TYPE}"
        )
