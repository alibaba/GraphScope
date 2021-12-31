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
import os
import re
import threading
import time
from queue import Queue

from kubernetes import client as kube_client
from kubernetes import config as kube_config
from kubernetes.client.rest import ApiException as K8SApiException

from graphscope.framework.errors import K8sError

logger = logging.getLogger("graphscope")


def try_to_resolve_api_client():
    """The order of resolves are as following.

    1. load from incluster configuration or,
    2. load from kubernetes config file or,
    3. set api address from env if `KUBE_API_ADDRESS` exist.
    4. RuntimeError will be raised if resolve failed.
    """
    try:
        # load from incluster configuration
        kube_config.load_incluster_config()
    except:  # noqa: E722
        try:
            # load from kubernetes config file
            kube_config.load_kube_config()
        except:  # noqa: E722
            if "KUBE_API_ADDRESS" in os.environ:
                # try to load from env `KUBE_API_ADDRESS`
                config = kube_client.Configuration()
                config.host = os.environ["KUBE_API_ADDRESS"]
                return kube_client.ApiClient(config)
            raise RuntimeError("Resolve kube api client failed.")
    return kube_client.ApiClient()


def parse_readable_memory(value):
    value = str(value).strip()
    num = value[:-2]
    suffix = value[-2:]
    try:
        float(num)
    except ValueError as e:
        raise ValueError(
            "Argument cannot be interpreted as a number: %s" % value
        ) from e
    if suffix not in ["Ki", "Mi", "Gi"]:
        raise ValueError("Memory suffix must be one of 'Ki', 'Mi' and 'Gi': %s" % value)
    return value


def try_to_read_namespace_from_context():
    try:
        contexts, active_context = kube_config.list_kube_config_contexts()
        if contexts and "namespace" in active_context["context"]:
            return active_context["context"]["namespace"]
    except:  # noqa: E722
        pass
    return None


def wait_for_deployment_complete(api_client, namespace, name, timeout_seconds=60):
    core_api = kube_client.CoreV1Api(api_client)
    app_api = kube_client.AppsV1Api(api_client)
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        time.sleep(2)
        response = app_api.read_namespaced_deployment_status(
            namespace=namespace, name=name
        )
        s = response.status
        if (
            s.updated_replicas == response.spec.replicas
            and s.replicas == response.spec.replicas
            and s.available_replicas == response.spec.replicas
            and s.observed_generation >= response.metadata.generation
        ):
            return True
        # check failed
        selector = ""
        for k, v in response.spec.selector.match_labels.items():
            selector += k + "=" + v + ","
        selector = selector[:-1]
        pods = core_api.list_namespaced_pod(
            namespace=namespace, label_selector=selector
        )
        for pod in pods.items:
            if pod.status.container_statuses is not None:
                for container_status in pod.status.container_statuses:
                    if (
                        not container_status.ready
                        and container_status.restart_count > 0
                    ):
                        raise K8sError("Deployment {} start failed.".format(name))
    raise TimeoutError("Waiting timeout for deployment {}".format(name))


class KubernetesPodWatcher(object):
    """Class for watching events and logs of kubernetes pod."""

    def __init__(self, api_client, namespace, pod, container=None, queue=None):
        self._api_client = api_client
        self._core_api = kube_client.CoreV1Api(api_client)
        self._app_api = kube_client.AppsV1Api(api_client)

        self._namespace = namespace
        self._pod = pod
        self._container = container

        self._pod_name = pod.metadata.name
        if queue is None:
            self._lines = Queue()
        else:
            self._lines = queue

        self._stream_event_thread = None
        self._stream_log_thread = None
        self._stopped = True

    def _stream_event_impl(self, simple=False):
        field_selector = "involvedObject.name=" + self._pod_name

        event_messages = []
        while not self._stopped:
            time.sleep(1)
            try:
                events = self._core_api.list_namespaced_event(
                    namespace=self._namespace,
                    field_selector=field_selector,
                    timeout_seconds=2,
                )
            except K8SApiException:
                pass
            else:
                for event in events.items:
                    msg = "{0}: {1}".format(self._pod_name, event.message)
                    if msg and msg not in event_messages:
                        event_messages.append(msg)
                        self._lines.put(msg)
                        logger.info(msg, extra={"simple": simple})
                        if event.reason == "Failed":
                            raise K8sError("Kubernetes event error: {}".format(msg))

    def _stream_log_impl(self, simple=False):
        log_messages = []
        while not self._stopped:
            time.sleep(1)
            try:
                logs = self._core_api.read_namespaced_pod_log(
                    namespace=self._namespace,
                    name=self._pod_name,
                    container=self._container,
                )
            except K8SApiException:
                pass
            else:
                for msg in logs.split("\n"):
                    if msg and msg not in log_messages:
                        log_messages.append(msg)
                        self._lines.put(msg)
                        logger.info(msg, extra={"simple": simple})

    def poll(self, block=True, timeout_seconds=None):
        return self._lines.get(block=block, timeout=timeout_seconds)

    def start(self):
        self._stopped = False
        self._stream_event_thread = threading.Thread(
            target=self._stream_event_impl, args=()
        )
        self._stream_event_thread.start()
        time.sleep(1)
        self._stream_log_thread = threading.Thread(
            target=self._stream_log_impl, args=(True,)
        )
        self._stream_log_thread.start()

    def stop(self, timeout_seconds=60):
        if not self._stopped:
            self._stopped = True
            self._stream_event_thread.join(timeout=timeout_seconds)
            self._stream_log_thread.join(timeout=timeout_seconds)
            if (
                self._stream_event_thread.is_alive()
                or self._stream_log_thread.is_alive()
            ):
                raise TimeoutError(
                    "Pod watcher thread joined timeout: {}.".format(self._pod_name)
                )


def get_service_endpoints(  # noqa: C901
    api_client,
    namespace,
    name,
    service_type,
    timeout_seconds=60,
    query_port=None,
):
    """Get service endpoint by service name and service type.

    Args:
        api_client: ApiClient
            An kubernetes ApiClient object, initialized with the client args.
        namespace: str
            Namespace of the service belongs to.
        name: str
            Service name.
        service_type: str
            Service type. Valid options are NodePort, LoadBalancer and ClusterIP.
        timeout_seconds: int
            Raise TimeoutError after waiting for this seconds, only used in LoadBalancer type.

    Raises:
        TimeoutError: If the underlying cloud-provider doesn't support the LoadBalancer
            service type.
        K8sError: The service type is not one of (NodePort, LoadBalancer, ClusterIP). Or
            the service has no endpoint.

    Returns: A list of endpoint.
        If service type is LoadBalancer, format with <load_balancer_ip>:<port>. And
        if service type is NodePort, format with <host_ip>:<node_port>, And
        if service type is ClusterIP, format with <cluster_ip>:<port>
    """
    start_time = time.time()

    core_api = kube_client.CoreV1Api(api_client)
    svc = core_api.read_namespaced_service(name=name, namespace=namespace)

    # get pods
    selector = ""
    for k, v in svc.spec.selector.items():
        selector += k + "=" + v + ","
    selector = selector[:-1]
    pods = core_api.list_namespaced_pod(namespace=namespace, label_selector=selector)

    ips = []
    ports = []
    if service_type == "NodePort":
        for pod in pods.items:
            ips.append(pod.status.host_ip)
        for port in svc.spec.ports:
            if query_port is None or port.port == query_port:
                ports.append(port.node_port)
    elif service_type == "LoadBalancer":
        while True:
            svc = core_api.read_namespaced_service(name=name, namespace=namespace)
            if svc.status.load_balancer.ingress is not None:
                for ingress in svc.status.load_balancer.ingress:
                    if ingress.hostname is not None:
                        ips.append(ingress.hostname)
                    else:
                        ips.append(ingress.ip)
                for port in svc.spec.ports:
                    if query_port is None or port.port == query_port:
                        ports.append(port.port)
                break
            time.sleep(1)
            if time.time() - start_time > timeout_seconds:
                raise TimeoutError("LoadBalancer service type is not supported yet.")
    elif service_type == "ClusterIP":
        ips.append(svc.spec.cluster_ip)
        for port in svc.spec.ports:
            if query_port is None or port.port == query_port:
                ports.append(port.port)
    else:
        raise K8sError("Service type {0} is not supported yet".format(service_type))

    # generate endpoint
    endpoints = []

    if not ips or not ports:
        raise K8sError("Get {0} service {1} failed.".format(service_type, name))

    for ip in ips:
        for port in ports:
            endpoints.append("{0}:{1}".format(ip, port))
    return endpoints


def get_kubernetes_object_info(api_client, target):
    """Get name and kind on valid kubernetes API object (i.e. List, Service, etc).

    Args:
        api_client: ApiClient
            An kubernetes ApiClient object, initialized with the client args.

        target: A valid kubernetes object.

    Returns:
        dict: Key is object name, value is object kind.
    """
    return {target.metadata.name: target.kind}


def delete_kubernetes_object(
    api_client, target, verbose=False, wait=False, timeout_seconds=60, **kwargs
):
    """Perform a delete action on valid kubernetes API object (i.e. List, Service, etc).

    Args:
        api_client: ApiClient
            An kubernetes ApiClient object, initialized with the client args.

        target: A valid kubernetes object.

        verbose: bool, optional
            If True, print confirmation from the delete action. Defaults to False.

        wait: bool, optional
            Waiting for delete object. Defaults to False.

        timeout_seconds: int, optional
            If waiting for delete timeout, just print a error message. Defaults to 60.

    Returns:
        Status: Return status for calls kubernetes delete method.
    """
    group, _, version = target.api_version.partition("/")
    if version == "":
        version = group
        group = "core"
    # Take care for the case e.g. api_type is "apiextensions.k8s.io"
    # Only replace the last instance
    group = "".join(group.rsplit(".k8s.io", 1))
    # convert group name from DNS subdomain format to
    # python class name convention
    group = "".join(word.capitalize() for word in group.split("."))
    fcn_to_call = "{0}{1}Api".format(group, version.capitalize())
    k8s_api = getattr(kube_client, fcn_to_call)(
        api_client
    )  # pylint: disable=not-callable

    kind = target.kind
    kind = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", kind)
    kind = re.sub("([a-z0-9])([A-Z])", r"\1_\2", kind).lower()

    try:
        # Expect the user to create namespaced objects more often
        kwargs["name"] = target.metadata.name
        if hasattr(k8s_api, "delete_namespaced_{0}".format(kind)):
            # Decide which namespace we are going to put the object in, if any
            kwargs["namespace"] = target.metadata.namespace
            resp = getattr(k8s_api, "delete_namespaced_{0}".format(kind))(**kwargs)
        else:
            kwargs.pop("namespace", None)
            resp = getattr(k8s_api, "delete_{0}".format(kind))(**kwargs)
    except K8SApiException:
        # Object already deleted.
        pass
    else:
        # waiting for delete
        if wait:
            start_time = time.time()
            if hasattr(k8s_api, "read_namespaced_{0}".format(kind)):
                while True:
                    try:
                        getattr(k8s_api, "read_namespaced_{0}".format(kind))(**kwargs)
                    except K8SApiException as ex:
                        if ex.status != 404:
                            logger.error(
                                "Deleting {0} {1} failed: {2}".format(
                                    kind, target.metadata.name, str(ex)
                                )
                            )
                        break
                    else:
                        time.sleep(1)
                        if time.time() - start_time > timeout_seconds:
                            logger.info(
                                "Deleting {0} {1} timeout".format(
                                    kind, target.metadata.name
                                )
                            )
        if verbose:
            msg = "{0}/{1} deleted.".format(kind, target.metadata.name)
            if hasattr(resp, "status"):
                msg += " status='{0}'".format(str(resp.status))
            logger.info(msg)
        return resp
