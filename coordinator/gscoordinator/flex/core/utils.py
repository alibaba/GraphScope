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
import functools
import logging
import os
import random
import re
import socket
import string
import time
import traceback
from typing import Union

import requests

from gscoordinator.flex.core.config import CLUSTER_TYPE

logger = logging.getLogger("graphscope")


def resolve_api_client(k8s_config_file=None):
    """Get ApiClient from predefined locations.

    Args:
        k8s_config_file(str): Path to kubernetes config file.

    Raises:
        RuntimeError: K8s api client resolve failed.

    Returns:
        An kubernetes ApiClient object, initialized with the client args.

    The order of resolution as follows:
        1. load from kubernetes config file or,
        2. load from incluster configuration

    RuntimeError will be raised if resolution failed.
    """
    try:
        from kubernetes import client as kube_client
        from kubernetes import config as kube_config

        # load from kubernetes config file
        kube_config.load_kube_config(k8s_config_file)
    except:  # noqa: E722
        try:
            # load from incluster configuration
            kube_config.load_incluster_config()
        except:  # noqa: E722
            raise RuntimeError("Resolve kube api client failed.")
    return kube_client.ApiClient()

def get_pod_ips(api_client, namespace, pod_prefix):
    """Get pod ip by pod name prefix.

    Args:
        api_client: ApiClient
            An kubernetes ApiClient object, initialized with the client args.
        namespace: str
            Namespace of the pod belongs to.
        pod_prefix: str
            Pod name prefix.

    Raises:
        RuntimeError: Get pod ip failed.

    Returns:
        Pod ip.
    """
    from kubernetes import client as kube_client
    core_api = kube_client.CoreV1Api(api_client)
    pods = core_api.list_namespaced_pod(namespace=namespace)
    ips = []
    for pod in pods.items:
        if pod.metadata.name.startswith(pod_prefix):
            if pod.status.phase == "Running":
                # append (ip, pod_name)
                ips.append((pod.status.pod_ip, pod.metadata.name))
            else:
                raise RuntimeError(f"Pod {pod.metadata.name} is not running.")
    if not ips:
        raise RuntimeError(f"Get pod ip failed.")
    return ips


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
            Raise TimeoutError after the duration, only used in LoadBalancer type.

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

    from kubernetes import client as kube_client
    core_api = kube_client.CoreV1Api(api_client)
    svc = core_api.read_namespaced_service(name=name, namespace=namespace)

    # get pods
    selector = ",".join([f"{k}={v}" for k, v in svc.spec.selector.items()])
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
            if svc.status.load_balancer.ingress is None:
                if time.time() - start_time > timeout_seconds:
                    raise TimeoutError(
                        "LoadBalancer service type is not supported yet."
                    )
                time.sleep(1)
                continue
            for ingress in svc.status.load_balancer.ingress:
                if ingress.hostname is not None:
                    ips.append(ingress.hostname)
                else:
                    ips.append(ingress.ip)
            for port in svc.spec.ports:
                if query_port is None or port.port == query_port:
                    ports.append(port.port)
            break
    elif service_type == "ClusterIP":
        ips.append(svc.spec.cluster_ip)
        for port in svc.spec.ports:
            if query_port is None or port.port == query_port:
                ports.append(port.port)
    else:
        raise RuntimeError("Service type {0} is not supported yet".format(service_type))

    if not ips or not ports:
        raise RuntimeError(f"Get {service_type} service {name} failed.")

    endpoints = [f"{ip}:{port}" for ip in ips for port in ports]
    return endpoints


def handle_api_exception():
    """Decorator to handle api exception for openapi controllers."""

    def _handle_api_exception(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logger.info("Exception occurred: %s, %s", str(e), traceback.format_exc())
                return f"Exception occurred: {str(e)}, traceback {traceback.format_exc()}", 500

        return wrapper

    return _handle_api_exception


def decode_datetimestr(datetime_str):
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
        "%Y-%m-%d-%H-%M-%S",
        "%Y-%m-%d-%H-%M-%S-%f",
    ]
    for f in formats:
        try:
            return datetime.datetime.strptime(datetime_str, f)
        except ValueError:
            pass
    raise RuntimeError(
        "Decode '{0}' failed: format should be one of '{1}'".format(
            datetime_str, str(formats)
        )
    )


def encode_datetime(dt):
    if isinstance(dt, datetime.datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


def random_string(nlen):
    return "".join([random.choice(string.ascii_lowercase) for _ in range(nlen)])


def get_current_time() -> datetime.datetime:
    return datetime.datetime.now()


def get_internal_ip() -> str:
    hostname = socket.gethostname()
    internal_ip = socket.gethostbyname(hostname)
    return internal_ip


def parse_file_metadata(location: str) -> dict:
    """
    Args:
        location: optional values:
            odps://path/to/file, hdfs://path/to/file, file:///path/to/file
            /home/graphscope/path/to/file
    """
    metadata = {"datasource": "file"}
    path = location
    pattern = r"^(odps|hdfs|file|oss|s3)?://([\w/.-]+)$"
    match = re.match(pattern, location)
    if match:
        datasource = match.group(1)
        metadata["datasource"] = datasource
        if datasource == "file":
            path = match.group(2)
    if metadata["datasource"] == "file":
        _, file_extension = os.path.splitext(path)
        metadata["file_type"] = file_extension[1:]
    return metadata


def get_public_ip() -> Union[str, None]:
    try:
        response = requests.get("https://api.ipify.org?format=json")
        if response.status_code == 200:
            data = response.json()
            return data["ip"]
        else:
            return None
    except requests.exceptions.RequestException as e:
        logger.warn("Failed to get public ip: %s", str(e))
        return None


def data_type_to_groot(property_type):
    if "primitive_type" in property_type:
        t = property_type["primitive_type"]
        if t == "DT_DOUBLE":
            return "double"
        elif t == "DT_SIGNED_INT32" or t == "DT_SIGNED_INT64":
            return "long"
        else:
            raise RuntimeError(f"Data type {t} is not supported yet.")
    elif "string" in property_type:
        return "str"
    else:
        raise RuntimeError(f"Data type {str(property_type)} is not supported yet.")
