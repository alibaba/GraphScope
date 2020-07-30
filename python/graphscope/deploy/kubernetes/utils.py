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
import re
import time

from kubernetes import client as kube_client
from kubernetes import config as kube_config
from kubernetes.client.rest import ApiException as K8SApiException

logger = logging.getLogger("graphscope")


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


def is_minikube_cluster():
    contexts, active_context = kube_config.list_kube_config_contexts()
    if contexts:
        return active_context["context"]["cluster"] == "minikube"


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
