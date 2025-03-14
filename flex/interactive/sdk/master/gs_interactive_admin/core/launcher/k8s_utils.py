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

from kubernetes import client as kube_client
from kubernetes import config as kube_config
from kubernetes.client.rest import ApiException as K8SApiException

import os


# Refer from: https://github.com/alibaba/GraphScope/blob/main/python/graphscope/deploy/kubernetes/utils.py
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
        2. load from incluster configuration or,
        3. set api address from env if `KUBE_API_ADDRESS` exist.
    RuntimeError will be raised if resolution failed.
    """
    try:
        # load from kubernetes config file
        kube_config.load_kube_config(k8s_config_file)
    except:  # noqa: E722
        try:
            # load from incluster configuration
            kube_config.load_incluster_config()
        except Exception as e:  # noqa: E722
            if "KUBE_API_ADDRESS" in os.environ:
                # try to load from env `KUBE_API_ADDRESS`
                config = kube_client.Configuration()
                config.host = os.environ["KUBE_API_ADDRESS"]
                return kube_client.ApiClient(config)
            raise RuntimeError("Resolve kube api client failed, exception: %s" % e)
    return kube_client.ApiClient()
