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

from graphscope.version import __is_prerelease__
from graphscope.version import __version__

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
    k8s_vineyard_image = "vineyardcloudnative/vineyardd:v0.11.7"
    k8s_vineyard_daemonset = None
    k8s_vineyard_cpu = 0.5
    k8s_vineyard_mem = "512Mi"
    vineyard_shared_mem = "4Gi"

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

    # launch graphscope with mars
    with_mars = False
    with_analytical = True
    with_analytical_java = False
    with_interactive = True
    with_learning = True
    # Demo dataset related
    with_dataset = False

    k8s_volumes = {}

    k8s_service_type = "NodePort"

    # support resource preemption or resource guarantee
    preemptive = True

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
