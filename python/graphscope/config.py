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

    # etcd image
    k8s_etcd_image = "quay.io/coreos/etcd:v3.4.13"

    # All in one image
    k8s_gs_image = f"{registry}/graphscope/graphscope:{__version__}"

    # Coordinator image
    # Also could be used as a client image
    k8s_coordinator_image = f"{registry}/graphscope/coordinator:{__version__}"

    # Dataset image
    k8s_dataset_image = f"{registry}/graphscope/dataset:{__version__}"

    # image pull configuration
    k8s_image_pull_policy = "IfNotPresent"
    k8s_image_pull_secrets = []

    # coordinator resource configuration
    k8s_coordinator_cpu = 1.5
    k8s_coordinator_mem = "2Gi"

    # etcd resource configuration
    etcd_addrs = None
    etcd_listening_client_port = 2379
    etcd_listening_peer_port = 2380
    k8s_etcd_num_pods = 1
    k8s_etcd_cpu = 1.0
    k8s_etcd_mem = "512Mi"

    # vineyard resource configuration
    k8s_vineyard_daemonset = "none"
    k8s_vineyard_cpu = 0.2
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
    k8s_etcd_pod_node_selector = None
    k8s_engine_pod_node_selector = None

    # launch graphscope with mars
    with_mars = False

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

    # Demo dataset related
    mount_dataset = None

    # download_retries
    dataset_download_retries = 3
