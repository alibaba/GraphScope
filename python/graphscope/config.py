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


class GSConfig(object):
    NAMESPACE = None

    # image
    ZOOKEEPER_IMAGE = "zookeeper:3.4.14"
    ETCD_IMAGE = "quay.io/coreos/etcd:v3.4.13"
    GS_IMAGE = "registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:latest"
    GIE_GRAPH_MANAGER_IMAGE = (
        "registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:1.0"
    )

    # image pull configuration
    IMAGE_PULL_POLICY = "IfNotPresent"
    IMAGE_PULL_SECRETS = []

    # coordinator resource configuration
    COORDINATOR_CPU = 1.0
    COORDINATOR_MEM = "4Gi"

    # vineyard resource configuration
    VINEYARD_CPU = 0.5
    VINEYARD_MEM = "512Mi"
    VINEYARD_SHARED_MEM = "4Gi"

    # engine resource configuration
    ENGINE_CPU = 0.5
    ENGINE_MEM = "4Gi"

    NUM_WORKERS = 2
    SHOW_LOG = False
    LOG_LEVEL = "info"

    TIMEOUT_SECONDS = 600
    WAITING_FOR_DELETE = False
