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

from dataclasses import dataclass
from dataclasses import field
from typing import Union

from simple_parsing import ArgumentParser
from simple_parsing.helpers import Serializable
from simple_parsing.helpers import list_field

import os

OSS_BUCKET_NAME = os.getenv("OSS_BUCKET_NAME", "graphscope")
OSS_BUCKET_DATA_DIR = os.getenv("OSS_BUCKET_DATA_DIR", "interactive")

INTERACTIVE_WORKSPACE = os.environ.get("INTERACTIVE_WORKSPACE", "/tmp/interactive_workspace")


@dataclass
class MetadataStore:
    """
    Stores configurations for the metadata store.
    """

    uri: str = ""


@dataclass
class ComputeEngine:
    """
    Stores configurations for the compute engine.
    """

    engine: str = "vineyard"
    vineyard_socket: str = "vineyard.default"
    vineyard_rpc_endpoint: str = ""

    thread_num_per_worker: int = 1
    memory_per_worker: str = "4Gi"

    metadata_store: MetadataStore = field(default_factory=MetadataStore)
    wal_uri: str = f"file://{{GRAPH_DATA_DIR}}/wal"
    
    config_file_mount_path: str = "/opt/flex/share/interactive_config.yaml"
    entrypoint_mount_path: str = "/etc/interactive/engine_entrypoint.sh"


@dataclass
class HttpService:
    """
    Stores configurations for the http service.
    """

    default_listen_address: str = "localhost"
    admin_port: int = 7777
    query_port: int = 10000
    max_content_length: str = "1GB"


@dataclass
class ServiceRegistry:
    """
    Stores configurations for the service registry.
    """

    type: str = "etcd"
    endpoint: str = "http://localhost:2379"
    ttl: int = 60


@dataclass
class K8sLauncherConfig:
    """
    Stores configurations for the k8s launcher.
    """

    # The namespace must be created before launching the interactive engine.
    namespace: Union[str, None] = "default"
    instance_prefix: str = "gs-interactive"
    instance_id: str = (
        None  # If instance_id is specified, it will override the instance_prefix
    )
    default_replicas: int = 1
    config_file: Union[str, None] = None

    image_pull_policy: str = "Always"
    image_registry: str = "registry.cn-hongkong.aliyuncs.com"
    image_tag: str = "debug"
    repository: str = "graphscope"
    image_name: str = "interactive"

    default_container_name: str = "interactive"

    volume_claim_name: str = "interactive-workspace"
    volume_mount_path: str = "/tmp/interactive"
    volume_size: str = "1Gi"
    volume_access_mode: str = "ReadWriteOnce"
    volume_storage_class: str = "standard"

    node_selectors: dict = field(default_factory=dict)
    affinity: dict = field(default_factory=dict)
    tolerations: list = field(default_factory=list)
    annotations: dict = field(default_factory=dict)

    service_type: str = "NodePort"
    cluster_ip: str = (
        None  # If service_type is ClusterIP, user could specify the cluster_ip
    )
    update_strategy: str = "RollingUpdate"
    engine_pod_annotations: dict = field(default_factory=dict)
    service_account_create: bool = False
    service_account_name: str = None
    
    engine_config_file_mount_path: str = "/opt/flex/share/interactive_config.yaml"
    engine_entrypoint_mount_path: str = "/etc/interactive/engine_entrypoint.sh"


@dataclass
class Master:
    port: int = 7776
    instance_name: str = "test"
    service_registry: ServiceRegistry = field(default_factory=ServiceRegistry)

    k8s_launcher_config: K8sLauncherConfig = field(default_factory=K8sLauncherConfig)
    launcher_type: str = "k8s"
    entrypoint_mount_path: str = "/etc/interactive/master_entrypoint.sh"
    config_file_mount_path: str = "/opt/flex/share/interactive_config.yaml"


@dataclass
class ConnectorConfig:
    disabled: bool = False
    port: int = 7687


@dataclass
class CompilerEndpoint:
    default_listen_address: str = "localhost"
    bolt_connector: ConnectorConfig = field(default_factory=ConnectorConfig)


@dataclass
class CompilerConfig:
    endpoint: CompilerEndpoint = field(default_factory=CompilerEndpoint)


@dataclass
class Config(Serializable):
    """
    Stores all configurations for Interactive. Corresponding to the yaml file https://github.com/alibaba/GraphScope/blob/main/flex/tests/hqps/interactive_config_test.yaml
    """

    log_level: str = "INFO"
    verbose_level: int = 0

    compute_engine: ComputeEngine = field(default_factory=ComputeEngine)

    compiler: CompilerConfig = field(default_factory=CompilerConfig)

    http_service: HttpService = field(default_factory=HttpService)

    workspace: str = "/tmp/interactive_workspace"

    master: Master = field(default_factory=Master)
