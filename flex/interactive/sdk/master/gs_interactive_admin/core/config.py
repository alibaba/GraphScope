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

from simple_parsing import ArgumentParser
from simple_parsing.helpers import Serializable
from simple_parsing.helpers import list_field


@dataclass
class ComputeEngine:
    """
    Stores configurations for the compute engine.
    """

    engine: str = "vineyard"
    vineyard_socket: str = "vineyard.default"
    vineyard_rpc_endpoint: str = ""


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
class Config(Serializable):
    """
    Stores all configurations for Interactive. Corresponding to the yaml file https://github.com/alibaba/GraphScope/blob/main/flex/tests/hqps/interactive_config_test.yaml
    """

    log_level: str = "INFO"
    verbose_level: int = 0

    compute_engine: ComputeEngine = field(default_factory=ComputeEngine)
    namespace: str = "interactive"
    instance_name: str = "default"

    http_service: HttpService = field(default_factory=HttpService)

    service_registry: ServiceRegistry = field(default_factory=ServiceRegistry)
