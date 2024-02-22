#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020-2023 Alibaba Group Holding Limited. All Rights Reserved.
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

from graphscope.config import Config
from graphscope.proto import types_pb2

from gscoordinator.constants import ANALYTICAL_CONTAINER_NAME
from gscoordinator.launcher import AbstractLauncher
from gscoordinator.utils import WORKSPACE
from gscoordinator.utils import delegate_command_to_pod
from gscoordinator.utils import run_kube_cp_command

logger = logging.getLogger("graphscope")


class OperatorLauncher(AbstractLauncher):
    def __init__(self, config: Config):
        super().__init__()
        # Session Config
        self._num_workers = config.session.num_workers
        self._instance_id = config.session.instance_id

        # Vineyard Config
        self._vineyard_socket = config.vineyard.socket
        self._vineyard_rpc_port = config.vineyard.rpc_port

        # Launcher Config
        self._namespace = config.operator_launcher.namespace
        self._hosts = config.operator_launcher.hosts
        self._analytical_engine_endpoint = config.operator_launcher.gae_endpoint

        # A graphscope instance may have multiple session by reconnecting to coordinator
        self._instance_workspace = os.path.join(WORKSPACE, self._instance_id)
        os.makedirs(self._instance_workspace, exist_ok=True)
        # setting during client connect to coordinator
        self._session_workspace = None

    def type(self):
        return types_pb2.OPERATOR

    def stop(self, is_dangling=False):
        pass

    def set_session_workspace(self, session_id: str):
        self._session_workspace = os.path.join(self._instance_workspace, session_id)
        os.makedirs(self._session_workspace, exist_ok=True)

    def get_namespace(self) -> str:
        return self._namespace

    @property
    def hosts(self):
        return self._hosts

    @property
    def vineyard_socket(self) -> str:
        return self._vineyard_socket

    @property
    def vineyard_endpoint(self) -> str:
        return f"{self._hosts[0]}:{self._vineyard_rpc_port}"

    def create_analytical_instance(self):
        pass

    def create_interactive_instance(
        self, object_id: int, schema_path: str, params: dict, with_cypher: bool
    ):
        pass

    def create_learning_instance(self, object_id, handle, config):
        pass

    def close_analytical_instance(self):
        pass

    def close_interactive_instance(self, object_id):
        pass

    def close_learning_instance(self, object_id):
        pass

    def launch_etcd(self):
        pass

    def launch_vineyard(self):
        pass

    def close_etcd(self):
        pass

    def close_vineyard(self):
        pass

    def configure_etcd_endpoint(self):
        pass

    def distribute_file(self, path):
        container = ANALYTICAL_CONTAINER_NAME
        for pod in self._hosts:
            try:
                # The library may exist in the analytical pod.
                test_cmd = f"test -f {path}"
                logger.debug(delegate_command_to_pod(test_cmd, pod, container))
                logger.info("Library exists, skip distribute")
            except RuntimeError:
                cmd = f"mkdir -p {os.path.dirname(path)}"
                logger.debug(delegate_command_to_pod(cmd, pod, container))
                logger.debug(run_kube_cp_command(path, path, pod, container, True))

    def start(self):
        return True

    def get_engine_config(self) -> dict:
        config = {
            "engine_hosts": ",".join(self._hosts),
            "mars_endpoint": None,
        }
        return config

    def get_vineyard_stream_info(self):
        hosts = [f"{self._namespace}:{host}" for host in self._pod_name_list]
        return "kubernetes", hosts
