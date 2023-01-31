#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited.
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
from abc import ABCMeta
from abc import abstractmethod

from gscoordinator.utils import GRAPHSCOPE_HOME

logger = logging.getLogger("graphscope")


def configure_environ():
    # add `${GRAPHSCOPE_HOME}/bin` to ${PATH}
    os.environ["PATH"] += os.pathsep + os.path.join(GRAPHSCOPE_HOME, "bin")
    # OPAL_PREFIX for openmpi
    if os.path.isdir(os.path.join(GRAPHSCOPE_HOME, "openmpi")):
        os.environ["OPAL_PREFIX"] = os.path.join(GRAPHSCOPE_HOME, "openmpi")
    if os.path.isdir(os.path.join("/opt", "openmpi")):
        os.environ["OPAL_PREFIX"] = os.path.join("/opt", "openmpi")
    # Darwin is open-mpi
    if os.path.isdir(os.path.join(GRAPHSCOPE_HOME, "open-mpi")):
        os.environ["OPAL_PREFIX"] = os.path.join(GRAPHSCOPE_HOME, "open-mpi")
    # add '${GRAPHSCOPE_HOME}/lib' to ${LD_LIBRARY_PATH} to find libvineyard_internal_registry.so(dylib)
    if "LD_LIBRARY_PATH" in os.environ:
        os.environ["LD_LIBRARY_PATH"] = (
            os.path.join(GRAPHSCOPE_HOME, "lib")
            + os.pathsep
            + os.environ["LD_LIBRARY_PATH"]
        )
    else:
        os.environ["LD_LIBRARY_PATH"] = os.path.join(GRAPHSCOPE_HOME, "lib")
    if "DYLD_LIBRARY_PATH" in os.environ:
        os.environ["DYLD_LIBRARY_PATH"] = (
            os.path.join(GRAPHSCOPE_HOME, "lib")
            + os.pathsep
            + os.environ["DYLD_LIBRARY_PATH"]
        )
    else:
        os.environ["DYLD_LIBRARY_PATH"] = os.path.join(GRAPHSCOPE_HOME, "lib")


class AbstractLauncher(metaclass=ABCMeta):
    def __init__(self):
        self._instance_id = None
        self._num_workers = None
        self._hosts = ""
        self._analytical_engine_process = None
        self._analytical_engine_endpoint = None
        self._session_workspace = None
        configure_environ()

    @abstractmethod
    def create_analytical_instance(self):
        pass

    @abstractmethod
    def create_interactive_instance(self, object_id: int, schema_path: str):
        pass

    @abstractmethod
    def create_learning_instance(self, object_id: int, handle: str, config: str):
        pass

    @abstractmethod
    def close_analytical_instance(self):
        pass

    @abstractmethod
    def close_interactive_instance(self, object_id: int):
        pass

    @abstractmethod
    def close_learning_instance(self, object_id: int):
        pass

    @abstractmethod
    def launch_etcd(self):
        pass

    @abstractmethod
    def launch_vineyard(self):
        pass

    @abstractmethod
    def close_etcd(self):
        pass

    @abstractmethod
    def close_vineyard(self):
        pass

    @abstractmethod
    def configure_etcd_endpoint(self):
        pass

    @abstractmethod
    def get_engine_config(self):
        pass

    @abstractmethod
    def get_vineyard_stream_info(self):
        pass

    @abstractmethod
    def distribute_file(self, path):
        pass

    @property
    def analytical_engine_endpoint(self):
        return self._analytical_engine_endpoint

    @property
    def analytical_engine_process(self):
        return self._analytical_engine_process

    @property
    def num_workers(self):
        if self._num_workers is None:
            raise RuntimeError("Get None value of workers number.")
        return int(self._num_workers)

    @property
    def instance_id(self):
        return self._instance_id

    @property
    def hosts(self):
        return self._hosts

    @abstractmethod
    def type(self):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self, is_dangling=False):
        pass

    @abstractmethod
    def set_session_workspace(self, session_id):
        pass

    def get_namespace(self):
        pass
