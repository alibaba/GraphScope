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
import random
import signal
import subprocess
import sys
from abc import ABCMeta
from abc import abstractmethod

from graphscope.proto import types_pb2

from gscoordinator.io_utils import PipeWatcher
from gscoordinator.utils import ANALYTICAL_ENGINE_HOME
from gscoordinator.utils import ANALYTICAL_ENGINE_PATH
from gscoordinator.utils import ResolveMPICmdPrefix
from gscoordinator.utils import is_port_in_use
from gscoordinator.utils import parse_as_glog_level

logger = logging.getLogger("graphscope")


class Launcher(metaclass=ABCMeta):
    def __init__(self):
        self._analytical_engine_endpoint = None

    def get_analytical_engine_endpoint(self):
        if self._analytical_engine_endpoint is None:
            raise RuntimeError("Get None value of analytical engine endpoint.")
        return str(self._analytical_engine_endpoint)

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
    def poll(self):
        pass


class LocalLauncher(Launcher):
    """
    Launch engine localy with serveral hosts.
    """

    def __init__(self, num_workers, hosts, vineyard_socket, log_level, timeout_seconds):
        super().__init__()
        self._num_workers = num_workers
        self._hosts = hosts
        self._vineyard_socket = vineyard_socket
        self._glog_level = parse_as_glog_level(log_level)
        self._timeout_seconds = timeout_seconds

        # analytical engine
        self._analytical_engine_process = None

    def type(self):
        return types_pb2.HOSTS

    def start(self):
        self._start_analytical_engine()
        return True

    def stop(self, is_dangling=False):
        self._stop_analytical_engine()

    def poll(self):
        if self._analytical_engine_process:
            return self._analytical_engine_process.poll()
        return -1

    @property
    def hosts(self):
        return self._hosts

    def _get_free_port(self, host):
        port = random.randint(60001, 65535)
        while is_port_in_use(host, port):
            port = random.randint(60001, 65535)
        return port

    def _start_analytical_engine(self):
        rmcp = ResolveMPICmdPrefix()
        cmd, mpi_env = rmcp.resolve(self._num_workers, self._hosts)

        master = self._hosts.split(",")[0]
        rpc_port = self._get_free_port(master)
        self._analytical_engine_endpoint = "{}:{}".format(master, str(rpc_port))

        cmd.append(ANALYTICAL_ENGINE_PATH)
        cmd.extend(["--port", str(rpc_port)])

        if rmcp.openmpi():
            cmd.extend(["-v", str(self._glog_level)])
        else:
            mpi_env["GLOG_v"] = str(self._glog_level)

        if self._vineyard_socket:
            cmd.extend(["--vineyard_socket", self._vineyard_socket])

        env = os.environ.copy()
        env.update(mpi_env)

        process = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(ANALYTICAL_ENGINE_PATH),
            env=env,
            universal_newlines=True,
            encoding="utf-8",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )

        logger.info("Server is initializing analytical engine.")
        stdout_watcher = PipeWatcher(process.stdout, sys.stdout)
        setattr(process, "stdout_watcher", stdout_watcher)

        self._analytical_engine_process = process

    def _stop_analytical_engine(self):
        if self._analytical_engine_process:
            self._analytical_engine_process.terminate()
            self._analytical_engine_process.wait()
            self._analytical_engine_process = None
            self._analytical_engine_endpoint = None
