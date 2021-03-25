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
import shutil
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
from gscoordinator.utils import get_timestamp
from gscoordinator.utils import is_port_in_use
from gscoordinator.utils import parse_as_glog_level

logger = logging.getLogger("graphscope")


class Launcher(metaclass=ABCMeta):
    def __init__(self):
        self._num_workers = None
        self._analytical_engine_endpoint = None

    @property
    def analytical_engine_endpoint(self):
        if self._analytical_engine_endpoint is None:
            raise RuntimeError("Get None value of analytical engine endpoint.")
        return str(self._analytical_engine_endpoint)

    @property
    def num_workers(self):
        if self._num_workers is None:
            raise RuntimeError("Get None value of workers number.")
        return int(self._num_workers)

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

    _vineyard_socket_prefix = "/tmp/vineyard.sock."

    def __init__(
        self,
        num_workers,
        hosts,
        vineyard_socket,
        shared_mem,
        log_level,
        timeout_seconds,
    ):
        super().__init__()
        self._num_workers = num_workers
        self._hosts = hosts
        self._vineyard_socket = vineyard_socket
        self._shared_mem = shared_mem
        self._glog_level = parse_as_glog_level(log_level)
        self._timeout_seconds = timeout_seconds

        # vineyardd
        self._vineyardd_process = None
        # analytical engine
        self._analytical_engine_process = None

    def type(self):
        return types_pb2.HOSTS

    def start(self):
        try:
            self._create_services()
        except Exception as e:
            logger.error("Error when launching GraphScope locally: %s", str(e))
            self.stop()
            return False
        return True

    def stop(self, is_dangling=False):
        self._stop_interactive_engine_service()
        self._stop_vineyard()
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

    def _create_interactive_engine_service(self):
        pass

    def _find_vineyardd(self):
        vineyardd = ""
        if "VINEYARD_HOME" in os.environ:
            vineyardd = os.path.expandvars("$VINEYARD_HOME/vineyardd")
        if not vineyardd:
            vineyardd = shutil.which("vineyardd")
        if not vineyardd:
            vineyardd = "vineyardd"
        return vineyardd

    def _create_vineyard(self):
        if self._vineyard_socket is None:
            ts = get_timestamp()
            vineyard_socket = "{0}{1}".format(self._vineyard_socket_prefix, ts)
            cmd = [self._find_vineyardd()]
            cmd.extend(["--socket", vineyard_socket])
            cmd.extend(["--size", self._shared_mem])
            cmd.extend(["--etcd_prefix", "vineyard.gsa.{0}".format(ts)])
            env = os.environ.copy()
            env["GLOG_v"] = str(self._glog_level)

            logger.info("Launch vineyardd with command: {0}".format(" ".join(cmd)))

            process = subprocess.Popen(
                cmd,
                start_new_session=True,
                cwd=os.getcwd(),
                env=env,
                universal_newlines=True,
                encoding="utf-8",
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
            )

            logger.info("Server is initializing vineyardd.")
            stdout_watcher = PipeWatcher(process.stdout, sys.stdout)
            setattr(process, "stdout_watcher", stdout_watcher)

            self._vineyard_socket = vineyard_socket
            self._vineyardd_process = process

    def _create_services(self):
        # create GIE graph manager
        self._create_interactive_engine_service()
        # create vineyard
        self._create_vineyard()
        # create GAE rpc service
        self._start_analytical_engine()

    def _start_analytical_engine(self):
        rmcp = ResolveMPICmdPrefix()
        cmd, mpi_env = rmcp.resolve(self._num_workers, self._hosts)

        master = self._hosts.split(",")[0]
        rpc_port = self._get_free_port(master)
        self._analytical_engine_endpoint = "{}:{}".format(master, str(rpc_port))

        cmd.append(ANALYTICAL_ENGINE_PATH)
        cmd.extend(["--host", "0.0.0.0"])
        cmd.extend(["--port", str(rpc_port)])

        if rmcp.openmpi():
            cmd.extend(["-v", str(self._glog_level)])
        else:
            mpi_env["GLOG_v"] = str(self._glog_level)

        if self._vineyard_socket:
            cmd.extend(["--vineyard_socket", self._vineyard_socket])

        env = os.environ.copy()
        env.update(mpi_env)

        logger.info("Launch analytical engine with command: {}".format(" ".join(cmd)))

        process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=os.getcwd(),
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

    def _stop_vineyard(self):
        self._stop_subprocess(self._vineyardd_process)

    def _stop_interactive_engine_service(self):
        pass

    def _stop_analytical_engine(self):
        self._stop_subprocess(self._analytical_engine_process)
        self._analytical_engine_endpoint = None

    def _stop_subprocess(self, proc):
        if proc:
            proc.terminate()
            proc.wait()
            proc = None
