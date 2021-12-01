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


import logging
import os
import signal
import subprocess
import sys

import graphscope
from graphscope.config import GSConfig as gs_config
from graphscope.deploy.launcher import Launcher
from graphscope.framework.utils import PipeWatcher
from graphscope.framework.utils import get_free_port
from graphscope.framework.utils import is_free_port
from graphscope.framework.utils import random_string

try:
    import gscoordinator

    COORDINATOR_HOME = os.path.abspath(os.path.join(gscoordinator.__file__, "..", ".."))
except ModuleNotFoundError:
    # If gscoordinator is not installed, try to locate it by relative path,
    # which is strong related with the directory structure of GraphScope
    COORDINATOR_HOME = os.path.abspath(
        os.path.join(__file__, "..", "..", "..", "..", "..", "coordinator")
    )

logger = logging.getLogger("graphscope")


class HostsClusterLauncher(Launcher):
    """Class for setting up GraphScope instance on hosts cluster"""

    def __init__(
        self,
        hosts=None,
        port=None,
        num_workers=None,
        vineyard_socket=None,
        timeout_seconds=None,
        vineyard_shared_mem=None,
        **kwargs
    ):
        self._hosts = hosts
        self._port = port
        self._num_workers = num_workers
        self._vineyard_socket = vineyard_socket
        self._timeout_seconds = timeout_seconds
        self._vineyard_shared_mem = vineyard_shared_mem

        self._instance_id = random_string(6)
        self._proc = None
        self._closed = True

    def poll(self):
        if self._proc is not None:
            return self._proc.poll()
        return -1

    def _launch_coordinator(self):
        if self._port is None:
            self._port = get_free_port()
        else:
            # check port conflict
            if not is_free_port(self._port):
                raise RuntimeError("Port {} already used.".format(self._port))

        self._coordinator_endpoint = "{}:{}".format(self._hosts[0], self._port)

        cmd = [
            sys.executable,
            "-m",
            "gscoordinator",
            "--num_workers",
            "{}".format(str(self._num_workers)),
            "--hosts",
            "{}".format(",".join(self._hosts)),
            "--log_level",
            "{}".format(gs_config.log_level),
            "--timeout_seconds",
            "{}".format(self._timeout_seconds),
            "--port",
            "{}".format(str(self._port)),
            "--cluster_type",
            self.type(),
            "--instance_id",
            self._instance_id,
        ]

        if self._vineyard_shared_mem is not None:
            cmd.extend(["--vineyard_shared_mem", self._vineyard_shared_mem])

        if self._vineyard_socket:
            cmd.extend(["--vineyard_socket", "{}".format(self._vineyard_socket)])

        logger.info("Initializing coordinator with command: %s", " ".join(cmd))

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "TRUE"
        # add graphscope module to PYTHONPATH
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = (
                os.path.join(os.path.dirname(graphscope.__file__), "..")
                + os.pathsep
                + env["PYTHONPATH"]
            )
        else:
            env["PYTHONPATH"] = os.path.join(os.path.dirname(graphscope.__file__), "..")

        # Param `start_new_session=True` is for putting child process to a new process group
        # so it won't get the signals from parent.
        process = subprocess.Popen(
            cmd,
            start_new_session=True,
            cwd=COORDINATOR_HOME,
            env=env,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE if gs_config.show_log else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1,
        )
        if gs_config.show_log:
            stdout_watcher = PipeWatcher(process.stdout, sys.stdout)
            setattr(process, "stdout_watcher", stdout_watcher)
        stderr_watcher = PipeWatcher(process.stderr, sys.stderr)
        setattr(process, "stderr_watcher", stderr_watcher)
        self._proc = process

    def type(self):
        return "hosts"

    def start(self):
        """Launch graphscope instance on hosts cluster.

        Raises:
            RuntimeError: If instance launch failed or timeout.

        Returns: tuple of process and endpoint
        """
        try:
            self._launch_coordinator()
            self._closed = False
            logger.info(
                "Coordinator service started successful, connecting to service..."
            )
        except Exception as e:
            self.stop()
            raise RuntimeError(
                "Error when launching coordinator on hosts cluster"
            ) from e

    def stop(self):
        """Stop GraphScope instance."""
        # coordinator's GRPCServer.wait_for_termination works for SIGINT (Ctrl-C)
        if not self._closed:
            if self._proc is not None:
                self._proc.send_signal(signal.SIGINT)
                self._proc.wait(timeout=10)
                self._proc = None
            self._closed = True
