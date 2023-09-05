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
import base64
import copy
import logging
import os
import signal
import subprocess
import sys

import graphscope
from graphscope.config import Config
from graphscope.deploy.launcher import Launcher
from graphscope.framework.utils import PipeWatcher
from graphscope.framework.utils import get_free_port
from graphscope.framework.utils import in_notebook
from graphscope.framework.utils import is_free_port

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

    def __init__(self, config: Config):
        self._config = copy.deepcopy(config)
        self._proc = None

        port = self._config.coordinator.service_port
        if not is_free_port(port):
            port = get_free_port()
            self._config.coordinator.service_port = port
        self._coordinator_endpoint = f"{self._config.hosts_launcher.hosts[0]}:{port}"

    def poll(self):
        if self._proc is not None:
            return self._proc.poll()
        return -1

    def base64_encode(self, string):
        return base64.b64encode(string.encode("utf-8")).decode("utf-8", errors="ignore")

    def _launch_coordinator(self):
        cmd = [
            sys.executable,
            "-m",
            "gscoordinator",
            "--config",
            self.base64_encode(self._config.dumps_json()),
        ]

        # logger.info("Initializing coordinator with command: %s", " ".join(cmd))

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "TRUE"
        # add graphscope module to PYTHONPATH
        graphscope_dir = os.path.join(os.path.dirname(graphscope.__file__), "..")
        coordinator_dir = os.path.join(graphscope_dir, "..", "coordinator")
        additional_path = graphscope_dir + os.pathsep + coordinator_dir

        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = additional_path + os.pathsep + env["PYTHONPATH"]
        else:
            env["PYTHONPATH"] = additional_path

        # Param `start_new_session=True` is for putting child process to a new process group
        # so it won't get the signals from parent.
        # In notebook environment, we need to accept the signal from kernel restarted/stoped.
        process = subprocess.Popen(
            cmd,
            start_new_session=False if in_notebook() else True,
            cwd=os.getcwd(),
            env=env,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1,
        )
        stdout_watcher = PipeWatcher(process.stdout, sys.stdout)
        if not self._config.session.show_log:
            stdout_watcher.add_filter(
                lambda line: "Loading" in line and "it/s]" in line
            )
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
            logger.info(
                "Coordinator service started successful, connecting to service..."
            )
        except Exception as e:
            self.stop()
            raise RuntimeError(
                "Error when launching coordinator on hosts cluster"
            ) from e

    def stop(self, wait=False):
        """Stop GraphScope instance."""
        # coordinator's GRPCServer.wait_for_termination works for SIGINT (Ctrl-C)
        if self._proc is not None:
            self._proc.send_signal(signal.SIGINT)
            self._proc.wait(timeout=10)
            self._proc = None
