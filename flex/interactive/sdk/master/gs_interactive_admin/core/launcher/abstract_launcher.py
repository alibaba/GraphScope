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

from abc import ABCMeta
from abc import abstractmethod

from gs_interactive_admin.core.config import Config


class InteractiveCluster(metaclass=ABCMeta):
    """
    An abstraction for the interactive cluster, built by K8sLauncher, and expose the common operations to start, stop.
    """

    def __init__(self):
        pass

    def __del__(self):
        self.stop()

    @abstractmethod
    def start(self):
        """
        Start the cluster.
        """
        pass

    @abstractmethod
    def stop(self):
        """
        Stop the cluster.
        """

    @abstractmethod
    def wait_pods_ready(self, timeout: int = 600):
        """
        Wait until the service is ready.
        """
        pass


class ILauncher(metaclass=ABCMeta):
    """
    Define the interface for the launcher, Which is used to launch new deployments.
    TODO: currently use graph_id as the unique identifier for the deployment,
    but it may be changed to a more general identifier in the future.
    """

    @abstractmethod
    def __init__(self, config: Config):
        """
        Initialize the launcher.
        """
        pass

    @abstractmethod
    def launch_cluster(self, graph_id: str, config: Config) -> InteractiveCluster:
        """
        Launch an interactive engine. return the cluster name.
        """
        pass

    @abstractmethod
    def update_cluster(self, graph_id: str, config: Config) -> bool:
        """
        Update the cluster. For example, increase or decrease the number of replicas.
        """
        pass

    @abstractmethod
    def delete_cluster(self, graph_id: str) -> bool:
        """
        Delete the cluster.
        """
        pass

    @abstractmethod
    def get_cluster_status(self, graph_id: str) -> str:
        """
        Get the status of the cluster.
        """
        pass

    @abstractmethod
    def get_all_clusters(self) -> list:
        """
        Get all the clusters.
        """
        pass
