import time
import unittest
import pytest

from flask import json
import os
import logging
import etcd3

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin.test import BaseTestCase
from gs_interactive_admin.core.launcher.k8s_launcher import K8sLauncher

from gs_interactive_admin.core.config import Config

logger = logging.getLogger("interactive")


class TestLaunchK8sCluster(unittest.TestCase):
    """AdminServiceJobManagementController integration test stubs"""

    def setup_class(self):
        self._config = Config()
        self._k8s_launcher = K8sLauncher(self._config)
        self._config.master.instance_name = "test"
        self._config.master.k8s_launcher_config.namespace = "default"
        self._config.master.k8s_launcher_config.default_replicas = 1
        self._cluster = None

    def test_launch_cluster(self):
        self._cluster = self._k8s_launcher.launch_cluster(
            "test", self._config, wait_service_ready=False
        )
        max_wait = 60
        while max_wait > 0:
            if self._cluster.is_ready():
                break
            time.sleep(1)
            max_wait -= 1

        assert self._cluster.is_ready()
        logger.info("Cluster is ready")

        instance_id = self._cluster.instance_id
        logger.info(f"instance_id {instance_id}")

        # Now stop the cluster
        self._cluster.stop()
        logger.info("Cluster is stopped")

        # Check from k8s_launcher
        clusters = self._k8s_launcher.get_cluster_status(instance_id)
        assert clusters is None

    def teardown_class(self):
        # if self._cluster:
        # self._cluster.stop()
        pass


if __name__ == "__main__":
    unittest.main()
