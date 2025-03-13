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
import unittest

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from flask import json
import logging
import threading
import time
import etcd3

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin.core.service_discovery.service_registry import (
    EtcdServiceRegistry,
)
from gs_interactive_admin.core.service_discovery.service_registry import EtcdKeyHelper
from gs_interactive_admin.core.service_discovery.service_registry import ServiceInstance


class TestServiceDiscovery(unittest.TestCase):
    def setup_class(self):
        # Read etcd server endpoint from environment variable
        if "ETCD_ENDPOINT" not in os.environ:
            raise Exception("ETCD_ENDPOINT is not set")
        self.etcd_endpoint = os.environ["ETCD_ENDPOINT"]
        host, port = self.etcd_endpoint.split(":")
        self.etcd_client = etcd3.client(host=host, port=int(port))
        self.etcd_key_helper = EtcdKeyHelper()
        self.registry = EtcdServiceRegistry(host, int(port))
        # config logging
        logging.basicConfig(level=logging.INFO)

    def test_discover(self):
        mock_endpoint = "11.12.234.38:7687"
        mock_metrics = '{"endpoint": "11.12.234.38:7687", "service_name": "cypher","snapshot_id": "0"}'

        def mock_service():
            """
            Mock the service registration and delete.
            """
            service_instance_list_prefix = (
                self.etcd_key_helper.service_instance_list_prefix("0", "cypher")
            )
            primary_key = self.etcd_key_helper.service_primary_key("0", "cypher")
            service_key = service_instance_list_prefix + "/" + mock_endpoint
            self.etcd_client.put(service_key, mock_metrics)
            self.etcd_client.put(primary_key, mock_metrics)
            logging.info(
                "Mock service registered, key: %s, value: %s", service_key, mock_metrics
            )

        self.registry.start()
        pre_registry = self.registry.discover("0", "cypher")
        assert pre_registry is None
        # mock_service()
        t = threading.Thread(target=mock_service)
        t.start()
        time.sleep(2)
        post_registry = self.registry.discover("0", "cypher")
        assert post_registry is not None
        assert (
            post_registry.is_valid()
            and post_registry.get_primary_instance()
            == ServiceInstance(mock_endpoint, mock_metrics)
        )
        self.registry.stop()
        t.join()

    def teardown_class(self):
        self.registry.stop()
        self.etcd_client.delete_prefix("/")
        self.etcd_client.close()
        logging.info("Clean up etcd.")
