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

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin.test import BaseTestCase
from gs_interactive_admin.core.metadata.kv_store import ETCDKeyValueStore


class TestEtcdKVStore(unittest.TestCase):
    def setup_class(self):
        # Read etcd server endpoint from environment variable
        if "ETCD_ENDPOINT" not in os.environ:
            raise Exception("ETCD_ENDPOINT is not set")
        self.etcd_endpoint = os.environ["ETCD_ENDPOINT"]
        host, port = self.etcd_endpoint.split(":")
        self.etcd_kv_store = ETCDKeyValueStore(host, int(port))
        self.etcd_kv_store.open()
        # config logging
        logging.basicConfig(level=logging.INFO)

    def test_insert(self):
        key = "test_key"
        value = "test_value"
        self.etcd_kv_store.insert(key, value)
        assert self.etcd_kv_store.get(key) == value
        # delete the key
        assert self.etcd_kv_store.delete(key)
        assert self.etcd_kv_store.get(key) is None

    def test_insert_without_key(self):
        value1 = "test_value"
        value2 = "test_value2"
        key1 = self.etcd_kv_store.insert_with_prefix("prefix", value1)
        key2 = self.etcd_kv_store.insert_with_prefix("prefix", value2)
        print("key1: ", key1)
        print("key2: ", key2)
        assert self.etcd_kv_store.get(key1) == value1
        assert self.etcd_kv_store.get(key2) == value2
        # delete the key
        assert self.etcd_kv_store.delete(key1)
        assert self.etcd_kv_store.get(key1) is None
        value3 = "test_value3"
        key3 = self.etcd_kv_store.insert_with_prefix("prefix", value3)
        assert self.etcd_kv_store.get(key3) == value3

        kv_tuples = self.etcd_kv_store.get_with_prefix("prefix")
        assert kv_tuples == [(key2, value2), (key3, value3)]

    def test_delete(self):
        key = "prefix/test_key"
        value = "test_value"
        self.etcd_kv_store.insert(key, value)
        assert self.etcd_kv_store.get(key) == value
        key2 = "prefix/test_key2"
        value2 = "test_value2"
        self.etcd_kv_store.insert(key2, value2)
        assert self.etcd_kv_store.get(key2) == value2

        self.etcd_kv_store.delete_with_prefix("prefix")
        assert self.etcd_kv_store.get(key) is None
        assert self.etcd_kv_store.get(key2) is None

    def test_update(self):
        key = "prefix/test_key"
        value = "test_value"
        self.etcd_kv_store.insert(key, value)
        assert self.etcd_kv_store.get(key) == value
        value2 = "test_value2"
        self.etcd_kv_store.update(key, value2)
        assert self.etcd_kv_store.get(key) == value2

        assert self.etcd_kv_store.update_with_func(key, lambda x: x + "2")
        assert self.etcd_kv_store.get(key) == value2 + "2"

        self.etcd_kv_store.delete(key)
        assert self.etcd_kv_store.get(key) is None

    def teardown_class(self):
        self.etcd_kv_store.delete_with_prefix("/")
        self.etcd_kv_store.close()
