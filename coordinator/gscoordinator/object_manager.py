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

from gremlin_python.driver.client import Client


class LibMeta(object):
    def __init__(self, key, type, lib_path):
        self.key = key
        self.type = type
        self.lib_path = lib_path


class GraphMeta(object):
    def __init__(self, key, vineyard_id, graph_def, schema_path=None):
        self.key = key
        self.type = "graph"
        self.vineyard_id = vineyard_id
        self.graph_def = graph_def
        self.schema_path = schema_path


class InteractiveQueryManager(object):
    def __init__(self, key, frontend_endpoint, object_id):
        self.key = key
        self.type = "gie_manager"
        # graph object id in vineyard
        self.object_id = object_id
        self.graph_url = f"ws://{frontend_endpoint}/gremlin"
        self.client = Client(self.graph_url, "g")
        self.closed = False

    def submit(self, message, bindings=None, request_options=None):
        return self.client.submit(message, bindings, request_options)

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass
        self.closed = True


class GremlinResultSet(object):
    def __init__(self, key, result_set):
        self.key = key
        self.type = "result_set"
        self.result_set = result_set


class LearningInstanceManager(object):
    def __init__(self, key, object_id):
        self.key = key
        self.type = "gle_manager"
        self.object_id = object_id
        self.closed = False


class ObjectManager(object):
    """Manage the objects hold by the coordinator."""

    def __init__(self):
        self._objects = {}

    def put(self, key, obj):
        self._objects[key] = obj

    def get(self, key):
        return self._objects.get(key)

    def pop(self, key):
        return self._objects.pop(key, None)

    def keys(self):
        return self._objects.keys()

    def clear(self):
        self._objects.clear()

    def __contains__(self, key):
        return key in self._objects
