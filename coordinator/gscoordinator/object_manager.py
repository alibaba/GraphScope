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
from gremlin_python.driver.serializer import GraphSONMessageSerializer

"""Patch for gremlin_python serializer to support "gae" processor
"""


def patch_for_gremlin_python():
    def get_processor(self, processor):
        if processor == "gae":
            return getattr(self, "standard", None)
        processor = getattr(self, processor, None)
        if not processor:
            raise Exception("Unknown processor")
        return processor

    setattr(GraphSONMessageSerializer, "get_processor", get_processor)


patch_for_gremlin_python()


class LibMeta(object):
    def __init__(self, key, type, lib_path):
        self.key = key
        self.type = type
        self.lib_path = lib_path


class GraphMeta(object):
    def __init__(self, key, vineyard_id, graph_def, schema_path=None, op_key=None):
        self.key = key
        self.type = "graph"
        self.vineyard_id = vineyard_id
        self.graph_def = graph_def
        self.schema_path = schema_path
        self.op_key = op_key


class InteractiveQueryManager(object):
    def __init__(self, key, frontend_endpoint, object_id):
        self.key = key
        self.type = "gie_manager"
        self.frontend_endpoint = frontend_endpoint
        # graph object id in vineyard
        self.object_id = object_id
        self.graph_url = "ws://{0}/gremlin".format(self.frontend_endpoint)
        self.client = Client(self.graph_url, "g")
        self.closed = False

    def submit(self, message, bindings=None, request_options=None):
        if request_options is not None and "engine" in request_options:
            from gremlin_python.driver import request

            rm = request.RequestMessage(
                # {"engine": "gae"} support only
                processor=request_options["engine"],
                op="eval",
                args={
                    "gremlin": message,
                    "aliases": {"g": self.client.traversal_source},
                },
            )
            print("[DEBUG] RequestMessage is: ", rm)
            return self.client.submit(rm)
        return self.client.submit(message, bindings, request_options)


class GremlinResultSet(object):
    def __init__(self, key, request_options, result_set):
        self.key = key
        self.type = "result_set"
        self.request_options = request_options
        self.result_set = result_set

    def query_on_gae_processor(self):
        if self.request_options is not None and "engine" in self.request_options:
            return "gae" == self.request_options["engine"]
        return False


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
