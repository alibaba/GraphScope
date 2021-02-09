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


class LibMeta(object):
    def __init__(self, key, type, lib_path):
        self.key = key
        self.type = type
        self.lib_path = lib_path


class GraphMeta(object):
    def __init__(self, key, vineyard_id, schema, schema_path=None):
        self.key = key
        self.type = "graph"
        self.vineyard_id = vineyard_id
        self.schema = schema
        self.schema_path = schema_path


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
