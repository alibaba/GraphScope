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

""" Manage connections of the GraphScope store service.
"""

import grpc
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from graphscope.framework.graph_schema import GraphSchema
from graphscope.proto import ddl_service_pb2
from graphscope.proto import ddl_service_pb2_grpc


class Graph:
    def __init__(self, graph_def, conn=None) -> None:
        self._schema = GraphSchema()
        self._schema.from_graph_def(graph_def)
        self._conn = conn
        self._schema._conn = conn

    def schema(self):
        return self._schema


class Connection:
    def __init__(self, addr, gremlin_endpoint=None) -> None:
        self._addr = addr
        self._gremlin_endpoint = gremlin_endpoint
        channel = grpc.insecure_channel(addr)
        self._stub = ddl_service_pb2_grpc.ClientDdlStub(channel)

    def submit(self, requests):
        return self._stub.batchSubmit(requests)

    def get_graph_def(self, requests):
        return self._stub.getGraphDef(requests)

    def g(self):
        request = ddl_service_pb2.GetGraphDefRequest()
        graph_def = self.get_graph_def(request).graph_def
        graph = Graph(graph_def, self)
        return graph

    def gremlin(self):
        graph_url = "ws://%s/gremlin" % self._gremlin_endpoint
        return traversal().withRemote(DriverRemoteConnection(graph_url, "g"))


def conn(addr, gremlin_endpoint=None):
    return Connection(addr, gremlin_endpoint)
