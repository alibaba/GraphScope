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

import sys

from gremlin_python import statics
from gremlin_python.driver.client import Client
from gremlin_python.driver.driver_remote_connection import \
    DriverRemoteConnection
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.structure.graph import Graph
from neo4j import GraphDatabase
from neo4j import Session as Neo4jSession

from gs_interactive.client.session import DefaultSession, Session


class Driver:
    def __init__(self, endpoint: str):
        # split uri into host and port
        self._endpoint = endpoint
        # prepend http:// to self._endpoint
        if not self._endpoint.startswith("http://"):
            raise ValueError("Invalid uri, expected format is http://host:port")
        host_and_port = self._endpoint[7:]
        splitted = host_and_port.split(":")
        if len(splitted) != 2:
            raise ValueError("Invalid uri, expected format is host:port")
        self._host = splitted[0]
        self._port = int(splitted[1])
        self._session = None

    def session(self) -> Session:
        return DefaultSession(self._endpoint)

    def getDefaultSession(self) -> Session:
        if self._session is None:
            self._session = self.session()
        return self._session

    def getNeo4jSession(self, **config) -> Neo4jSession:
        return self.getNeo4jSessionImpl(**config)

    def getGremlinClient(self) -> str:
        return self.getGremlinClientImpl()

    @property
    def get_host(self) -> str:
        return self._host

    @property
    def get_port(self) -> int:
        return self._port

    def getNeo4jSessionImpl(self, **config) -> Neo4jSession:
        endpoint = self.getNeo4jEndpoint()
        return GraphDatabase.driver(endpoint, auth=None).session(**config)

    def getNeo4jEndpoint(self) -> str:
        service_status = self.getDefaultSession().get_service_status()
        if service_status.is_ok():
            bolt_port = service_status.get_value().bolt_port
            return "bolt://" + self._host + ":" + str(bolt_port)
        else:
            raise ValueError(
                "Failed to get service status " + service_status.get_status_message()
            )

    def getGremlinClientImpl(self):
        gremlin_endpoint = self.getGremlinEndpoint()
        graph_url = "ws://" + gremlin_endpoint + "/gremlin"
        return Client(graph_url, "g")

    def getGremlinEndpoint(self):
        service_status = self.getDefaultSession().get_service_status()
        if service_status.is_ok():
            gremlin_port = service_status.get_value().gremlin_port
            return self._host + ":" + str(gremlin_port)
        else:
            raise ValueError(
                "Failed to get service status " + service_status.get_status_message()
            )
