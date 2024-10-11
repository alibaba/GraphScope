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

import os

from gremlin_python.driver.client import Client
from neo4j import GraphDatabase
from neo4j import Session as Neo4jSession

from gs_interactive.client.session import DefaultSession
from gs_interactive.client.session import Session


class Driver:
    """
    The main entry point for the Interactive SDK. With the Interactive endpoints provided,
    you can create a Interactive Session to interact with the Interactive service,
    and create a Neo4j Session to interact with the Neo4j service.
    """

    def __init__(
        self,
        admin_endpoint: str = None,
        stored_proc_endpoint: str = None,
        cypher_endpoint: str = None,
        gremlin_endpoint: str = None,
    ):
        """
        Construct a new driver using the specified endpoints.
        If no endpoints are provided, the driver will read them from environment variables.
        You will receive the endpoints after starting the Interactive service.

        Args:
            admin_endpoint: the endpoint for the admin service.
            stored_proc_endpoint (str, optional): the endpoint for the stored procedure service.
            cypher_endpoint (str, optional): the endpoint for the cypher service.
        """
        if admin_endpoint is None:
            self.read_endpoints_from_env()
        else:
            self._admin_endpoint = admin_endpoint
            self._stored_proc_endpoint = stored_proc_endpoint
            self._cypher_endpoint = cypher_endpoint
            self._gremlin_endpoint = gremlin_endpoint
        self._session = None
        self.init_host_and_port()
        self._neo4j_driver = None

    def close(self):
        """
        Close the driver and release resources.
        """
        if self._neo4j_driver is not None:
            self._neo4j_driver.close()

    def __del__(self):
        self.close()

    def init_host_and_port(self):
        # prepend http:// to self._admin_endpoint
        if not self._admin_endpoint.startswith("http://"):
            raise ValueError("Invalid uri, expected format is http://host:port")
        host_and_port = self._admin_endpoint[7:]
        splitted = host_and_port.split(":")
        if len(splitted) != 2:
            raise ValueError("Invalid uri, expected format is host:port")
        self._host = splitted[0]
        self._port = int(splitted[1])

    def read_endpoints_from_env(self):
        """
        Construct a new driver from the endpoints declared in environment variables.
        INTERACTIVE_ADMIN_ENDPOINT: http://host:port
        INTERACTIVE_STORED_PROC_ENDPOINT: http://host:port
        INTERACTIVE_CYPHER_ENDPOINT: neo4j://host:port or bolt://host:port
        """
        self._admin_endpoint = os.environ.get("INTERACTIVE_ADMIN_ENDPOINT")
        assert (
            self._admin_endpoint is not None
        ), "INTERACTIVE_ADMIN_ENDPOINT is not set, "
        "did you forget to export the environment variable after deploying Interactive?"
        " see https://graphscope.io/docs/latest/flex/interactive/installation"
        self._stored_proc_endpoint = os.environ.get("INTERACTIVE_STORED_PROC_ENDPOINT")
        if self._stored_proc_endpoint is None:
            print(
                "INTERACTIVE_STORED_PROC_ENDPOINT is not set,"
                "will try to get it from service status endpoint"
            )
        self._cypher_endpoint = os.environ.get("INTERACTIVE_CYPHER_ENDPOINT")
        if self._cypher_endpoint is None:
            print(
                "INTERACTIVE_CYPHER_ENDPOINT is not set,"
                "will try to get it from service status endpoint"
            )
        self._gremlin_endpoint = os.environ.get("INTERACTIVE_GREMLIN_ENDPOINT")
        if self._gremlin_endpoint is None:
            print(
                "INTERACTIVE_GREMLIN_ENDPOINT is not set,"
                "will try to get it from service status endpoint"
            )

    def session(self) -> Session:
        """
        Create a session with the specified endpoints.
        """
        if self._stored_proc_endpoint is not None:
            return DefaultSession(self._admin_endpoint, self._stored_proc_endpoint)
        return DefaultSession(self._admin_endpoint)

    def getDefaultSession(self) -> Session:
        """
        Get the default session.
        """
        if self._session is None:
            self._session = self.session()
        return self._session

    def getNeo4jSession(self, **config) -> Neo4jSession:
        """
        Create a neo4j session with the specified endpoints.
        Args:
            config: a dictionary of configuration options, the same as the ones
                in neo4j.Driver.session
        """
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
        if self._cypher_endpoint is None:
            self._cypher_endpoint = self.getNeo4jEndpoint()
        if self._neo4j_driver is None:
            self._neo4j_driver = GraphDatabase.driver(self._cypher_endpoint, auth=None)
        return self._neo4j_driver.session(**config)

    def getNeo4jEndpoint(self) -> str:
        """
        Get the bolt endpoint from the service status endpoint.
        Only works if the sdk was running in the same pod as the service.
        """
        service_status = self.getDefaultSession().get_service_status()
        if service_status.is_ok():
            bolt_port = service_status.get_value().bolt_port
            return "bolt://" + self._host + ":" + str(bolt_port)
        else:
            raise ValueError(
                "Failed to get service status " + service_status.get_status_message()
            )

    def getGremlinClientImpl(self):
        if self._gremlin_endpoint is None:
            self._gremlin_endpoint = self.getGremlinEndpoint()
        graph_url = "ws://" + self._gremlin_endpoint + "/gremlin"
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
