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

import logging
from enum import Enum

from gremlin_python.driver.client import Client
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from graphscope.framework.dag_utils import gremlin_to_subgraph
from graphscope.framework.utils import deprecated

logger = logging.getLogger("graphscope")


class InteractiveQueryStatus(Enum):
    """Enumeration class of current status of InteractiveQuery"""

    Initializing = 0
    Running = 1
    Failed = 2
    Closed = 3


class InteractiveQuery(object):
    """`InteractiveQuery` class, is a simple wrapper around
    `Gremlin-Python <https://pypi.org/project/gremlinpython/>`_,
    which implements Gremlin within the Python language.
    It also can expose gremlin endpoint which can be used by
    any other standard gremlin console, with the method `gremlin_url()`.

    It also has a method called `subgraph` which can extract some fragments
    from origin graph, produce a new, smaller but concise graph stored in vineyard,
    which lifetime is independently of the origin graph.

    User can either use `execute()` to submit a script, or use `traversal_source()`
    to get a `GraphTraversalSource` for further traversal.
    """

    def __init__(self, graph, frontend_gremlin_endpoint, frontend_cypher_endpoint):
        """Construct a :class:`InteractiveQuery` object."""
        # graph object id stored in vineyard
        self._graph = graph
        self._session = graph._session
        self._gremlin_url = [
            f"ws://{endpoint}/gremlin"
            for endpoint in frontend_gremlin_endpoint.split(",")
        ]
        self._cypher_url = [
            f"neo4j://{endpoint}" for endpoint in frontend_cypher_endpoint.split(",")
        ]
        self._conn = None
        self._gremlin_client = None
        self._cypher_driver = None
        self.closed = False

    @property
    @deprecated("Please use `gremlin_url` instead")
    def graph_url(self):
        """This will be deprecated in the future, use `gremlin_url` instead."""
        return self._gremlin_url

    @property
    def gremlin_url(self):
        """The gremlin graph url can be used with any standard gremlin console,
        e.g., tinkerpop."""
        return self._gremlin_url

    @property
    def cypher_url(self):
        """The cypher graph url can be used with any standard cypher console,
        e.g., neo4j."""
        return self._cypher_url

    @property
    def object_id(self):
        return self._graph.vineyard_id

    @property
    def session(self):
        return self._session

    @property
    def session_id(self):
        return self._session.session_id

    def execute(self, query, lang="gremlin", request_options=None, **kwargs):
        """A simple wrapper around `submit`, for compatibility"""
        return self.submit(query, lang, request_options=request_options, **kwargs)

    def submit(self, query, lang="gremlin", request_options=None, **kwargs):
        from neo4j import RoutingControl

        """Execute gremlin or cypher querying scripts.

        <Gremlin>
        Args:
            query (str): Scripts that written in gremlin query language.
            request_options (dict, optional): Gremlin request options. format:
            {
                "engine": "gae"
            }

        Returns:
            :class:`gremlin_python.driver.client.ResultSet`:

        <Cypher>
        Args:
            query (str): Scripts that written in cypher query language.
            kwargs (dict, optional): Cypher request options. e.g.:
            routing_ = RoutingControl.READ

        Returns:
            :class:`neo4j.work.result.Result`:
        """
        if lang == "gremlin":
            return self.gremlin_client.submit(query, request_options=request_options)
        elif lang == "cypher":
            return self.cypher_driver.execute_query(
                query, routing_=RoutingControl.READ, **kwargs
            )
        else:
            raise ValueError(
                f"Unsupported query language: {lang} other than gremlin and cypher"
            )

    @property
    def gremlin_client(self):
        if self._gremlin_client is None:
            self._gremlin_client = Client(self._gremlin_url[0], "g")
        return self._gremlin_client

    @property
    def cypher_driver(self):
        from neo4j import GraphDatabase

        if self._cypher_driver is None:
            self._cypher_driver = GraphDatabase.driver(
                self._cypher_url[0], auth=("", "")
            )
        return self._cypher_driver

    def subgraph(self, gremlin_script, lang="gremlin", request_options=None):
        """We currently only support subgraph using gremlin script.

        Create a subgraph, which input is the executor result of `gremlin_script`.

        Any gremlin script that output a set of edges can be used to construct a subgraph.

        Args:
            gremlin_script (str): Gremlin script to be executed.
            request_options (dict, optional): Gremlin request options. format:
            {
                "engine": "gae"
            }

        Returns:
            :class:`graphscope.framework.graph.GraphDAGNode`:
                A new graph constructed by the gremlin output, that also stored in vineyard.
        """
        assert lang == "gremlin", "Only support gremlin script"
        # avoid circular import
        from graphscope.framework.graph import GraphDAGNode

        op = gremlin_to_subgraph(
            self,
            gremlin_script=gremlin_script,
            request_options=request_options,
            oid_type=self._graph._oid_type,
        )
        return self._session._wrapper(GraphDAGNode(self._session, op))

    def traversal_source(self):
        """We currently only support traversal_source using gremlin.

        Create a GraphTraversalSource and return.
        Once `g` has been created using a connection, we can start to write
        Gremlin traversals to query the remote graph.

        Raises:
            RuntimeError: If the interactive script is not running.

        Examples:

            .. code:: python

                sess = graphscope.session()
                graph = load_modern_graph(sess, modern_graph_data_dir)
                interactive = sess.gremlin(graph)
                g = interactive.traversal_source()
                print(g.V().both()[1:3].toList())
                print(g.V().both().name.toList())

        Returns:
            `GraphTraversalSource`
        """
        if self._conn is None:
            self._conn = DriverRemoteConnection(self._gremlin_url[0], "g")
        return traversal().withRemote(self._conn)

    def close(self):
        if self.closed or self._session.closed:
            return
        """Close interactive instance and release resources"""
        if self._conn is not None:
            try:
                self._conn.close()
            except:  # noqa: E722
                pass
        if self._gremlin_client is not None:
            try:
                self._gremlin_client.close()
            except:  # noqa: E722
                pass
        if self._cypher_driver is not None:
            try:
                self._cypher_driver.close()
            except:  # noqa: E722
                pass
        self._session._close_interactive_instance(self)
        self.closed = True
