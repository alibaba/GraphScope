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
from copy import deepcopy
from enum import Enum

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from graphscope.framework.dag import DAGNode
from graphscope.framework.dag_utils import fetch_gremlin_result
from graphscope.framework.dag_utils import gremlin_query
from graphscope.framework.dag_utils import gremlin_to_subgraph

logger = logging.getLogger("graphscope")


class InteractiveQueryStatus(Enum):
    """Enumeration class of current status of InteractiveQuery"""

    Initializing = 0
    Running = 1
    Failed = 2
    Closed = 3


class ResultSetDAGNode(DAGNode):
    """A class represents a result set node in a DAG.

    This is a wrapper for :class:`gremlin_python.driver.resultset.ResultSet`,
    and you can get the result by :method:`one()` or :method:`all()`.
    """

    def __init__(self, interactive, op):
        self._session = interactive.session
        self._op = op
        # add op to dag
        self._session.dag.add_op(self._op)

    def one(self):
        """See details in :method:`gremlin_python.driver.resultset.ResultSet.one`"""
        # avoid circular import
        from graphscope.framework.context import ResultDAGNode

        op = fetch_gremlin_result(self, "one")
        return ResultDAGNode(self, op)

    def all(self):
        """See details in :method:`gremlin_python.driver.resultset.ResultSet.all`

        Note that this method is equal to `ResultSet.all().result()`
        """
        # avoid circular import
        from graphscope.framework.context import ResultDAGNode

        op = fetch_gremlin_result(self, "all")
        return ResultDAGNode(self, op)


class ResultSet(object):
    def __init__(self, result_set_node):
        self._result_set_node = result_set_node
        self._session = self._result_set_node.session
        # copy and set op evaluated
        self._result_set_node.op = deepcopy(self._result_set_node.op)
        self._result_set_node.evaluated = True
        self._session.dag.add_op(self._result_set_node.op)

    def one(self):
        return self._session._wrapper(self._result_set_node.one())

    def all(self):
        return self._session._wrapper(self._result_set_node.all())


class InteractiveQuery(object):
    """`InteractiveQuery` class, is a simple wrapper around
    `Gremlin-Python <https://pypi.org/project/gremlinpython/>`_,
    which implements Gremlin within the Python language.
    It also can expose gremlin endpoint which can be used by
    any other standard gremlin console, with the method `graph_url()`.

    It also has a method called `subgraph` which can extract some fragments
    from origin graph, produce a new, smaller but concise graph stored in vineyard,
    which lifetime is independently of the origin graph.

    User can either use `execute()` to submit a script, or use `traversal_source()`
    to get a `GraphTraversalSource` for further traversal.
    """

    def __init__(self, graph, frontend_endpoint):
        """Construct a :class:`InteractiveQuery` object."""
        self._conn = None
        # graph object id stored in vineyard
        self._graph = graph
        self._session = graph._session
        frontend_endpoint = frontend_endpoint.split(",")
        self._graph_url = [f"ws://{endpoint}/gremlin" for endpoint in frontend_endpoint]
        self.closed = False

    @property
    def graph_url(self):
        """The gremlin graph url can be used with any standard gremlin console, e.g., tinkerpop."""
        return self._graph_url

    @property
    def object_id(self):
        return self._graph.vineyard_id

    @property
    def session(self):
        return self._session

    @property
    def session_id(self):
        return self._session.session_id

    def execute(self, query, request_options=None):
        """Execute gremlin querying scripts.

        Args:
            query (str): Scripts that written in gremlin quering language.
            request_options (dict, optional): Gremlin request options. format:
            {
                "engine": "gae"
            }

        Returns:
            :class:`graphscope.framework.context.ResultDAGNode`:
                A result holds the gremlin result, evaluated in eager mode.
        """
        op = gremlin_query(self, query, request_options)
        return self._session._wrapper(ResultSetDAGNode(self, op))

    def submit(self, query, request_options=None):
        return self.execute(query, request_options)

    def subgraph(self, gremlin_script, request_options=None):
        """Create a subgraph, which input is the result of the execution of `gremlin_script`.

        Any gremlin script that output a set of edges can be used to contruct a subgraph.

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
        """Create a GraphTraversalSource and return.
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
            self._conn = DriverRemoteConnection(self._graph_url[0], "g")
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
        self._session._close_interactive_instance(self)
        self.closed = True
