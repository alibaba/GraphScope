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

import datetime
import logging
import random
from concurrent.futures import ThreadPoolExecutor

from gremlin_python.driver.client import Client
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from graphscope.framework.loader import Loader

logger = logging.getLogger("graphscope")


class InteractiveQuery(object):
    """`InteractiveQuery` class, is a simple wrapper around
    `Gremlin-Python <https://pypi.org/project/gremlinpython/>`_,
    which implements Gremlin within the Python language.
    It also can expose gremlin endpoint which can be used by
    any other standard gremlin console, with the method `graph_url()`.

    It also has a method called `subgraph` which can extract some fragments
    from origin graph, produce a new, smaller but concise graph stored in vineyard,
    which lifetime is independent from the origin graph.

    User can either use `execute()` to submit a script, or use `traversal_source()`
    to get a `GraphTraversalSource` for further traversal.
    """

    def __init__(self, graphscope_session, object_id, front_ip, front_port):
        self._graphscope_session = graphscope_session
        self._object_id = object_id
        self._graph_url = "ws://%s:%d/gremlin" % (front_ip, front_port)
        self._client = Client(self._graph_url, "g")
        self._closed = False

    @property
    def object_id(self):
        """Get the vineyard object id of graph.

        Returns:
            str: object id
        """
        return self._object_id

    @property
    def graph_url(self):
        """The gremlin graph url can be used with any standard gremlin console, e.g., thinkerpop."""
        return self._graph_url

    def closed(self):
        """Return if the current instance is closed."""
        return self._closed

    def subgraph(self, gremlin_script):
        """Create a subgraph, which input is the result of the execution of `gremlin_script`.
        Any gremlin script that will output a set of edges can be used to contruct a subgraph.
        Args:
            gremlin_script (str): gremlin script to be executed

        Raises:
            RuntimeError: If the interactive instance is closed.

        Returns:
            :class:`Graph`: constructed subgraph. which is also stored in vineyard.
        """
        if self.closed():
            raise RuntimeError("Interactive query is closed.")

        now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_num = random.randint(0, 10000000)
        graph_name = "%s_%s" % (str(now_time), str(random_num))

        # create graph handle by name
        self._client.submit(
            "g.createGraph('%s').with('graphType', 'vineyard')" % graph_name
        ).all().result()

        # start a thread to launch the graph
        def load_subgraph(name):
            import vineyard

            host, port = self._graphscope_session.info["engine_config"][
                "vineyard_rpc_endpoint"
            ].split(":")
            client = vineyard.connect(host, int(port))

            # get vertex/edge stream id
            vstream = client.get_name("__%s_vertex_stream" % name, True)
            estream = client.get_name("__%s_edge_stream" % name, True)

            # invoke load_from
            g = self._graphscope_session.load_from(
                edges=[Loader(estream)],
                vertices=[Loader(vstream)],
                generate_eid=False,
            )
            client.put_name(vineyard.ObjectID(g.vineyard_id), graph_name)
            logger.info("subgraph has been loaded")
            return g

        pool = ThreadPoolExecutor()
        subgraph_task = pool.submit(load_subgraph, (graph_name,))

        # add subgraph vertices and edges
        subgraph_script = "%s.subgraph('%s').outputVineyard('%s')" % (
            gremlin_script,
            graph_name,
            graph_name,
        )
        self._client.submit(subgraph_script).all().result()

        return subgraph_task.result()

    def execute(self, query):
        """Execute gremlin querying scripts.
        Behind the scene, it uses `gremlinpython` to send the query.

        Args:
            query (str): Scripts that written in gremlin quering language.

        Raises:
            RuntimeError: If the interactive script is closed

        Returns:
            execution results
        """
        if self.closed():
            raise RuntimeError("Interactive query is closed.")
        return self._client.submit(query)

    def traversal_source(self):
        """Create a GraphTraversalSource and return.
        Once `g` has been created using a connection, we can start to write
        Gremlin traversals to query the remote graph.

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
        return traversal().withRemote(DriverRemoteConnection(self._graph_url, "g"))

    def close(self):
        """Close interactive instance and release resources"""
        if not self.closed():
            self._closed = True
            self._graphscope_session._close_interactive_instance(self)
