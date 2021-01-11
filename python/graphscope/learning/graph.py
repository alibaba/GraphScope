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

import base64
import collections
import json

try:
    from graphlearn import Graph as GLGraph
except ImportError:
    GLGraph = object

from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument


class Graph(GLGraph):
    def __init__(self, handle, config=None, object_id=None, graphscope_session=None):
        """Initialize a graph for the learning engine using a handle."""
        handle = self.decode_arg(handle)
        config = self.decode_arg(config)

        if config is None:
            if "config" in handle:
                config = handle["config"]
        if config is None:
            config = collections.defaultdict(lambda: dict)

        if object_id is None:
            object_id = handle["vineyard_id"]

        self.handle = handle
        self.config = config
        self.object_id = object_id
        self.closed = False
        self.graphscope_session = graphscope_session
        super(Graph, self).__init__()

        self.vineyard(handle, config["nodes"], config["edges"])
        for label, node_attr in config["node_attributes"].items():
            n_ints, n_floats, n_strings = (
                node_attr[1][0],
                node_attr[1][1],
                node_attr[1][2],
            )
            self.node_attributes(label, node_attr[0], n_ints, n_floats, n_strings)
        for label, edge_attr in config["edge_attributes"].items():
            n_ints, n_floats, n_strings = (
                edge_attr[1][0],
                edge_attr[1][1],
                edge_attr[1][2],
            )
            self.edge_attributes(label, edge_attr[0], n_ints, n_floats, n_strings)

        for node_view_label, node_label, nsplit, split_range in config["gen_labels"]:
            self.node_view(
                node_view_label, node_label, nsplit=nsplit, split_range=split_range
            )

        self.init_vineyard(worker_index=0, worker_count=1)

    def decode_arg(self, arg):
        if arg is None or isinstance(arg, dict):
            return arg
        return json.loads(base64.b64decode(arg.encode("utf-8")).decode("utf-8"))

    def close(self):
        if not self.closed:
            self.closed = True
            super(Graph, self).close()  # close client first
            # close server instance
            if self.graphscope_session is not None:
                self.graphscope_session._close_learning_instance(self)

    @staticmethod  # noqa: C901
    def preprocess_args(handle, nodes, edges, gen_labels):  # noqa: C901
        handle = json.loads(base64.b64decode(handle).decode("utf-8", errors="ignore"))
        node_names = []
        node_attributes = {}
        edge_names = []
        edge_attributes = {}

        def selected_property_schema(attr_types, attributes):
            prop_counts = collections.defaultdict(lambda: 0)
            for attr in attributes:
                prop_counts[attr_types[attr]] += 1
            return [prop_counts["i"], prop_counts["f"], prop_counts["s"]]

        if nodes is not None:
            for node in nodes:
                if isinstance(node, str):
                    if node in node_names:
                        raise InvalidArgumentError("Duplicate node type: %s" % node)
                    node_names.append(node)
                elif isinstance(node, tuple):
                    if node[0] in node_names:
                        raise InvalidArgumentError("Duplicate node type: %s" % node[0])
                    node_names.append(node[0])
                    attr_types = handle["node_attribute_types"][node[0]]
                    attr_schema = selected_property_schema(attr_types, node[1])
                    node_attributes[node[0]] = (node[1], attr_schema)
                else:
                    raise InvalidArgumentError(
                        "The node parameter is in bad format: %s" % node
                    )
        else:
            for node in handle["node_schema"]:
                node_names.append(node.split(":")[0])

        if edges is not None:
            for edge in edges:
                if isinstance(edge, str):
                    if len(node_names) > 1:
                        raise InvalidArgumentError(
                            "Cannot inference edge type when multiple kinds of nodes exists"
                        )
                    edge_names.append((node_names[0], edge, node_names[0]))
                elif (
                    isinstance(edge, tuple)
                    and isinstance(edge[0], str)
                    and isinstance(edge[1], str)
                ):
                    edge_names.append(edge)
                elif (
                    isinstance(edge, tuple)
                    and isinstance(edge[0], str)
                    and isinstance(edge[1], list)
                ):
                    if len(node_names) > 1:
                        raise InvalidArgumentError(
                            "Cannot inference edge type when multiple kinds of nodes exists"
                        )
                    edge_names.append((node_names[0], edge[0], node_names[0]))
                    attr_types = handle["edge_attribute_types"][edge[0]]
                    attr_schema = selected_property_schema(attr_types, edge[1])
                    edge_attributes[edge[0]] = (edge[1], attr_schema)
                elif (
                    isinstance(edge, tuple)
                    and isinstance(edge[0], (list, tuple))
                    and isinstance(edge[1], list)
                ):
                    edge_names.append(edge[0])
                    attr_types = handle["edge_attribute_types"][edge[0][1]]
                    attr_schema = selected_property_schema(attr_types, edge[1])
                    edge_attributes[edge[0][1]] = (edge[1], attr_schema)
                else:
                    raise InvalidArgumentError(
                        "The edge parameter is in bad format: %s" % edge
                    )

        split_groups = collections.defaultdict(list)
        if gen_labels is not None:
            for label in gen_labels:
                if len(label) == 3 or len(label) == 4:
                    split_groups[label[1]].append(label)
                else:
                    raise InvalidArgumentError(
                        "Bad gen_labels arguments: %s" % gen_labels
                    )

        split_labels = []
        for label, group in split_groups.items():
            lengths = [len(split) for split in group]
            check_argument(
                lengths[:-1] == lengths[1:], "Invalid gen labels: %s" % group
            )
            if len(group[0]) == 3:
                length_sum = sum(split[2] for split in group)
                s, ss = 0, []
                for split in group:
                    ss.append((s, s + split[2]))
                    s += split[2]
                group = [
                    (split[0], split[1], length_sum, s) for split, s in zip(group, ss)
                ]
            for split in group:
                split_labels.append(split)

        return {
            "nodes": node_names if node_names else None,
            "edges": edge_names if edge_names else None,
            "node_attributes": node_attributes,
            "edge_attributes": edge_attributes,
            "gen_labels": split_labels,
        }

    def get_handle(self, worker_count=1):
        """Return a base64-encoded handle for distributed training."""
        handle_copy = self.handle.copy()
        handle_copy["client_count"] = worker_count
        return base64.b64encode(json.dumps(handle_copy).encode("utf-8")).decode("utf-8")

    def V(self, t, feed=None):
        """Entry of Gremlin-like query. Start from node.

        Args:
        t (string): The type of node which is the entry of query or the type
            of edge when node is from edge source or dst.
        feed (None| numpy.ndarray | types.GeneratorType | `Nodes`): When `feed`
            is not `None`, the `type` should be a node type, which means query the
            attributes of the specified node ids.
            None: Default. Sample nodes with the following .shuffle and .batch API.
            numpy.ndarray: Any shape of ids. Get nodes of the given ids and
            node_type.
            types.Generator: A generator of numpy.ndarray. Get nodes of generated
            ids and given node_type.
            `Nodes`: A `Nodes` object.

        Return:
        A 'Query' object.

        Example:

        .. code:: python

            >>> import numpy as np
            >>> g.V("user").shuffle().batch(64)
            >>> g.V("user", feed=np.array([1, 2, 3]))
            >>> def gen():
            >>>   while True:
            >>>     yield  np.array([1, 2, 3])
            >>> gen = gen()
            >>> g.V("user", feed=gen)
        """
        return super(Graph, self).V(t, feed)

    def E(self, edge_type, feed=None, reverse=False):
        """Entry of Gremlin-like query. Start from edge.

        Args:
            edge_type (string): The type of edge which is the entry of query.
            feed (None| (np.ndarray, np.ndarray) | types.GeneratorType | `Edges`):
                None: Default. Sample edges with the following .shuffle and .batch API.
                (np.ndarray, np.ndarray): src_ids, dst_ids. Get edges of the given
                (src_ids, dst_ids) and given edge_type. src_ids and dst_ids must be
                the same shape, dtype is int.
                types.Generator: A generator of (numpy.ndarray, numpy.ndarray). Get
                edges of generated (src_ids, dst_ids) and given edge_type.
                `Edges`: An `Edges` object.

        Return:
            A 'Query' object.

        Example:

        .. code:: python

            >>> import numpy as np
            >>> g.E("buy").shuffle().batch(64)
            >>> g.E("buy", feed=(np.array([1, 2, 3]), np.array([4, 5, 6]))
            >>> def gen():
            >>>   while True:
            >>>     yield  (np.array([1, 2, 3]), np.array([4, 5, 6]))
            >>> gen = gen()
            >>> g.E("buy", feed=gen)
        """
        return super(Graph, self).E(edge_type, feed, reverse)
