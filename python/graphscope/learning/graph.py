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
from copy import deepcopy

try:
    import graphlearn
    from graphlearn import Graph as GLGraph
except ImportError:
    GLGraph = object

from graphscope.framework.errors import InvalidArgumentError
from graphscope.framework.errors import check_argument
from graphscope.proto import graph_def_pb2


class Graph(GLGraph):
    def __init__(self, graph, handle, config=None, object_id=None):
        """Initialize a graph for the learning engine using a handle."""
        self.graph = graph
        self.graphscope_session = self.graph._session

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

        for mask, node_label, nsplit, split_range in config["gen_labels"]:
            self.node_view(node_label, mask, nsplit=nsplit, split_range=split_range)

        self.init_vineyard(worker_index=0, worker_count=1)

    def decode_arg(self, arg):
        if arg is None or isinstance(arg, dict):
            return arg
        return json.loads(base64.b64decode(arg.encode("utf-8")).decode("utf-8"))

    def close(self):
        if self.closed or self.graphscope_session.closed:
            return
        self.closed = True
        super(Graph, self).close()  # close client first
        # close server instance
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
        handle_copy["config"] = self.config
        handle_copy["client_count"] = worker_count
        return base64.b64encode(json.dumps(handle_copy).encode("utf-8")).decode("utf-8")

    def V(
        self,
        t,
        feed=None,
        node_from=graphlearn.pywrap.NodeFrom.NODE,
        mask=graphlearn.python.utils.Mask.NONE,
    ):
        """Entry of GSL, starting from VERTEX.

        Args:
            t (string): The type of node which is the entry of query or the type
                of edge when node is from edge source or dst.
            feed (None| numpy.ndarray | types.GeneratorType | `Nodes`): When `feed`
                is not `None`, the `type` should be a node type, which means query the
                attributes of the specified node ids.

                - None: Default. Sample nodes with the following .shuffle and .batch API.
                  numpy.ndarray: Any shape of ids. Get nodes of the given ids and
                  node_type.
                - types.Generator: A generator of numpy.ndarray. Get nodes of generated
                  ids and given node_type.
                - `Nodes`: A `Nodes` object.
            node_from (NodeFrom): Default is `NodeFrom.NODE`, which means sample or
                or iterate node from node. `NodeFrom.EDGE_SRC` means sample or
                iterate node from source node of edge, and `NodeFrom.EDGE_DST` means
                sample or iterate node from destination node of edge. If node is from
                edge, the `type` must be an edge type.
            mask (NONE | TRAIN | TEST | VAL): The given node set is indexed by both the
                raw node type and mask value. The default mask value is NONE, which plays
                nothing on the index.
        """
        return super(Graph, self).V(t, feed, node_from, mask)

    def E(self, edge_type, feed=None, reverse=False):
        """Entry of GSL, starting from EDGE.

        Args:
            edge_type (string): The type of edge which is the entry of query.
            feed (None| (np.ndarray, np.ndarray) | types.GeneratorType | `Edges`):

                - None: Default. Sample edges with the following .shuffle and .batch API.
                  (np.ndarray, np.ndarray): src_ids, dst_ids. Get edges of the given
                  (src_ids, dst_ids) and given edge_type. src_ids and dst_ids must be
                  the same shape, dtype is int.
                - types.Generator: A generator of (numpy.ndarray, numpy.ndarray). Get
                  edges of generated (src_ids, dst_ids) and given edge_type.
                - `Edges`: An `Edges` object.
        """
        return super(Graph, self).E(edge_type, feed, reverse)


def get_gl_handle(schema, vineyard_id, engine_hosts, engine_config, fragments=None):
    """Dump a handler for GraphLearn for interaction.

    Fields in :code:`schema` are:

    + the name of node type or edge type
    + whether the graph is weighted graph
    + whether the graph is labeled graph
    + the number of int attributes
    + the number of float attributes
    + the number of string attributes

    An example of the graph handle:

    .. code:: python

        {
            "server": "127.0.0.1:8888,127.0.0.1:8889",
            "client_count": 1,
            "vineyard_socket": "/var/run/vineyard.sock",
            "vineyard_id": 13278328736,
            "fragments": [13278328736, ...],  # fragment ids
            "node_schema": [
                "user:false:false:10:0:0",
                "item:true:false:0:0:5"
            ],
            "edge_schema": [
                "user:click:item:true:false:0:0:0",
                "user:buy:item:true:true:0:0:0",
                "item:similar:item:false:false:10:0:0"
            ],
            "node_attribute_types": {
                "person": {
                    "age": "i",
                    "name": "s",
                },
            },
            "edge_attribute_types": {
                "knows": {
                    "weight": "f",
                },
            },
        }

    The handle can be decoded using:

    .. code:: python

       base64.b64decode(handle.encode('ascii')).decode('ascii')

    Note that the ports are selected from a range :code:`(8000, 9000)`.

    Args:
        schema: The graph schema.
        vineyard_id: The object id of graph stored in vineyard.
        engine_hosts: A list of hosts for GraphScope engine workers.
        engine_config: dict of config for GAE engine.

    Returns:
        str: Base64 encoded handle

    """

    def group_property_types(props):
        weighted, labeled, i, f, s, attr_types = "false", "false", 0, 0, 0, {}
        for prop in props:
            if prop.type in [graph_def_pb2.STRING]:
                s += 1
                attr_types[prop.name] = "s"
            elif prop.type in (graph_def_pb2.FLOAT, graph_def_pb2.DOUBLE):
                f += 1
                attr_types[prop.name] = "f"
            else:
                i += 1
                attr_types[prop.name] = "i"
            if prop.name == "weight":
                weighted = "true"
            elif prop.name == "label":
                labeled = "true"
        return weighted, labeled, i, f, s, attr_types

    node_schema, node_attribute_types = [], dict()
    for label in schema.vertex_labels:
        weighted, labeled, i, f, s, attr_types = group_property_types(
            schema.get_vertex_properties(label)
        )
        node_schema.append(
            "{}:{}:{}:{}:{}:{}".format(label, weighted, labeled, i, f, s)
        )
        node_attribute_types[label] = attr_types

    edge_schema, edge_attribute_types = [], dict()
    for label in schema.edge_labels:
        weighted, labeled, i, f, s, attr_types = group_property_types(
            schema.get_edge_properties(label)
        )
        for rel in schema.get_relationships(label):
            edge_schema.append(
                "{}:{}:{}:{}:{}:{}:{}:{}".format(
                    rel[0], label, rel[1], weighted, labeled, i, f, s
                )
            )
        edge_attribute_types[label] = attr_types

    engine_hosts = ",".join(engine_hosts)
    handle = {
        "hosts": engine_hosts,
        "client_count": 1,
        "vineyard_id": vineyard_id,
        "vineyard_socket": engine_config["vineyard_socket"],
        "node_schema": node_schema,
        "edge_schema": edge_schema,
        "node_attribute_types": node_attribute_types,
        "edge_attribute_types": edge_attribute_types,
        "fragments": fragments,
    }
    handle_json_string = json.dumps(handle)
    return base64.b64encode(handle_json_string.encode("utf-8")).decode("utf-8")
