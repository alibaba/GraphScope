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

from collections.abc import MutableMapping

from graphscope.proto import types_pb2

__all__ = ["NodeDict", "AdjDict"]


class NodeDict(MutableMapping):
    __slots__ = "_graph"

    def __init__(self, graph):
        self._graph = graph

    def __len__(self):
        return self._graph.number_of_nodes()

    def __getitem__(self, key):
        if key not in self._graph:
            raise KeyError(key)
        return NodeAttrDict(self._graph, key)

    def __setitem__(self, key, value):
        if key not in self._graph:
            self._graph.add_nodes_from([key, value])
        else:
            self._graph.set_node_data(key, value)

    def __delitem__(self, key):
        self._graph.remove_node(key)

    def __iter__(self):
        # batch get nodes
        node_num = self.__len__()
        count = 0
        pos = (0, 0)  # start iterate from the (worker:0, lid:0) node.
        while count < node_num:
            while True:
                ret = self._graph.batch_get_node(pos)
                pos = ret["next"]
                if ret["status"] is True:
                    break
            batch = ret["batch"]
            count += len(batch)
            for node in batch:
                yield node["id"]


class NodeAttrDict(MutableMapping):
    __slots__ = ("_graph", "_node", "mapping")

    def __init__(self, graph, node, data=None):
        self._graph = graph
        self._node = node
        if data:
            self.mapping = data
        else:
            self.mapping = graph.get_node_data(node)

    def __len__(self):
        return len(self.mapping)

    def __getitem__(self, key):
        return self.mapping[key]

    def __setitem__(self, key, value):
        self.mapping[key] = value
        self._graph.set_node_data(self._node, self.mapping)

    def __delitem__(self, key):
        del self.mapping[key]
        self._graph.set_node_data(self._node, self.mapping)

    def __iter__(self):
        return iter(self.mapping)

    def copy(self):
        return self.mapping

    def __repr__(self):
        return self.mapping


# NB: implement the dict structure to reuse the views of networkx. since we
# fetch the neighbor messages of node one by one, it's slower than reimplemented
# views.
# NB: maybe we can reimpl keysview, valuesview and itemsview for the AdjDict.
class AdjDict(MutableMapping):
    __slots__ = ("_graph", "_rtype")

    def __init__(self, graph, rtype=types_pb2.SUCCS_BY_NODE):
        self._graph = graph
        self._rtype = rtype

    def __len__(self):
        return self._graph.number_of_nodes()

    def __getitem__(self, key):
        if key not in self._graph:
            raise KeyError(key)
        return AdjInnerDict(self._graph, key, self._rtype)

    def __setitem__(self, key, value):
        raise NotImplementedError("hard to implement this method in rpc way")

    def __delitem__(self, key):
        # NB: not really identical to del g._adj[key]
        self._graph.remove_node(key)

    def __iter__(self):
        # batch get nodes
        node_num = self.__len__()
        count = 0
        pos = (0, 0)  # start iterate from the (worker:0, lid:0) node.
        while count < node_num:
            while True:
                ret = self._graph.batch_get_node(pos)
                pos = ret["next"]
                if ret["status"] is True:
                    break
            batch = ret["batch"]
            count += len(batch)
            for node in batch:
                yield node["id"]

    def __repr__(self):
        return f"{type(self).__name__}"


class AdjInnerDict(MutableMapping):
    __slots__ = ("_graph", "_node", "_type", "mapping")

    def __init__(self, graph, node, rtype):
        self._graph = graph
        self._node = node
        self._type = rtype
        self.mapping = graph.get_nbrs(node, rtype)

    def __len__(self):
        return len(self.mapping)

    def __getitem__(self, key):
        if key not in self.mapping:
            raise KeyError(key)
        return AdjEdgeAttrDict(self._graph, self._node, key, self.mapping[key])

    def __setitem__(self, key, value):
        if key in self.mapping:
            self._graph.set_edge_data(self._node, key, value)
        else:
            self._graph.add_edges_from([(self._node, key, value)])
        self.mapping[key] = value

    def __delitem__(self, key):
        if key in self.mapping:
            del self.mapping[key]
            self._graph.remove_edge(self._node, key)

    def __iter__(self):
        return iter(self.mapping)

    def __str__(self):
        return str(self.mapping)

    def __repr__(self):
        return f"{type(self).__name__}({self.mapping})"


class AdjEdgeAttrDict(MutableMapping):
    __slots__ = ("_graph", "_node", "_nbr", "mapping")

    def __init__(self, graph, node, nbr, data):
        self._graph = graph
        self._node = node
        self._nbr = nbr
        self.mapping = data

    def __len__(self):
        return len(self.mapping)

    def __getitem__(self, key):
        return self.mapping[key]

    def __setitem__(self, key, value):
        self.mapping[key] = value
        self._graph.set_edge_data(self._node, self._nbr, self.mapping)

    def __delitem__(self, key):
        if key in self.mapping:
            del self.mapping[key]
            self._graph.set_edge_data(self._node, self._nbr, self.mapping)

    def __iter__(self):
        return iter(self.mapping)

    def copy(self):
        return self.mapping

    def __str__(self):
        return str(self.mapping)

    def __repr__(self):
        return f"{type(self).__name__}({self.mapping})"
