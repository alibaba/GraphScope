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


from collections import UserDict
from collections.abc import Mapping

__all__ = ["NodeDict", "AdjListDict"]


class NodeDict(Mapping):
    """A adhoc read-only view for nodes on graphscope.nx Graph."""

    __slots__ = (
        "_graph",
        "_cache",
    )

    def __init__(self, graph):
        self._graph = graph
        self._cache = graph.cache

    def __contains__(self, key):
        return key in self._cache or key in self._graph

    def update(self, nodes_to_update):
        # N.B: for forward use networkx.relabel_nodes
        self._graph.add_nodes_from(nodes_to_update)
        self._cache.clear_node_attr_cache()

    def __len__(self):
        return len(self._cache)

    def __getitem__(self, key):
        if key in self._cache and self._cache.align_node_attr_cache():
            index = self._cache.id2i[key]
            return NodeAttrDict(self._graph, key, self._cache.node_attr_cache[index])
        if key in self._graph:
            attr = self._cache.get_node_attr(key)
            return NodeAttrDict(self._graph, key, attr)
        raise KeyError(key)

    def __iter__(self):
        return iter(self._cache)

    def __eq__(self, other):
        if not isinstance(other, Mapping):
            return NotImplementedError

        def item_equal(a, b):
            for key in a:
                if key not in b or a[key] != b[key]:
                    return False
            return True

        return len(self) == len(other) and item_equal(self, other)

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def __str__(self):
        return self.__repr__()


class NodeAttrDict(UserDict):
    """Wrapper for attributes of node."""

    __slots__ = (
        "_graph",
        "_key",
        "data",
    )

    def __init__(self, graph, node, data):
        self._graph = graph
        self._node = node
        self.data = data

    def copy(self):
        return self.data.copy()

    def __setitem__(self, key, item):
        super().__setitem__(key, item)
        self._graph.set_node_data(self._node, self.data)

    def __delitem__(self, key):
        super().__delitem__(key)
        self._graph.set_node_data(self._node, self.data)

    def clear(self):
        super().clear()
        self._graph.set_node_data(self._node, self.data)

    def update(self, data=None, **kwargs):
        super().update(data, **kwargs)
        self._graph.set_node_data(self._node, self.data)

    def pop(self, key, *args):
        val = super().pop(key, *args)
        self._graph.set_node_data(self._node, self.data)
        return val

    def popitem(self):
        item = super().popitem()
        self._graph.set_node_data(self._node, self.data)
        return item


class AdjListDict(Mapping):
    """A adhoc read-only view for adjlist of nodes on graphscope.nx Graph."""

    __slots__ = ("_graph", "_cache", "_pred")

    def __init__(self, graph, pred=False):
        self._graph = graph
        self._cache = graph.cache
        self._pred = pred

    def __contains__(self, key):
        return key in self._cache or key in self._graph

    def __len__(self):
        return len(self._cache)

    def __getitem__(self, key):
        if key in self._cache and self._cache.align_neighbor_cache(self._pred):
            # the node attribute already cached in iteration
            index = self._cache.id2i[key]
            neighbor = (
                self._cache.pred_cache[index]
                if self._pred
                else self._cache.succ_cache[index]
            )
            return NeighborDict(self._graph, key, neighbor, self._pred)
        if key in self._graph:
            # LRU cache
            nbr = (
                self._cache.get_predecessors(key)
                if self._pred
                else self._cache.get_successors(key)
            )
            return NeighborDict(self._graph, key, nbr, self._pred)
        raise KeyError(key)

    def __iter__(self):
        return iter(self._cache)

    def __eq__(self, other):
        if not isinstance(other, Mapping):
            return NotImplementedError

        def item_equal(a, b):
            for key in a:
                if key not in b or a[key] != b[key]:
                    return False
            return True

        return len(self) == len(other) and item_equal(self, other)

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def __str__(self):
        return f"{self.__class__.__name__}"


class NeighborDict(Mapping):
    """A adhoc read-only view for neighbors of nodes on graphscope.nx Graph."""

    __slots__ = (
        "_graph",
        "_cache",
        "_node",
        "_nbr_list",
        "_pred",
        "_nbr2i",
        "_nbr_attrs",
    )

    def __init__(self, graph, node, nbr_list, pred=False):
        self._graph = graph
        self._cache = graph.cache
        self._node = node
        self._nbr_list = nbr_list
        self._pred = pred
        self._nbr2i = None
        self._nbr_attrs = None

    def __repr__(self):
        return f"{self.__class__.__name__} node {self._node} nbr_list{self._nbr_list} nbr_attrs {self._nbr_attrs}"

    def __str__(self):
        return self.__repr__()

    def __contains__(self, key):
        if self._nbr2i is None:
            self._nbr2i = {k: v for v, k in enumerate(self._nbr_list)}
        return key in self._nbr2i

    def __len__(self):
        return len(self._nbr_list)

    def __getitem__(self, key):
        if key in self:
            if self._node in self._cache and self._cache.align_neighbor_attr_cache(
                self._pred
            ):
                # the neighbors already cached in iteration
                if self._nbr_attrs is None:
                    index = self._cache.id2i[self._node]
                    self._nbr_attrs = (
                        self._cache.pred_attr_cache[index]
                        if self._pred
                        else self._cache.succ_attr_cache[index]
                    )
            else:
                # LRU cache
                self._nbr_attrs = (
                    self._cache.get_pred_attr(self._node)
                    if self._pred
                    else self._cache.get_succ_attr(self._node)
                )
            return NeighborAttrDict(
                self._graph, self._node, key, self._nbr_attrs[self._nbr2i[key]]
            )

        raise KeyError(key)

    def __iter__(self):
        yield from self._nbr_list

    def keys(self):
        return self._nbr_list

    def __eq__(self, other):
        if not isinstance(other, Mapping):
            return NotImplementedError

        def item_equal(a, b):
            for key in a:
                if key not in b or a[key] != b[key]:
                    return False
            return True

        return len(self) == len(other) and item_equal(self, other)

    def copy(self):
        return self


class NeighborAttrDict(UserDict):
    """Wrapper for attributes of edge."""

    __slots__ = ("_graph", "_cache", "_u", "_v", "data")

    def __init__(self, graph, u, v, data):
        self._graph = graph
        self._u = u
        self._v = v
        self.data = data

    def copy(self):
        return self.data.copy()

    def __setitem__(self, key, item):
        super().__setitem__(key, item)
        self._graph.set_edge_data(self._u, self._v, self.data)

    def __delitem__(self, key):
        super().__delitem__(key)
        self._graph.set_edge_data(self._u, self._v, self.data)

    def clear(self):
        super().clear()
        self._graph.set_edge_data(self._u, self._v, self.data)

    def update(self, data=None, **kwargs):
        super().update(data, **kwargs)
        self._graph.set_edge_data(self._u, self._v, self.data)

    def pop(self, key, *args):
        val = super().pop(key, *args)
        self._graph.set_edge_data(self._u, self._v, self.data)
        return val

    def popitem(self):
        item = super().popitem()
        self._graph.set_edge_data(self._u, self._v, self.data)
        return item
