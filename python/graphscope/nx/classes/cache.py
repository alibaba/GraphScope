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

import concurrent.futures
import io
from functools import lru_cache

import msgpack

try:
    import simdjson as json
except ImportError:
    try:
        import ujson as json
    except ImportError:
        import json

from graphscope.framework import dag_utils
from graphscope.nx.utils.misc import clear_mutation_cache
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2

__all__ = ["Cache"]


class Cache:
    """A adhoc cache for graphscope.nx Graph.
    The Cache is consists of two kind of cache: the iteration batch cache for
    __iter__ and the LRU cache for cache miss.
    """

    def __init__(self, graph):
        self._graph = graph

        # the iteration caches for graph data
        self.node_id_cache = ()
        self.node_attr_cache = ()
        self.succ_cache = ()
        self.succ_attr_cache = ()
        self.pred_cache = ()
        self.pred_attr_cache = ()

        # status for iteration batch cache
        self._len = 0
        self.id2i = {}
        self.enable_iter_cache = False
        self.iter_gid = 0
        self.iter_pre_gid = 0
        self.node_attr_align = False
        self.succ_align = False
        self.succ_attr_align = False
        self.pred_align = False
        self.pred_attr_align = False

        # thread pool and promises for iteration batch cache fetch
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.futures = {
            "node_id": None,
            "node_attr": None,
            "succ": None,
            "succ_attr": None,
            "pred": None,
            "pred_attr": None,
        }

    def warmup(self):
        """Warm up the iteration cache."""
        self._len = self._graph.number_of_nodes()
        if self._len > 1000:
            # avoid much small graphs to compete thread resource
            self.enable_iter_cache = True
            self._async_fetch_node_id_cache(0)
            self._async_fetch_succ_cache(0)
            self._async_fetch_node_attr_cache(0)
            self._async_fetch_succ_attr_cache(0)

    # LRU Caches
    @lru_cache(1000000)
    def get_node_attr(self, n):
        return get_node_data(self._graph, n)

    @lru_cache(1000000)
    def get_successors(self, n):
        return get_neighbors(self._graph, n)

    @lru_cache(1000000)
    def get_succ_attr(self, n):
        return get_neighbors_attr(self._graph, n)

    @lru_cache(1000000)
    def get_predecessors(self, n):
        return get_neighbors(self._graph, n, pred=True)

    @lru_cache(1000000)
    def get_pred_attr(self, n):
        return get_neighbors_attr(self._graph, n, pred=True)

    def align_node_attr_cache(self):
        """Check and align the node attr cache with node id cache"""
        if self.enable_iter_cache and self.node_attr_align is False:
            if self.futures["node_attr"] is not None:
                start_gid, self.node_attr_cache = self.futures["node_attr"].result()
                if start_gid == self.iter_pre_gid:
                    # align to current node_id_cache
                    if self.iter_gid != self.iter_pre_gid:
                        self._async_fetch_node_attr_cache(self.iter_gid)
                    self.node_attr_align = True
                else:
                    # not align to current node_id_cache, should fetch again
                    self._async_fetch_node_attr_cache(self.iter_pre_gid)
        return self.node_attr_align

    def align_succ_cache(self):
        """Check and align the succ neighbor cache with node id cache"""
        if self.enable_iter_cache and self.succ_align is False:
            start_gid, self.succ_cache = self.futures["succ"].result()
            if start_gid == self.iter_pre_gid:
                if self.iter_gid != self.iter_pre_gid:
                    self._async_fetch_succ_cache(self.iter_gid)
                self.succ_align = True
            else:
                self._async_fetch_succ_cache(self.iter_pre_gid)

        return self.succ_align

    def align_succ_attr_cache(self):
        """Check and align the succ neighbor attr cache with node id cache"""
        if self.enable_iter_cache and self.succ_attr_align is False:
            if self.futures["succ_attr"] is not None:
                start_gid, self.succ_attr_cache = self.futures["succ_attr"].result()
                if start_gid == self.iter_pre_gid:
                    if self.iter_gid != self.iter_pre_gid:
                        self._async_fetch_succ_attr_cache(self.iter_gid)
                    self.succ_attr_align = True
                else:
                    self._async_fetch_succ_attr_cache(self.iter_pre_gid)

        return self.succ_attr_align

    def align_pred_cache(self):
        """Check and align the pred neighbor cache with node id cache"""
        if self.enable_iter_cache and self.pred_align is False:
            if self.futures["pred"] is None:
                self._async_fetch_pred_cache(self.iter_pre_gid)
            start_gid, self.pred_cache = self.futures["pred"].result()
            if start_gid == self.iter_pre_gid:
                if self.iter_gid != self.iter_pre_gid:
                    self._async_fetch_pred_cache(self.iter_gid)
                self.pred_align = True
            else:
                print("pred not align", start_gid, self.iter_pre_gid)
                self._async_fetch_pred_cache(self.iter_pre_gid)

        return self.pred_align

    def align_pred_attr_cache(self):
        """Check and align the pred neighbor attr cache with node id cache"""
        if self.enable_iter_cache and self.pred_attr_align is False:
            if self.futures["pred_attr"] is None:
                self._async_fetch_pred_attr_cache(self.iter_pre_gid)
            start_gid, self.pred_attr_cache = self.futures["pred_attr"].result()
            if start_gid == self.iter_pre_gid:
                if self.iter_gid != self.iter_pre_gid:
                    self._async_fetch_pred_attr_cache(self.iter_gid)
                self.pred_attr_align = True
            else:
                self._async_fetch_pred_attr_cache(self.iter_pre_gid)

        return self.pred_attr_align

    def align_neighbor_cache(self, pred=False):
        return self.align_pred_cache() if pred else self.align_succ_cache()

    def align_neighbor_attr_cache(self, pred=True):
        return self.align_pred_attr_cache() if pred else self.align_succ_attr_cache()

    @clear_mutation_cache
    def __contains__(self, key):
        if self.enable_iter_cache:
            if len(self.node_id_cache) == 0 and self.futures["node_id"] is not None:
                self.iter_pre_gid = self.iter_gid
                self.iter_gid, node_size, self.node_id_cache = self.futures[
                    "node_id"
                ].result()
                self.futures["node_id"] = None
                if self.iter_gid != self.iter_pre_gid:
                    self._async_fetch_node_id_cache(self.iter_gid)

            if not self.id2i and self.node_id_cache:
                # initialize the id to index hash map
                self.id2i = {k: v for v, k in enumerate(self.node_id_cache)}
        return key in self.id2i

    @clear_mutation_cache
    def __len__(self):
        return self._len

    @clear_mutation_cache
    def __iter__(self):
        iter_n = 0
        while True:
            if iter_n >= self._len:
                break
            if iter_n == 0 and len(self.node_id_cache) > 0:
                iter_n += len(self.node_id_cache)
            else:
                self.iter_pre_gid = self.iter_gid
                if self.enable_iter_cache:
                    self.iter_gid, node_size, self.node_id_cache = self.futures[
                        "node_id"
                    ].result()
                    if self.iter_gid != self.iter_pre_gid:
                        self._async_fetch_node_id_cache(self.iter_gid)
                else:
                    (
                        self.iter_gid,
                        node_size,
                        self.node_id_cache,
                    ) = self._get_node_id_cache(self.iter_gid)
                iter_n += node_size
                self.id2i.clear()
                self.node_attr_align = False
                self.succ_align = False
                self.succ_attr_align = False
                self.pred_align = False
                self.pred_attr_align = False
            yield from self.node_id_cache

    def shutdown(self):
        for _, future in self.futures.items():
            if future is not None:
                future.cancel()

        for _, future in self.futures.items():
            if future is not None:
                try:
                    future.result()
                except concurrent.futures.CancelledError:
                    pass
                future = None

    def shutdown_executor(self):
        self.executor.shutdown(wait=True)

    def clear(self):
        """Clear batch cache and lru cache, reset the status and warmup again"""
        if self.enable_iter_cache:
            self.shutdown()
        self.enable_iter_cache = False
        self.iter_gid = 0
        self.iter_pre_gid = 0
        self.id2i.clear()
        self.node_id_cache = ()
        self.node_attr_cache = ()
        self.succ_cache = ()
        self.succ_attr_cache = ()
        self.pred_cache = ()
        self.pred_attr_cache = ()
        self.node_attr_align = (
            self.succ_align
        ) = self.succ_attr_align = self.pred_align = self.pred_attr_align = False
        self.get_node_attr.cache_clear()
        self.get_successors.cache_clear()
        self.get_succ_attr.cache_clear()
        self.get_predecessors.cache_clear()
        self.get_pred_attr.cache_clear()
        self.warmup()

    def clear_node_attr_cache(self):
        """Clear the node attr cache"""
        if self.futures["node_attr"] is not None:
            self.futures["node_attr"].cancel()
        if self.futures["node_attr"] is not None:
            try:
                self.futures["node_attr"].result()
            except concurrent.futures.CancelledError:
                pass
        self.futures["node_attr"] = None
        self.node_attr_cache = ()
        self.get_node_attr.cache_clear()
        self.node_attr_align = False

    def clear_neighbor_attr_cache(self):
        """Clear the neighbor attr cache"""
        if self.futures["succ_attr"] is not None:
            self.futures["succ_attr"].cancel()
        if self.futures["pred_attr"] is not None:
            self.futures["pred_attr"].cancel()
        if self.futures["succ_attr"] is not None:
            try:
                self.futures["succ_attr"].result()
            except concurrent.futures.CancelledError:
                pass
        if self.futures["pred_attr"] is not None:
            try:
                self.futures["pred_attr"].result()
            except concurrent.futures.CancelledError:
                pass
        self.futures["succ_attr"] = None
        self.futures["pred_attr"] = None
        self.succ_attr_cache = ()
        self.pred_attr_cache = ()
        self.get_succ_attr.cache_clear()
        self.get_pred_attr.cache_clear()
        self.succ_attr_align = False
        self.pred_attr_align = False

    def _async_fetch_node_id_cache(self, gid):
        self.futures["node_id"] = self.executor.submit(self._get_node_id_cache, gid)

    def _async_fetch_node_attr_cache(self, gid):
        self.futures["node_attr"] = self.executor.submit(self._get_node_attr_cache, gid)

    def _async_fetch_succ_cache(self, gid):
        self.futures["succ"] = self.executor.submit(self._get_succ_cache, gid)

    def _async_fetch_pred_cache(self, gid):
        self.futures["pred"] = self.executor.submit(self._get_pred_cache, gid)

    def _async_fetch_succ_attr_cache(self, gid):
        self.futures["succ_attr"] = self.executor.submit(self._get_succ_attr_cache, gid)

    def _async_fetch_pred_attr_cache(self, gid):
        self.futures["pred_attr"] = self.executor.submit(self._get_pred_attr_cache, gid)

    def _get_node_id_cache(self, gid):
        op = dag_utils.report_graph(
            self._graph, types_pb2.NODE_ID_CACHE_BY_GID, gid=gid
        )
        archive = op.eval()
        gid = archive.get_uint64()
        node_size = archive.get_uint32()
        fp = io.BytesIO(archive.get_bytes())
        node_array = msgpack.load(fp, use_list=False)
        return gid, node_size, node_array

    def _get_node_attr_cache(self, gid):
        op = dag_utils.report_graph(
            self._graph, types_pb2.NODE_ATTR_CACHE_BY_GID, gid=gid
        )
        archive = op.eval()
        gid = archive.get_uint64()
        fp = io.BytesIO(archive.get_bytes())
        node_attr_cache = msgpack.load(fp, use_list=False)
        return gid, node_attr_cache

    def _get_succ_cache(self, gid):
        op = dag_utils.report_graph(self._graph, types_pb2.SUCC_BY_GID, gid=gid)
        archive = op.eval()
        gid = archive.get_uint64()
        fp = io.BytesIO(archive.get_bytes())
        succ_cache = msgpack.load(fp, use_list=False)
        return gid, succ_cache

    def _get_pred_cache(self, gid):
        op = dag_utils.report_graph(self._graph, types_pb2.PRED_BY_GID, gid=gid)
        archive = op.eval()
        gid = archive.get_uint64()
        fp = io.BytesIO(archive.get_bytes())
        pred_cache = msgpack.load(fp, use_list=False)
        return gid, pred_cache

    def _get_succ_attr_cache(self, gid):
        op = dag_utils.report_graph(self._graph, types_pb2.SUCC_ATTR_BY_GID, gid=gid)
        archive = op.eval()
        gid = archive.get_uint64()
        fp = io.BytesIO(archive.get_bytes())
        succ_attr_cache = msgpack.load(fp, use_list=False)
        return gid, succ_attr_cache

    def _get_pred_attr_cache(self, gid):
        op = dag_utils.report_graph(self._graph, types_pb2.PRED_ATTR_BY_GID, gid=gid)
        archive = op.eval()
        gid = archive.get_uint64()
        fp = io.BytesIO(archive.get_bytes())
        pred_attr_cache = msgpack.load(fp, use_list=False)
        return gid, pred_attr_cache


def get_neighbors(graph, n, pred=False):
    """Get the neighbors of node in graph.

    Parameters
    ----------
    graph:
        the graph to query.
    n: node
        the node to get neighbors.
    report_type:
        the report type of report graph operation,
            types_pb2.SUCCS_BY_NODE: get the successors of node,
            types_pb2.PREDS_BY_NODE: get the predecessors of node,
    """
    if graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
        n = graph._convert_to_label_id_tuple(n)
    report_t = types_pb2.PREDS_BY_NODE if pred else types_pb2.SUCCS_BY_NODE
    op = dag_utils.report_graph(
        graph, report_t, node=json.dumps(n).encode("utf-8", errors="ignore")
    )
    archive = op.eval()
    return msgpack.unpackb(archive.get_bytes(), use_list=False)


def get_neighbors_attr(graph, n, pred=False):
    """Get the neighbors attr of node in graph.

    Parameters
    ----------
    graph:
        the graph to query.
    n: node
        the node to get neighbors.
    report_type:
        the report type of report graph operation,
            types_pb2.SUCC_ATTR_BY_NODE: get the successors attr of node,
            types_pb2.PRED_ATTR_BY_NODE: get the predecessors attr of node,

    Returns
    -------
    attr: tuple
    """
    if graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
        n = graph._convert_to_label_id_tuple(n)
    report_t = types_pb2.PRED_ATTR_BY_NODE if pred else types_pb2.SUCC_ATTR_BY_NODE
    op = dag_utils.report_graph(
        graph, report_t, node=json.dumps(n).encode("utf-8", errors="ignore")
    )
    archive = op.eval()
    return json.loads(archive.get_bytes())


def get_node_data(graph, n):
    """Returns the attribute dictionary of node n.

    This is identical to `G[n]`.

    Parameters
    ----------
    n : nodes

    Returns
    -------
    node_dict : dictionary
        The node attribute dictionary.

    Examples
    --------
    >>> G = nx.path_graph(4)  # or DiGraph etc
    >>> G[0]
    {}

    Warning: Assigning to `G[n]` is not permitted.
    But it is safe to assign attributes `G[n]['foo']`

    >>> G[0]['weight'] = 7
    >>> G[0]['weight']
    7

    >>> G = nx.path_graph(4)  # or DiGraph etc
    >>> G.get_node_data(0, 1)
    {}

    """
    if graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
        n = graph._convert_to_label_id_tuple(n)
    op = dag_utils.report_graph(
        graph,
        types_pb2.NODE_DATA,
        node=json.dumps(n).encode("utf-8", errors="ignore"),
    )
    archive = op.eval()
    return msgpack.loads(archive.get_bytes(), use_list=False)
