#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file reportviews.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/classes/reportviews.py
#
# which has the following license:
#
# Copyright (C) 2004-2020, NetworkX Developers
# Aric Hagberg <hagberg@lanl.gov>
# Dan Schult <dschult@colgate.edu>
# Pieter Swart <swart@lanl.gov>
# All rights reserved.
#
# This file is part of NetworkX.
#
# NetworkX is distributed under a BSD license; see LICENSE.txt for more
# information.
#

from networkx.classes.reportviews import EdgeDataView as _EdgeDataView
from networkx.classes.reportviews import EdgeView as _EdgeView
from networkx.classes.reportviews import InEdgeDataView as _InEdgeDataView
from networkx.classes.reportviews import InEdgeView as _InEdgeView
from networkx.classes.reportviews import NodeView as _NodeView
from networkx.classes.reportviews import OutEdgeDataView as _OutEdgeDataView
from networkx.classes.reportviews import OutEdgeView as _OutEdgeView

from graphscope.nx.utils.compat import patch_docstring


# NodeViews
@patch_docstring(_NodeView)
class NodeView(_NodeView):
    __slots__ = (
        "_graph",
        "_nodes",
    )

    def __getstate__(self):
        return {"_graph": self._graph, "_nodes": self._nodes}

    def __setstate__(self, state):
        self._graph = state["_graph"]
        self._nodes = state["_nodes"]

    def __init__(self, graph):
        self._graph = graph
        self._nodes = graph._node

    # Mapping methods
    def __len__(self):
        return self._graph.number_of_nodes()

    def __iter__(self):
        return iter(self._nodes)


# EdgeDataViews
@patch_docstring(_OutEdgeDataView)
class OutEdgeDataView(_OutEdgeDataView):
    def __len__(self):
        if self._nbunch:
            return sum(len(nbrs) for n, nbrs in self._nodes_nbrs())
        return self._viewer._graph.number_of_edges()


@patch_docstring(_EdgeDataView)
class EdgeDataView(_EdgeDataView):
    def __len__(self):
        if self._nbunch:
            return sum(1 for e in self)
        return self._viewer._graph.number_of_edges()


@patch_docstring(_InEdgeDataView)
class InEdgeDataView(_InEdgeDataView):
    def __len__(self):
        if self._nbunch:
            return sum(len(nbrs) for n, nbrs in self._nodes_nbrs())
        return self._viewer._graph.number_of_edges()


@patch_docstring(_OutEdgeView)
class OutEdgeView(_OutEdgeView):
    dataview = OutEdgeDataView

    # Set methods
    def __len__(self):
        return self._graph.number_of_edges()


@patch_docstring(_EdgeView)
class EdgeView(_EdgeView):
    __slots__ = ()

    dataview = EdgeDataView

    # Set methods
    def __len__(self):
        return self._graph.number_of_edges()


@patch_docstring(_InEdgeView)
class InEdgeView(_InEdgeView):
    dataview = InEdgeDataView

    # Set methods
    def __len__(self):
        return self._graph.number_of_edges()

    def __contains__(self, e):
        return self._graph.has_edge(*e)
