#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file convert_matrix.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/convert_matrix.py
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

import networkx.convert_matrix
from networkx.convert_matrix import from_pandas_edgelist as _from_pandas_edgelist

from graphscope.experimental import nx
from graphscope.experimental.nx.utils.compat import import_as_graphscope_nx
from graphscope.experimental.nx.utils.compat import patch_docstring

import_as_graphscope_nx(networkx.convert_matrix)


@patch_docstring(_from_pandas_edgelist)
def from_pandas_edgelist(
    df, source="source", target="target", edge_attr=None, create_using=None
):
    g = nx.empty_graph(0, create_using)

    if edge_attr is None:
        g.add_edges_from(zip(df[source], df[target]))
        return g

    # Additional columns requested
    if edge_attr is True:
        cols = [c for c in df.columns if c is not source and c is not target]
    elif isinstance(edge_attr, (list, tuple)):
        cols = edge_attr
    else:
        cols = [edge_attr]
    if len(cols) == 0:
        msg = f"Invalid edge_attr argument. No columns found with name: {cols}"
        raise nx.NetworkXError(msg)

    try:
        eattrs = zip(*[df[col] for col in cols])
    except (KeyError, TypeError) as e:
        msg = f"Invalid edge_attr argument: {edge_attr}"
        raise nx.NetworkXError(msg) from e

    edges = []
    for s, t, attrs in zip(df[source], df[target], eattrs):
        edges.append((s, t, zip(cols, attrs)))
    g.add_edges_from(edges)
    return g
