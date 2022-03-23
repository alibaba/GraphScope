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

from graphscope import nx
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import patch_docstring

import_as_graphscope_nx(networkx.convert_matrix)


@patch_docstring(_from_pandas_edgelist)
def from_pandas_edgelist(
    df,
    source="source",
    target="target",
    edge_attr=None,
    create_using=None,
    edge_key=None,
):
    g = nx.empty_graph(0, create_using)

    if edge_attr is None:
        g.add_edges_from(zip(df[source], df[target]))
        return g

    reserved_columns = [source, target]

    # Additional columns requested
    attr_col_headings = []
    attribute_data = []
    if edge_attr is True:
        attr_col_headings = [c for c in df.columns if c not in reserved_columns]
    elif isinstance(edge_attr, (list, tuple)):
        attr_col_headings = edge_attr
    else:
        attr_col_headings = [edge_attr]
    if len(attr_col_headings) == 0:
        raise nx.NetworkXError(
            f"Invalid edge_attr argument: No columns found with name: {attr_col_headings}"
        )

    try:
        attribute_data = zip(*[df[col] for col in attr_col_headings])
    except (KeyError, TypeError) as e:
        msg = f"Invalid edge_attr argument: {edge_attr}"
        raise nx.NetworkXError(msg) from e

    if g.is_multigraph():
        # => append the edge keys from the df to the bundled data
        if edge_key is not None:
            try:
                multigraph_edge_keys = df[edge_key]
                attribute_data = zip(attribute_data, multigraph_edge_keys)
            except (KeyError, TypeError) as e:
                msg = f"Invalid edge_key argument: {edge_key}"
                raise nx.NetworkXError(msg) from e

        for s, t, attrs in zip(df[source], df[target], attribute_data):
            if edge_key is not None:
                attrs, multigraph_edge_key = attrs
                key = g.add_edge(s, t, key=multigraph_edge_key)
            else:
                key = g.add_edge(s, t)

            g[s][t][key].update(zip(attr_col_headings, attrs))
    else:
        edges = []
        for s, t, attrs in zip(df[source], df[target], attribute_data):
            edges.append((s, t, zip(attr_col_headings, attrs)))
        g.add_edges_from(edges)

    return g
