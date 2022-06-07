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

from collections import defaultdict

import networkx.convert_matrix

from graphscope import nx
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import patch_docstring

import_as_graphscope_nx(networkx.convert_matrix)


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


def to_numpy_array(
    G,
    nodelist=None,
    dtype=None,
    order=None,
    multigraph_weight=sum,
    weight="weight",
    nonedge=0.0,
):
    import numpy as np

    if nodelist is None:
        nodelist = list(G)
        nodeset = G
        nlen = len(G)
    else:
        nlen = len(nodelist)
        nodeset = set(G.nbunch_iter(nodelist))
        if nlen != len(nodeset):
            for n in nodelist:
                if n not in G:
                    raise nx.NetworkXError(f"Node {n} in nodelist is not in G")
            raise nx.NetworkXError("nodelist contains duplicates.")

    A = np.full((nlen, nlen), fill_value=nonedge, dtype=dtype, order=order)

    # Corner cases: empty nodelist or graph without any edges
    if nlen == 0 or G.number_of_edges() == 0:
        return A

    # If dtype is structured and weight is None, use dtype field names as
    # edge attributes
    edge_attrs = None  # Only single edge attribute by default
    if A.dtype.names:
        if weight is None:
            edge_attrs = dtype.names
        else:
            raise ValueError(
                "Specifying `weight` not supported for structured dtypes\n."
                "To create adjacency matrices from structured dtypes, use `weight=None`."
            )

    idx = dict(zip(sorted(nodelist), range(nlen)))
    if len(nodelist) < len(G):
        G = G.subgraph(nodelist)  # A real subgraph, not view

    # Collect all edge weights and reduce with `multigraph_weights`
    if G.is_multigraph():
        if edge_attrs:
            raise nx.NetworkXError(
                "Structured arrays are not supported for MultiGraphs"
            )
        d = defaultdict(list)
        for u, v, wt in G.edges(data=weight, default=1.0):
            d[(idx[u], idx[v])].append(wt)
        i, j = np.array(list(d.keys())).T  # indices
        wts = [multigraph_weight(ws) for ws in d.values()]  # reduced weights
    else:
        i, j, wts = [], [], []

        # Special branch: multi-attr adjacency from structured dtypes
        if edge_attrs:
            # Extract edges with all data
            for u, v, data in G.edges(data=True):
                i.append(idx[u])
                j.append(idx[v])
                wts.append(data)
            # Map each attribute to the appropriate named field in the
            # structured dtype
            for attr in edge_attrs:
                attr_data = [wt.get(attr, 1.0) for wt in wts]
                A[attr][i, j] = attr_data
                if not G.is_directed():
                    A[attr][j, i] = attr_data
            return A

        for u, v, wt in G.edges(data=weight, default=1.0):
            i.append(idx[u])
            j.append(idx[v])
            wts.append(wt)

    # Set array values with advanced indexing
    A[i, j] = wts
    if not G.is_directed():
        A[j, i] = wts

    return A


def to_numpy_matrix(
    G,
    nodelist=None,
    dtype=None,
    order=None,
    multigraph_weight=sum,
    weight="weight",
    nonedge=0.0,
):
    import numpy as np

    A = to_numpy_array(
        G,
        nodelist=nodelist,
        dtype=dtype,
        order=order,
        multigraph_weight=multigraph_weight,
        weight=weight,
        nonedge=nonedge,
    )
    M = np.asmatrix(A, dtype=dtype)
    return M


def to_scipy_sparse_matrix(G, nodelist=None, dtype=None, weight="weight", format="csr"):
    import scipy as sp
    import scipy.sparse

    A = to_scipy_sparse_array(
        G, nodelist=nodelist, dtype=dtype, weight=weight, format=format
    )
    return sp.sparse.csr_matrix(A).asformat(format)


def to_scipy_sparse_array(G, nodelist=None, dtype=None, weight="weight", format="csr"):
    import scipy as sp
    import scipy.sparse  # call as sp.sparse

    if len(G) == 0:
        raise nx.NetworkXError("Graph has no nodes or edges")

    if nodelist is None:
        nodelist = sorted(G)
        nlen = len(G)
    else:
        nlen = len(nodelist)
        if nlen == 0:
            raise nx.NetworkXError("nodelist has no nodes")
        nodeset = set(G.nbunch_iter(nodelist))
        if nlen != len(nodeset):
            for n in nodelist:
                if n not in G:
                    raise nx.NetworkXError(f"Node {n} in nodelist is not in G")
            raise nx.NetworkXError("nodelist contains duplicates.")
        if nlen < len(G):
            G = G.subgraph(nodelist)

    index = dict(zip(nodelist, range(nlen)))
    coefficients = zip(
        *((index[u], index[v], wt) for u, v, wt in G.edges(data=weight, default=1))
    )
    try:
        row, col, data = coefficients
    except ValueError:
        # there is no edge in the subgraph
        row, col, data = [], [], []

    if G.is_directed():
        A = sp.sparse.coo_array((data, (row, col)), shape=(nlen, nlen), dtype=dtype)
    else:
        # symmetrize matrix
        d = data + data
        r = row + col
        c = col + row
        # selfloop entries get double counted when symmetrizing
        # so we subtract the data on the diagonal
        selfloops = list(nx.selfloop_edges(G, data=weight, default=1))
        if selfloops:
            diag_index, diag_data = zip(*((index[u], -wt) for u, v, wt in selfloops))
            d += diag_data
            r += diag_index
            c += diag_index
        A = sp.sparse.coo_array((d, (r, c)), shape=(nlen, nlen), dtype=dtype)
    try:
        return A.asformat(format)
    except ValueError as err:
        raise nx.NetworkXError(f"Unknown sparse matrix format: {format}") from err
