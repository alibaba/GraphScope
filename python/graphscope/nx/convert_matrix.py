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
from networkx.convert_matrix import to_numpy_array as _to_numpy_array
from networkx.convert_matrix import to_numpy_matrix as _to_numpy_matrix
from networkx.convert_matrix import to_scipy_sparse_array as _to_scipy_sparse_array
from networkx.convert_matrix import to_scipy_sparse_matrix as _to_scipy_sparse_matrix

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


@patch_docstring(_to_numpy_array)
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

    undirected = not G.is_directed()
    index = dict(zip(sorted(nodelist), range(nlen)))

    if G.is_multigraph():
        # Handle MultiGraphs and MultiDiGraphs
        A = np.full((nlen, nlen), np.nan, order=order)
        # use numpy nan-aware operations
        operator = {sum: np.nansum, min: np.nanmin, max: np.nanmax}
        try:
            op = operator[multigraph_weight]
        except Exception as e:
            raise ValueError("multigraph_weight must be sum, min, or max") from e

        for u, v, attrs in G.edges(data=True):
            if (u in nodeset) and (v in nodeset):
                i, j = index[u], index[v]
                e_weight = attrs.get(weight, 1)
                A[i, j] = op([e_weight, A[i, j]])
                if undirected:
                    A[j, i] = A[i, j]
    else:
        # Graph or DiGraph, this is much faster than above
        A = np.full((nlen, nlen), np.nan, order=order)
        for u, nbrdict in G.adjacency():
            for v, d in nbrdict.items():
                try:
                    A[index[u], index[v]] = d.get(weight, 1)
                except KeyError:
                    # This occurs when there are fewer desired nodes than
                    # there are nodes in the graph: len(nodelist) < len(G)
                    pass

    A[np.isnan(A)] = nonedge
    A = np.asarray(A, dtype=dtype)
    return A


@patch_docstring(_to_numpy_matrix)
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


@patch_docstring(_to_scipy_sparse_matrix)
def to_scipy_sparse_matrix(G, nodelist=None, dtype=None, weight="weight", format="csr"):
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
        M = sp.sparse.coo_matrix((data, (row, col)), shape=(nlen, nlen), dtype=dtype)
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
        M = sp.sparse.coo_matrix((d, (r, c)), shape=(nlen, nlen), dtype=dtype)
    try:
        return M.asformat(format)
    # From Scipy 1.1.0, asformat will throw a ValueError instead of an
    # AttributeError if the format if not recognized.
    except (AttributeError, ValueError) as e:
        raise nx.NetworkXError(f"Unknown sparse matrix format: {format}") from e


@patch_docstring(_to_scipy_sparse_array)
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
