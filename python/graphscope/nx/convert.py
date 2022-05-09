#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file convert.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/convert.py
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

import warnings

import networkx.convert

from graphscope import nx
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import patch_docstring

__all__ = [
    "to_networkx_graph",
    "from_dict_of_dicts",
    "to_dict_of_dicts",
    "from_dict_of_lists",
    "to_dict_of_lists",
    "from_edgelist",
    "to_edgelist",
]

import_as_graphscope_nx(networkx.convert)


@patch_docstring(networkx.convert.to_networkx_graph)
def to_networkx_graph(data, create_using=None, multigraph_input=False):  # noqa: C901
    # networkx graph or graphscope.nx graph
    if hasattr(data, "adj"):
        try:
            result = from_dict_of_dicts(
                data.adj,
                create_using=create_using,
                multigraph_input=data.is_multigraph(),
            )
            if hasattr(data, "graph"):  # data.graph should be dict-like
                result.graph.update(data.graph)
            if hasattr(data, "nodes"):  # data.nodes should be dict-like
                result.add_nodes_from(data.nodes.items())
            return result
        except Exception as err:
            raise nx.NetworkXError(
                "Input is not a correct NetworkX-like graph."
            ) from err

    # dict of dicts/lists
    if isinstance(data, dict):
        try:
            return from_dict_of_dicts(
                data, create_using=create_using, multigraph_input=multigraph_input
            )
        except Exception as err:
            if multigraph_input is True:
                raise nx.NetworkXError(
                    f"converting multigraph_input raised:\n{type(err)}: {err}"
                )
            try:
                return from_dict_of_lists(data, create_using=create_using)
            except Exception as err:
                raise TypeError("Input is not known type.") from err

    # Pandas DataFrame
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            if data.shape[0] == data.shape[1]:
                try:
                    return nx.from_pandas_adjacency(data, create_using=create_using)
                except Exception as err:
                    msg = "Input is not a correct Pandas DataFrame adjacency matrix."
                    raise nx.NetworkXError(msg) from err
            else:
                try:
                    return nx.from_pandas_edgelist(
                        data, edge_attr=True, create_using=create_using
                    )
                except Exception as err:
                    msg = "Input is not a correct Pandas DataFrame edge-list."
                    raise nx.NetworkXError(msg) from err
    except ImportError:
        msg = "pandas not found, skipping conversion test."
        warnings.warn(msg, ImportWarning)

    # numpy matrix or ndarray
    try:
        import numpy

        if isinstance(data, (numpy.matrix, numpy.ndarray)):
            try:
                return nx.from_numpy_matrix(data, create_using=create_using)
            except Exception as err:
                raise nx.NetworkXError(
                    "Input is not a correct numpy matrix or array."
                ) from err
    except ImportError:
        warnings.warn("numpy not found, skipping conversion test.", ImportWarning)

    # scipy sparse matrix - any format
    try:
        import scipy

        if hasattr(data, "format"):
            try:
                return nx.from_scipy_sparse_matrix(data, create_using=create_using)
            except Exception as err:
                raise nx.NetworkXError(
                    "Input is not a correct scipy sparse matrix type."
                ) from err
    except ImportError:
        warnings.warn("scipy not found, skipping conversion test.", ImportWarning)

    # Note: most general check - should remain last in order of execution
    # Includes containers (e.g. list, set, dict, etc.), generators, and
    # iterators (e.g. itertools.chain) of edges
    if isinstance(data, (Collection, Generator, Iterator)):
        try:
            return from_edgelist(data, create_using=create_using)
        except Exception as err:
            raise nx.NetworkXError("Input is not a valid edge list") from err

    raise nx.NetworkXError("Input is not a known data type for conversion.")


def to_nx_graph(nx_graph):
    import networkx

    if not nx_graph.is_directed() and not nx_graph.is_multigraph():
        g = networkx.Graph()
        edges = nx_graph.edges.data()
    elif nx_graph.is_directed() and not nx_graph.is_multigraph():
        g = networkx.DiGraph()
        edges = nx_graph.edges.data()
    elif not nx_graph.is_directed() and nx_graph.is_multigraph():
        g = networkx.MultiGraph()
        edges = nx_graph.edges.data(keys=True)
    else:
        g = networkx.MultiDiGraph()
        edges = nx_graph.edges.data(keys=True)
    nodes = nx_graph.nodes.data()
    g.update(edges, nodes)
    g.graph.update(nx_graph.graph)
    return g
