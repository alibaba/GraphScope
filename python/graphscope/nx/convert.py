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
from networkx.convert import from_dict_of_dicts
from networkx.convert import from_dict_of_lists
from networkx.convert import from_edgelist
from networkx.convert import to_dict_of_dicts
from networkx.convert import to_dict_of_lists

from graphscope import nx
from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.convert)


def to_networkx_graph(data, create_using=None, multigraph_input=False):  # noqa: C901
    """Make a graph from a known data structure.

    The preferred way to call this is automatically
    from the class constructor

    >>> d = {0: {1: {'weight':1}}} # dict-of-dicts single edge (0,1)
    >>> G = nx.Graph(d)

    instead of the equivalent

    >>> G = nx.from_dict_of_dicts(d)

    Parameters
    ----------
    data : object to be converted

        Current known types are:
         any NetworkX graph
         dict-of-dicts
         dict-of-lists
         container (ie set, list, tuple, iterator) of edges
         Pandas DataFrame (row per edge)
         numpy matrix
         numpy ndarray
         scipy sparse matrix

    create_using : nx graph constructor, optional (default=nx.Graph)
        Graph type to create. If graph instance, then cleared before populated.

    multigraph_input : bool (default False)
        If True and  data is a dict_of_dicts,
        try to create a multigraph assuming dict_of_dict_of_lists.
        If data and create_using are both multigraphs then create
        a multigraph from a multigraph.
    """
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
        except Exception as e:
            raise nx.NetworkXError("Input is not a correct NetworkX-like graph.") from e

    # dict of dicts/lists
    if isinstance(data, dict):
        try:
            return from_dict_of_dicts(
                data, create_using=create_using, multigraph_input=multigraph_input
            )
        except Exception:
            try:
                return from_dict_of_lists(data, create_using=create_using)
            except Exception as e:
                raise TypeError("Input is not known type.") from e

    # list or generator of edges
    if isinstance(data, (list, tuple)) or any(
        hasattr(data, attr) for attr in ["_adjdict", "next", "__next__"]
    ):
        try:
            return from_edgelist(data, create_using=create_using)
        except Exception as e:
            raise nx.NetworkXError("Input is not a valid edge list") from e

    # Pandas DataFrame
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            if data.shape[0] == data.shape[1]:
                try:
                    return nx.from_pandas_adjacency(data, create_using=create_using)
                except Exception as e:
                    msg = "Input is not a correct Pandas DataFrame adjacency matrix."
                    raise nx.NetworkXError(msg) from e
            else:
                try:
                    return nx.from_pandas_edgelist(
                        data, edge_attr=True, create_using=create_using
                    )
                except Exception as e:
                    msg = "Input is not a correct Pandas DataFrame edge-list."
                    raise nx.NetworkXError(msg) from e
    except ImportError:
        msg = "pandas not found, skipping conversion test."
        warnings.warn(msg, ImportWarning)

    # numpy matrix or ndarray
    try:
        import numpy

        if isinstance(data, (numpy.matrix, numpy.ndarray)):
            try:
                return nx.from_numpy_matrix(data, create_using=create_using)
            except Exception as e:
                raise nx.NetworkXError(
                    "Input is not a correct numpy matrix or array."
                ) from e
    except ImportError:
        warnings.warn("numpy not found, skipping conversion test.", ImportWarning)

    # scipy sparse matrix - any format
    try:
        import scipy

        if hasattr(data, "format"):
            try:
                return nx.from_scipy_sparse_matrix(data, create_using=create_using)
            except Exception as e:
                raise nx.NetworkXError(
                    "Input is not a correct scipy sparse matrix type."
                ) from e
    except ImportError:
        warnings.warn("scipy not found, skipping conversion test.", ImportWarning)

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
