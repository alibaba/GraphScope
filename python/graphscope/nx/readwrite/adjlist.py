#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file adjlist.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/readwrite/adjlist.py
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

import networkx.readwrite.adjlist
from networkx.readwrite.adjlist import parse_adjlist as _parse_adjlist
from networkx.utils.decorators import open_file

from graphscope import nx
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import patch_docstring

import_as_graphscope_nx(networkx.readwrite.adjlist)


@patch_docstring(_parse_adjlist)
def parse_adjlist(
    lines, comments="#", delimiter=None, create_using=None, nodetype=None
):
    G = nx.empty_graph(0, create_using)
    edges = []
    nodes = []  # nodes that has not any adjacency
    for line in lines:
        p = line.find(comments)
        if p >= 0:
            line = line[:p]
        if not line:
            continue
        vlist = line.strip().split(delimiter)
        u = vlist.pop(0)
        # convert types
        if nodetype is not None:
            try:
                u = nodetype(u)
            except Exception as e:
                raise TypeError(
                    "Failed to convert node ({}) to type {}".format(u, nodetype)
                ) from e
        if len(vlist) == 0:
            nodes.append(u)
        if nodetype is not None:
            try:
                vlist = map(nodetype, vlist)
            except Exception as e:
                raise TypeError(
                    "Failed to convert nodes ({}) to type {}".format(
                        ",".join(vlist), nodetype
                    )
                ) from e
        edges.extend([u, v] for v in vlist)
    if nodes:
        G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    return G


@open_file(0, mode="rb")
def read_adjlist(
    path,
    comments="#",
    delimiter=None,
    create_using=None,
    nodetype=None,
    encoding="utf-8",
):
    """Read graph in adjacency list format from path.

    Parameters
    ----------
    path : string or file
       Filename or file handle to read.
       Filenames ending in .gz or .bz2 will be uncompressed.

    create_using : graphscope.nx graph constructor, optional (default=nx.Graph)
       Graph type to create. If graph instance, then cleared before populated.

    nodetype : int, str, float, tuple, bool Python object, optional
       Convert nodes to this type.

    comments : string, optional
       Marker for comment lines

    delimiter : string, optional
       Separator for node labels.  The default is whitespace.

    Returns
    -------
    G: graphscope.nx graph
        The graph corresponding to the lines in adjacency list format.

    Notes
    -----
    This format does not store graph or node data.

    See Also
    --------
    read_edgelist
    """
    lines = (line.decode(encoding) for line in path)
    return parse_adjlist(
        lines,
        comments=comments,
        delimiter=delimiter,
        create_using=create_using,
        nodetype=nodetype,
    )


# fixture for pytest
def teardown_module(module):
    import os

    for fname in ["test.adjlist", "test.adjlist.gz"]:
        if os.path.isfile(fname):
            os.unlink(fname)
