#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file edgelist.py is referred and derived from project NetworkX,
#
#  https://github.com/networkx/networkx/blob/master/networkx/readwrite/edgelist.py
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

import networkx.readwrite.edgelist
from networkx.readwrite.edgelist import parse_edgelist as _parse_edgelist
from networkx.readwrite.edgelist import read_edgelist as _read_edgelist
from networkx.utils.decorators import open_file

from graphscope.experimental import nx
from graphscope.experimental.nx.utils.compat import import_as_graphscope_nx
from graphscope.experimental.nx.utils.compat import patch_docstring

import_as_graphscope_nx(networkx.readwrite.edgelist)


@patch_docstring(_parse_edgelist)
def parse_edgelist(
    lines, comments="#", delimiter=None, create_using=None, nodetype=None, data=True
):
    from ast import literal_eval

    G = nx.empty_graph(0, create_using)
    edges = []
    for line in lines:
        p = line.find(comments)
        if p >= 0:
            line = line[:p]
        if not len(line):
            continue
        # split line, should have 2 or more
        s = line.strip().split(delimiter)
        if len(s) < 2:
            continue
        u = s.pop(0)
        v = s.pop(0)
        d = s
        if nodetype is not None:
            try:
                u = nodetype(u)
                v = nodetype(v)
            except Exception as e:
                raise TypeError(
                    "Failed to convert nodes %s,%s to type %s." % (u, v, nodetype)
                ) from e

        if len(d) == 0 or data is False:
            # no data or data type specified
            edgedata = {}
        elif data is True:
            # no edge types specified
            try:  # try to evaluate as dictionary
                edgedata = dict(literal_eval(" ".join(d)))
            except Exception as e:
                raise TypeError(
                    "Failed to convert edge data (%s) to dictionary." % (d)
                ) from e
        else:
            # convert edge data to dictionary with specified keys and type
            if len(d) != len(data):
                raise IndexError(
                    "Edge data %s and data_keys %s are not the same length" % (d, data)
                )
            edgedata = {}
            for (edge_key, edge_type), edge_value in zip(data, d):
                try:
                    edge_value = edge_type(edge_value)
                except Exception as e:
                    raise TypeError(
                        "Failed to convert %s data %s to type %s."
                        % (edge_key, edge_value, edge_type)
                    ) from e
                edgedata.update({edge_key: edge_value})
        edges.append((u, v, edgedata))
    G.add_edges_from(edges)
    return G


@open_file(0, mode="rb")
@patch_docstring(_read_edgelist)
def read_edgelist(
    path,
    comments="#",
    delimiter=None,
    create_using=None,
    nodetype=None,
    data=True,
    edgetype=None,
    encoding="utf-8",
):
    lines = (line.decode(encoding) for line in path)
    return parse_edgelist(
        lines,
        comments=comments,
        delimiter=delimiter,
        create_using=create_using,
        nodetype=nodetype,
        data=data,
    )
