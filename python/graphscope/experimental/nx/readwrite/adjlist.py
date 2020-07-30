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
from networkx.readwrite.adjlist import read_adjlist as _read_adjlist
from networkx.utils.decorators import open_file

from graphscope.experimental import nx
from graphscope.experimental.nx.utils.compat import import_as_graphscope_nx
from graphscope.experimental.nx.utils.compat import patch_docstring

import_as_graphscope_nx(networkx.readwrite.adjlist)


@patch_docstring(_parse_adjlist)
def parse_adjlist(
    lines, comments="#", delimiter=None, create_using=None, nodetype=None
):
    G = nx.empty_graph(0, create_using)
    edges = []
    for line in lines:
        p = line.find(comments)
        if p >= 0:
            line = line[:p]
        if not len(line):
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
    G.add_edges_from(edges)
    return G


@open_file(0, mode="rb")
@patch_docstring(_read_adjlist)
def read_adjlist(
    path,
    comments="#",
    delimiter=None,
    create_using=None,
    nodetype=None,
    encoding="utf-8",
):
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
