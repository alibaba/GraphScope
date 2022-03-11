#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file is referred and derived from project NetworkX
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

import networkx.readwrite.tests.test_gml
import pytest
from networkx.readwrite.tests.test_gml import TestGraph

from graphscope import nx
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.readwrite.tests.test_gml,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGraph)
class TestGraph:
    def test_tuplelabels(self):
        # https://github.com/networkx/networkx/pull/1048
        # Writing tuple labels to GML failed.
        G = nx.Graph()
        G.add_edge((0, 1), (1, 0))
        data = "\n".join(nx.generate_gml(G, stringizer=literal_stringizer))
        answer = """graph [
  node [
    id 0
    label "(0,1)"
  ]
  node [
    id 1
    label "(1,0)"
  ]
  edge [
    source 0
    target 1
  ]
]"""
        assert data == answer

    def test_data_types(self):
        # NB: json can't use tuple, byte as key
        data = [
            True,
            False,
            10**10,  # 10 ** 20 overflow on folly::dynamic
            -2e33,
            "'",
            '"&&amp;&&#34;"',
            [{"\xfd": "\x7f", chr(0x4444): [1, 2]}, [2, "3"]],
        ]
        try:  # fails under IronPython
            data.append(chr(0x14444))
        except ValueError:
            data.append(chr(0x1444))
        G = nx.Graph()
        G.name = data
        G.graph["data"] = data
        print(dict(data=data))
        G.add_node(0, int=-1, data=dict(data=data))
        G.add_edge(0, 0, float=-2.5, data=data)
        gml = "\n".join(nx.generate_gml(G, stringizer=literal_stringizer))
        G = nx.parse_gml(gml, destringizer=literal_destringizer)
        assert data == G.name
        assert {"name": data, "data": data} == G.graph
        assert list(G.nodes(data=True)) == [(0, dict(int=-1, data=dict(data=data)))]
        assert list(G.edges(data=True)) == [(0, 0, dict(float=-2.5, data=data))]
        G = nx.Graph()
        G.graph["data"] = "frozenset([1, 2, 3])"
        G = nx.parse_gml(nx.generate_gml(G), destringizer=literal_eval)
        assert G.graph["data"] == "frozenset([1, 2, 3])"

    def test_tuplelabels(self):
        # https://github.com/networkx/networkx/pull/1048
        # Writing tuple labels to GML failed.
        G = nx.Graph()
        G.add_edge((0, 1), (1, 0))
        data = "\n".join(nx.generate_gml(G, stringizer=literal_stringizer))
        answer = (
            """graph [
  node [
    id 0
    label "(0,1)"
  ]
  node [
    id 1
    label "(1,0)"
  ]
  edge [
    source 0
    target 1
  ]
]""",
            """graph [
  node [
    id 0
    label "(1,0)"
  ]
  node [
    id 1
    label "(0,1)"
  ]
  edge [
    source 0
    target 1
  ]
]""",
        )
        assert data in answer

    @pytest.mark.skip(
        reason="the folly json serialization does not support to keep the decimal point in SHORTEST mode, keep record on issue #1167"
    )
    def test_float_label(self):
        node = 1.0
        G = nx.Graph()
        G.add_node(node)
        fobj = tempfile.NamedTemporaryFile()
        nx.write_gml(G, fobj)
        fobj.seek(0)
        # Should be bytes in 2.x and 3.x
        data = fobj.read().strip().decode("ascii")
        answer = """graph [
  node [
    id 0
    label "1"
  ]
]"""
        assert data == answer

    @pytest.mark.skip(reason="rapidjson not support inf.")
    def test_special_float_label(self):
        pass
