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

import io
import os
import sys
import time

import pytest
from networkx.readwrite.tests.test_gexf import TestGEXF

import graphscope.nx as nx
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGEXF)
class TestGEXF:
    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only test on standalone",
    )
    def test_write_with_node_attributes(self):
        # Addresses #673.
        G = nx.Graph()
        G.add_edges_from([(0, 1), (1, 2), (2, 3)])
        for i in range(4):
            G.nodes[i]["id"] = i
            G.nodes[i]["label"] = i
            G.nodes[i]["pid"] = i
            G.nodes[i]["start"] = i
            G.nodes[i]["end"] = i + 1

        if sys.version_info < (3, 8):
            expected = f"""<gexf version="1.2" xmlns="http://www.gexf.net/1.2\
draft" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:\
schemaLocation="http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/\
gexf.xsd">
  <meta lastmodifieddate="{time.strftime('%Y-%m-%d')}">
    <creator>NetworkX {nx.__version__}</creator>
  </meta>
  <graph defaultedgetype="undirected" mode="dynamic" name="" timeformat="long">
    <nodes>
      <node end="1" id="0" label="0" pid="0" start="0" />
      <node end="2" id="1" label="1" pid="1" start="1" />
      <node end="3" id="2" label="2" pid="2" start="2" />
      <node end="4" id="3" label="3" pid="3" start="3" />
    </nodes>
    <edges>
      <edge id="0" source="0" target="1" />
      <edge id="1" source="1" target="2" />
      <edge id="2" source="2" target="3" />
    </edges>
  </graph>
</gexf>"""
        else:
            expected = f"""<gexf xmlns="http://www.gexf.net/1.2draft" xmlns:xsi\
="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation=\
"http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/\
gexf.xsd" version="1.2">
  <meta lastmodifieddate="{time.strftime('%Y-%m-%d')}">
    <creator>NetworkX {nx.__version__}</creator>
  </meta>
  <graph defaultedgetype="undirected" mode="dynamic" name="" timeformat="long">
    <nodes>
      <node id="0" label="0" pid="0" start="0" end="1" />
      <node id="1" label="1" pid="1" start="1" end="2" />
      <node id="2" label="2" pid="2" start="2" end="3" />
      <node id="3" label="3" pid="3" start="3" end="4" />
    </nodes>
    <edges>
      <edge source="0" target="1" id="0" />
      <edge source="1" target="2" id="1" />
      <edge source="2" target="3" id="2" />
    </edges>
  </graph>
</gexf>"""
        obtained = "\n".join(nx.generate_gexf(G))
        assert expected == obtained

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only test on standalone",
    )
    def test_edge_id_construct(self):
        G = nx.Graph()
        G.add_edges_from([(0, 1, {"id": 0}), (1, 2, {"id": 2}), (2, 3)])

        if sys.version_info < (3, 8):
            expected = f"""<gexf version="1.2" xmlns="http://www.gexf.net/\
1.2draft" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:\
schemaLocation="http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/\
gexf.xsd">
  <meta lastmodifieddate="{time.strftime('%Y-%m-%d')}">
    <creator>NetworkX {nx.__version__}</creator>
  </meta>
  <graph defaultedgetype="undirected" mode="static" name="">
    <nodes>
      <node id="0" label="0" />
      <node id="1" label="1" />
      <node id="2" label="2" />
      <node id="3" label="3" />
    </nodes>
    <edges>
      <edge id="0" source="0" target="1" />
      <edge id="2" source="1" target="2" />
      <edge id="1" source="2" target="3" />
    </edges>
  </graph>
</gexf>"""
        else:
            expected = f"""<gexf xmlns="http://www.gexf.net/1.2draft" xmlns:xsi\
="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.\
gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd" version="1.2">
  <meta lastmodifieddate="{time.strftime('%Y-%m-%d')}">
    <creator>NetworkX {nx.__version__}</creator>
  </meta>
  <graph defaultedgetype="undirected" mode="static" name="">
    <nodes>
      <node id="0" label="0" />
      <node id="1" label="1" />
      <node id="2" label="2" />
      <node id="3" label="3" />
    </nodes>
    <edges>
      <edge source="0" target="1" id="0" />
      <edge source="1" target="2" id="2" />
      <edge source="2" target="3" id="1" />
    </edges>
  </graph>
</gexf>"""

        obtained = "\n".join(nx.generate_gexf(G))
        assert expected == obtained

    def test_simple_list(self):
        G = nx.Graph()
        list_value = [[1, 2, 3], [9, 1, 2]]
        G.add_node(1, key=list_value)
        fh = io.BytesIO()
        nx.write_gexf(G, fh)
        fh.seek(0)
        H = nx.read_gexf(fh, node_type=int)
        assert H.nodes[1]["networkx_key"] == list_value

    @pytest.mark.skip(reason="rapidjson not support inf.")
    def test_specials(self):
        pass

    @pytest.mark.skip(reason="rapidjson not support inf.")
    def test_numpy_type(self):
        pass
