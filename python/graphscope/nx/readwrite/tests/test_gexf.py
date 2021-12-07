#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import time

from networkx.readwrite.tests.test_gexf import TestGEXF
import pytest

import graphscope.nx as nx
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGEXF)
class TestGEXF:
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
