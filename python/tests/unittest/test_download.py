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

import pytest

from graphscope.dataset.ldbc import load_ldbc
from graphscope.dataset.modern_graph import load_modern_graph
from graphscope.dataset.ogbl_collab import load_ogbl_collab
from graphscope.dataset.ogbl_ddi import load_ogbl_ddi
from graphscope.dataset.ogbn_arxiv import load_ogbn_arxiv
from graphscope.dataset.ogbn_mag import load_ogbn_mag
from graphscope.dataset.ogbn_proteins import load_ogbn_proteins


def test_download_dataset(graphscope_session):
    g1 = load_modern_graph(graphscope_session)
    g1.unload()
    g2 = load_ldbc(graphscope_session)
    g2.unload()
    g3 = load_ogbn_mag(graphscope_session)
    g3.unload()
    g4 = load_ogbn_arxiv(graphscope_session)
    g4.unload()
    g5 = load_ogbn_proteins(graphscope_session)
    g5.unload()
    g6 = load_ogbl_collab(graphscope_session)
    g6.unload()
    g7 = load_ogbl_ddi(graphscope_session)
    g7.unload()
