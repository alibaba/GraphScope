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

import os

import pytest

from graphscope.dataset import *


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_download_dataset(graphscope_session):
    g1 = load_modern_graph(graphscope_session)
    del g1
    g2 = load_ldbc(graphscope_session)
    del g2
    g3 = load_ogbn_mag(graphscope_session)
    del g3
    g4 = load_ogbn_arxiv(graphscope_session)
    del g4
    # Unable to fit in the memory of the CI environment
    # g5 = load_ogbn_proteins(graphscope_session)
    # del g5
    g6 = load_ogbl_collab(graphscope_session)
    del g6
    g7 = load_ogbl_ddi(graphscope_session)
    del g7
    g8 = load_p2p_network(graphscope_session)
    del g8
    g9 = load_cora(graphscope_session)
    del g9
    g10 = load_ppi(graphscope_session)
    del g10
    g11 = load_u2i(graphscope_session)
    del g11
