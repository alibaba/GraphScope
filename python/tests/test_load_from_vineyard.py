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

import graphscope
from graphscope.framework.loader import Loader


@pytest.fixture
def p2p_31_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property")):
    return "vineyard://%s/p2p-31_property_e_0#header_row=true&delimiter=," % data_dir


@pytest.fixture
def p2p_31_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property")):
    return "vineyard://%s/p2p-31_property_v_0#header_row=true&delimiter=," % data_dir


@pytest.fixture
def student_v(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return "vineyard://%s/student.v#header_row=true&delimiter=," % data_dir


@pytest.fixture
def student_group_e(data_dir=os.path.expandvars("${GS_TEST_DIR}/property_graph")):
    return "vineyard://%s/group.e#header_row=true&delimiter=," % data_dir


@pytest.mark.skip("requires vineyard's io adaptors installed properly")
def test_p2p(graphscope_session, p2p_31_e, p2p_31_v):
    graph = graphscope.graphscope_session.g()
    graph = graph.add_vertices(Loader(p2p_31_v, session=graphscope_session), "person")
    graph = graph.add_edges(Loader(p2p_31_e, session=graphscope_session), "knows")
    assert graph.schema is not None


@pytest.mark.skip("requires vineyard's io adaptors installed properly")
def test_group(graphscope_session, student_group_e, student_v):
    graph = graphscope.graphscope_session.g()
    graph = graph.add_vertices(Loader(student_v, session=graphscope_session), "student")
    graph = graph.add_edges(
        Loader(student_group_e, session=graphscope_session), "group"
    )
    assert graph.schema is not None
