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

gar_test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")


@pytest.mark.skip(reason="Issue 3162")
def test_load_from_gar(graphscope_session):
    graph_yaml = os.path.join(
        gar_test_repo_dir, "graphar/ldbc_sample/parquet/ldbc_sample.graph.yml"
    )
    print(graph_yaml)
    graph = graphscope_session.load_from_gar(graph_yaml)
    assert graph.schema is not None
    del graph


@pytest.mark.skip(reason="Issue 3162")
def test_archive_to_gar(ldbc_graph):
    graph_yaml = os.path.join(gar_test_repo_dir, "graphar/ldbc/ldbc.graph.yml")
    ldbc_graph.archive(graph_yaml)
