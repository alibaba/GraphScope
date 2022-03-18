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

import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from graphscope import JavaApp


@pytest.fixture(scope="module")
def not_exist_jar():
    path = os.path.join("not_exist_dir", "not_exist.jar")
    return path


@pytest.fixture(scope="module")
def not_jar_file():
    return os.path.expandvars("${GS_TEST_DIR}/p2p-31.e")


@pytest.fixture(scope="module")
def a_gar_file():
    return os.path.expandvars("${GS_TEST_DIR}/gars/sssp_pie.gar")


@pytest.fixture(scope="module")
def empty_jar():
    return os.path.expandvars("${GS_TEST_DIR}/jars/empty.jar")


@pytest.fixture(scope="module")
def demo_jar():
    return os.path.expandvars("${USER_JAR_PATH}")


@pytest.fixture(scope="module")
def projected_graph_sssp_class():
    return "com.alibaba.graphscope.example.sssp.SSSP"


@pytest.fixture(scope="module")
def non_exist_java_class():
    return "com.alibaba.graphscope.example.non.existing.java.class"


@pytest.mark.skipif(
    os.environ.get("RUN_JAVA_TESTS") != "ON",
    reason="Java SDK is disabled, skip this test.",
)
def test_load_non_existing_jar(
    not_exist_jar, projected_graph_sssp_class, non_exist_java_class
):
    with pytest.raises(FileNotFoundError):
        sssp = JavaApp(not_exist_jar, projected_graph_sssp_class)
    with pytest.raises(FileNotFoundError):
        sssp = JavaApp(not_exist_jar, non_exist_java_class)


@pytest.mark.skipif(
    os.environ.get("RUN_JAVA_TESTS") != "ON",
    reason="Java SDK is disabled, skip this test.",
)
def test_load_not_a_jar(not_jar_file, projected_graph_sssp_class, non_exist_java_class):
    with pytest.raises(KeyError):
        sssp = JavaApp(not_jar_file, projected_graph_sssp_class)
    with pytest.raises(KeyError):
        sssp = JavaApp(not_jar_file, non_exist_java_class)


@pytest.mark.skipif(
    os.environ.get("RUN_JAVA_TESTS") != "ON",
    reason="Java SDK is disabled, skip this test.",
)
def test_load_gar_file(a_gar_file, projected_graph_sssp_class, non_exist_java_class):
    with pytest.raises(KeyError):
        sssp = JavaApp(a_gar_file, projected_graph_sssp_class)
    with pytest.raises(KeyError):
        sssp = JavaApp(a_gar_file, non_exist_java_class)


@pytest.mark.skipif(
    os.environ.get("RUN_JAVA_TESTS") != "ON",
    reason="Java SDK is disabled, skip this test.",
)
def test_load_empty_jar(empty_jar, projected_graph_sssp_class, non_exist_java_class):
    with pytest.raises(KeyError):
        sssp = JavaApp(empty_jar, projected_graph_sssp_class)
    with pytest.raises(KeyError):
        sssp = JavaApp(empty_jar, non_exist_java_class)


@pytest.mark.skipif(
    os.environ.get("RUN_JAVA_TESTS") != "ON",
    reason="Java SDK is disabled, skip this test.",
)
def test_load_correct_jar(projected_graph_sssp_class, demo_jar):
    sssp = JavaApp(demo_jar, projected_graph_sssp_class)


@pytest.mark.skipif(
    os.environ.get("RUN_JAVA_TESTS") != "ON",
    reason="Java SDK is disabled, skip this test.",
)
def test_sssp_property_vertex_data(
    demo_jar,
    graphscope_session,
    p2p_project_directed_graph,
    projected_graph_sssp_class,
):
    sssp = JavaApp(full_jar_path=demo_jar, java_app_class=projected_graph_sssp_class)
    sssp(p2p_project_directed_graph, src=6, threadNum=1)
