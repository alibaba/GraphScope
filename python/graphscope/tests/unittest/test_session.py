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

import json
import os
import tempfile

import pytest

import graphscope
from graphscope.dataset import load_p2p_network

COORDINATOR_HOME = os.path.join(os.path.dirname(__file__), "../", "../coordinator")
new_data_dir = os.path.expandvars("${GS_TEST_DIR}/new_property/v2_e2")


def setUpModule():
    graphscope.set_option(show_log=True)


@pytest.fixture
def invalid_config_file():
    with tempfile.TemporaryDirectory() as dir_name:
        json_path = os.path.join(dir_name, "test.json")
        with open(json_path, "w") as f:
            # json format is incorrect.
            f.write('{"xxx": ["xxx"],"xxx": 9527 "num_workers": 4}')
        yield json_path


@pytest.fixture
def local_config_file():
    conf = {"num_workers": 4}
    with tempfile.TemporaryDirectory() as dir_name:
        json_path = os.path.join(dir_name, "test.json")
        with open(json_path, "w") as f:
            json.dump(conf, f)
        yield json_path


def test_default_session():
    default_sess = graphscope.get_default_session()
    assert default_sess.info["status"] == "active"
    default_sess.close()
    assert default_sess.info["status"] == "closed"


def test_launch_cluster_on_local(local_config_file):
    s = graphscope.session(cluster_type="hosts", config=local_config_file)
    info = s.info
    assert info["status"] == "active"
    s.close()


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_launch_session_from_config(local_config_file):
    saved = os.environ.get("GS_CONFIG_PATH", "")
    try:
        os.environ["GS_CONFIG_PATH"] = local_config_file
        s = graphscope.session(cluster_type="hosts")

        info = s.info
        assert info["status"] == "active"
        s.close()
    finally:
        os.environ["GS_CONFIG_PATH"] = saved


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_launch_session_from_dict():
    conf_dict = {"num_workers": 4}
    s = graphscope.session(cluster_type="hosts", config=conf_dict)

    info = s.info
    assert info["status"] == "active"
    s.close()


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_config_dict_has_highest_priority(local_config_file):
    s = graphscope.session(
        cluster_type="hosts", config=local_config_file, num_workers=2
    )

    info = s.info
    assert info["status"] == "active"
    s.close()


def test_error_on_config_file_not_exist():
    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        graphscope.session(cluster_type="hosts", config="~/non_existing_filename.txt")


def test_error_on_invalid_config_file(invalid_config_file):
    # invalid config file (example json format incorrect)
    with pytest.raises(json.decoder.JSONDecodeError):
        graphscope.session(cluster_type="hosts", config=invalid_config_file)


def test_correct_closing_on_hosts():
    s1 = graphscope.session(cluster_type="hosts")

    s1.close()
    # check, launched coordinator and graphscope-engines on local are correctly closed.
    # test close twice
    s1.close()


@pytest.mark.skipif("FULL_TEST_SUITE" not in os.environ, reason="Run in nightly CI")
def test_border_cases():
    s1 = graphscope.session(cluster_type="hosts")
    s2 = graphscope.session(cluster_type="hosts")
    s3 = graphscope.session(cluster_type="hosts")

    s1.as_default()
    assert graphscope.get_default_session() == s1

    g3 = load_p2p_network(s3)
    pg3 = g3.project(vertices={"host": ["id"]}, edges={"connect": ["dist"]})

    with pytest.raises(
        ValueError,
        match="A default session is already active. You must explicitly call Session.close().",
    ):
        s2.as_default()

    s1.close()

    s2.as_default()
    assert graphscope.get_default_session() == s2
    s2.close()

    s3.as_default()
    assert graphscope.get_default_session() == s3
    sssp = graphscope.sssp(pg3, src=4)  # ok, g3 belong to s3
    s3.close()


def test_with():
    with graphscope.session(cluster_type="hosts") as sess:
        assert graphscope.get_default_session() == sess

    sess = graphscope.session(cluster_type="hosts")
    with sess:
        pass
    assert sess.info["status"] == "closed"
