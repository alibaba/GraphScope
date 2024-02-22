#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020-2021 Alibaba Group Holding Limited. All Rights Reserved.
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

import pytest

import graphscope
from graphscope.config import gs_config

graphscope.set_option(show_log=True)
logger = logging.getLogger("graphscope")


def get_k8s_volumes():
    k8s_volumes = {
        "data": {
            "type": "hostPath",
            "field": {"path": os.environ["GS_TEST_DIR"], "type": "Directory"},
            "mounts": {"mountPath": "/testingdata"},
        }
    }
    return k8s_volumes


def get_gs_registry_on_ci_env():
    if "GS_REGISTRY" in os.environ:
        return os.environ["GS_REGISTRY"]
    return gs_config.kubernetes_launcher.image.registry


def get_gs_tag_on_ci_env():
    if "GS_TAG" in os.environ:
        return os.environ["GS_TAG"]
    return gs_config.kubernetes_launcher.image.tag


@pytest.fixture
def gs_session():
    sess = graphscope.session(
        num_workers=2,
        k8s_image_registry=get_gs_registry_on_ci_env(),
        k8s_image_tag=get_gs_tag_on_ci_env(),
        k8s_coordinator_cpu=2,
        k8s_coordinator_mem="4Gi",
        k8s_vineyard_cpu=2,
        k8s_vineyard_mem="512Mi",
        k8s_engine_cpu=2,
        k8s_engine_mem="4Gi",
        k8s_etcd_cpu=2,
        k8s_etcd_num_pods=3,
        k8s_etcd_mem="256Mi",
        vineyard_shared_mem="4Gi",
        k8s_volumes=get_k8s_volumes(),
        with_mars=True,  # enable mars
    )
    yield sess
    sess.close()


@pytest.mark.skipif("WITH_MARS" not in os.environ, reason="Mars tests is not enabled.")
def test_mars_session(gs_session):
    from mars import new_session
    from mars import tensor as mt

    ep = gs_session.engine_config["mars_endpoint"]
    mars_session = new_session(ep).as_default()

    tensor = mt.ones((4, 5, 6))
    b = mt.to_vineyard(tensor)
    print(b.execute().fetch()[0])
