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


@pytest.fixture(scope="module")
def graphscope_session():
    graphscope.set_option(show_log=True)
    graphscope.set_option(log_level="DEBUG")
    if os.environ.get("DEPLOYMENT", None) == "standalone":
        sess = graphscope.session(cluster_type="hosts", num_workers=1)
    else:
        sess = graphscope.session(cluster_type="hosts")
    sess.as_default()
    yield sess
    sess.close()


def pytest_collection_modifyitems(items):
    for item in items:
        timeout_marker = None
        if hasattr(item, "get_closest_marker"):
            timeout_marker = item.get_closest_marker("timeout")
        elif hasattr(item, "get_marker"):
            timeout_marker = item.get_marker("timeout")
        if timeout_marker is None:
            item.add_marker(pytest.mark.timeout(600))
