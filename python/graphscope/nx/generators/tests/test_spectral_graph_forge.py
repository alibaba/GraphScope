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

# fmt: off
from networkx.generators.tests.test_spectral_graph_forge import \
    test_spectral_graph_forge

from graphscope.nx import NetworkXError
from graphscope.nx import is_isomorphic
from graphscope.nx.generators import karate_club_graph
from graphscope.nx.generators.spectral_graph_forge import spectral_graph_forge
from graphscope.nx.utils.compat import with_graphscope_nx_context

# fmt: on


@pytest.mark.skipif(
    os.environ.get("DEPLOYMENT", None) != "standalone",
    reason="TODO: fix on distributed deployment",
)
@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_spectral_graph_forge)
def test_spectral_graph_forge():
    pass
