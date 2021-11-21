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
from networkx.algorithms.assortativity import degree_mixing_dict

# fmt: off
from networkx.generators.tests.test_joint_degree_seq import \
    test_is_valid_directed_joint_degree
from networkx.generators.tests.test_joint_degree_seq import test_is_valid_joint_degree
from networkx.generators.tests.test_joint_degree_seq import test_joint_degree_graph

from graphscope.nx.generators import gnm_random_graph
from graphscope.nx.generators import powerlaw_cluster_graph
from graphscope.nx.generators.joint_degree_seq import directed_joint_degree_graph
from graphscope.nx.generators.joint_degree_seq import is_valid_directed_joint_degree
from graphscope.nx.generators.joint_degree_seq import is_valid_joint_degree
from graphscope.nx.generators.joint_degree_seq import joint_degree_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_is_valid_joint_degree)
def test_is_valid_joint_degree():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_joint_degree_graph)
def test_joint_degree_graph():
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_is_valid_directed_joint_degree)
def test_is_valid_directed_joint_degree():
    pass
