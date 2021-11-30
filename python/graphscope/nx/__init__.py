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

# forward
import networkx.exception as exception
import networkx.testing as testing
from networkx.exception import *

import graphscope.nx.drawing.nx_agraph as nx_agraph
import graphscope.nx.drawing.nx_pydot as nx_pydot
from graphscope.nx.classes import *
from graphscope.nx.convert import *
from graphscope.nx.convert_matrix import *
from graphscope.nx.drawing import *
from graphscope.nx.generators import *
from graphscope.nx.readwrite import *
from graphscope.nx.relabel import *
from graphscope.nx.utils import *

# NB: algorithm may conflict in name with generators, use algorithm first
from graphscope.nx.algorithms import *  # isort:skip

# set session attribute to Graph and DiGraph
setattr(Graph, "_session", None)
setattr(DiGraph, "_session", None)
