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

# from timeit import default_timer as timer

# begin = timer()
import networkx.classes.coreviews
import networkx.classes.graphviews
import networkx.classes.reportviews

# begin = timer()
# graphscope.nx not implement MultiGraph and MultiDiGraph, forward NetworkX to pass tests
from networkx.classes import MultiDiGraph
from networkx.classes import MultiGraph

# print("class import 5", timer() - begin)
# begin = timer()
from graphscope.nx.classes.digraph import DiGraph

# print("class import 3", timer() - begin)
# begin = timer()
from graphscope.nx.classes.function import *

# begin = timer()
from graphscope.nx.classes.graph import Graph

# print("class import 1", timer() - begin)

# print("class import 2", timer() - begin)

# print("class import 4", timer() - begin)
