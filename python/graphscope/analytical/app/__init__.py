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


# fmt: off
from graphscope.analytical.app.attribute_assortativity import \
    attribute_assortativity_coefficient
from graphscope.analytical.app.attribute_assortativity import \
    numeric_assortativity_coefficient
from graphscope.analytical.app.average_degree_connectivity import \
    average_degree_connectivity
from graphscope.analytical.app.average_shortest_path_length import \
    average_shortest_path_length

# fmt: on
from graphscope.analytical.app.bfs import bfs
from graphscope.analytical.app.bfs import property_bfs
from graphscope.analytical.app.cdlp import cdlp
from graphscope.analytical.app.clustering import clustering

# fmt: off
from graphscope.analytical.app.degree_assortativity_coefficient import \
    degree_assortativity_coefficient

# fmt: on
from graphscope.analytical.app.degree_centrality import degree_centrality
from graphscope.analytical.app.eigenvector_centrality import eigenvector_centrality
from graphscope.analytical.app.hits import hits
from graphscope.analytical.app.is_simple_path import is_simple_path
from graphscope.analytical.app.java_app import JavaApp
from graphscope.analytical.app.k_core import k_core
from graphscope.analytical.app.k_shell import k_shell
from graphscope.analytical.app.katz_centrality import katz_centrality
from graphscope.analytical.app.louvain import louvain
from graphscope.analytical.app.lpa import lpa
from graphscope.analytical.app.pagerank import pagerank
from graphscope.analytical.app.pagerank_nx import pagerank_nx
from graphscope.analytical.app.sssp import property_sssp
from graphscope.analytical.app.sssp import sssp
from graphscope.analytical.app.triangles import triangles
from graphscope.analytical.app.wcc import wcc
