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
import platform
import sys

# Tensorflow with Python 3.7 and ARM platform requires lower version of protobuf
if (sys.version_info.major == 3 and sys.version_info.minor == 7) or (
    platform.system() == "Linux" and platform.processor() == "aarch64"
):
    os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

# The gremlinpython has a async event loop, which may conflicts with
# jupyter notebook's event loop. The nest_asyncio must be applied
# before other imports, see also #2663.
import nest_asyncio

nest_asyncio.apply()

from graphscope.analytical.app import *
from graphscope.analytical.udf import declare
from graphscope.analytical.udf.types import Vertex
from graphscope.client.connection import conn
from graphscope.client.session import Session
from graphscope.client.session import g
from graphscope.client.session import get_default_session
from graphscope.client.session import graphlearn
from graphscope.client.session import graphlearn_torch
from graphscope.client.session import gremlin
from graphscope.client.session import has_default_session
from graphscope.client.session import interactive
from graphscope.client.session import session
from graphscope.client.session import set_option
from graphscope.framework.errors import *
from graphscope.framework.graph import Graph
from graphscope.framework.graph_builder import load_from
from graphscope.framework.graph_builder import load_from_gar
from graphscope.version import __version__

__doc__ = """
GraphScope - A unified distributed graph computing platform
=====================================================================

GraphScope provides a one-stop environment for performing diverse graph
operations on a cluster of computers through a user-friendly Python interface.
GraphScope makes multi-staged processing of large-scale graph data on compute
clusters simple by combining several important pieces of Alibaba technology:
including GRAPE, MaxGraph, and Graph-Learn (GL) for analytics, interactive,
and graph neural networks (GNN) computation, respectively, and
the vineyard store that offers efficient in-memory data transfers.

Main Components
---------------

Here are the main components that GraphScope includes:

  - Graph Interactive Engine (GIE): a parallel interactive engine for graph traversal

  - Graph Analytical Engine (GAE): a high-performance graph analytics engine

  - Graph Learning Engine (GLE): an end-to-end graph learning framework
"""


def __inject_graphscope_extensions():
    """The graphscope extensions follows the following signature:

    def ext(graphscope):
        ...

    It may inject classes, functions, methods and attributes to the graphscope module.
    """

    if "__graphscope_extensions__" in globals():
        for ext in globals()["__graphscope_extensions__"]:
            try:
                ext(sys.modules[__name__])
            except Exception as e:  # noqa
                pass


__inject_graphscope_extensions()
del __inject_graphscope_extensions
