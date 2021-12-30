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

try:
    sys.path.insert(0, os.path.dirname(__file__))

    import vineyard

    ctx = dict()
    if platform.system() != "Darwin":
        ctx["VINEYARD_USE_LOCAL_REGISTRY"] = "TRUE"
    with vineyard.envvars(ctx):
        import graphlearn

    try:
        import examples
    except ImportError:
        pass

    from graphscope.learning.graph import Graph
except ImportError:
    pass
finally:
    sys.path.pop(sys.path.index(os.path.dirname(__file__)))
