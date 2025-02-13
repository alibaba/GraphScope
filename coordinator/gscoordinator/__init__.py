#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited.
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
import sys

from .version import __version__

try:
    import graphscope
except ModuleNotFoundError:
    # if graphscope is not installed, try to locate it by relative path,
    # which is strong related with the directory structure of GraphScope
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
