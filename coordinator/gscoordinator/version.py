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


import os

from packaging import version

version_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "VERSION")

with open(version_file_path, "r", encoding="utf-8") as fp:
    sv = version.parse(fp.read().strip())
    __is_prerelease__ = sv.is_prerelease
    __version__ = str(sv)

__version_tuple__ = (v for v in __version__.split("."))

del version_file_path
