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


import array
import functools

__all__ = ["declare", "Vertex", "VertexVector", "VertexHeap"]


# Emulated PIE type
class GraphScopeMetaType(type):
    def __getitem__(type, ix):
        return array(type, ix)


GraphScopeTypeObject = GraphScopeMetaType("GrapeTypeObject", (object,), {})


class GraphScopeType(GraphScopeTypeObject):
    def __init__(self, name=None):
        self.name = name

    def __repr__(self):
        return self.name


Vertex = GraphScopeType("Vertex")
VertexVector = GraphScopeType("VertexVector")
VertexHeap = GraphScopeType("VertexHeap")


# Place holder
class __PlaceHolder(object):
    """Access it will trigger an exception"""

    def __init__(self, func):
        functools.update_wrapper(self, func)

    def __getattr__(self, key):
        raise RuntimeError("Operation not allowed.")

    def __call__(self, *args, **kwargs):
        raise RuntimeError("Operation not allowed.")


def declare(graphscope_type, variable):
    return __PlaceHolder(declare)
