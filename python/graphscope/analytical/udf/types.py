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

__all__ = ["declare", "Vertex"]


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
    """Declare a GraphScope data type.

    Args:
        graphscope_type (:class:`graphscope.analytical.udf.GraphScopeType`):
            Valid options are `graphscope.Vertex`
        variable:
            Python variable.

    Examples:
        >>> @pie(vd_type="string", md_type="string")
        >>> class MyAlgorithm(AppAssets):
        >>>     @staticmethod
        >>>     def Init(frag, context):
        >>>         graphscope.declare(graphscope.Vertex, source)
        >>>         # means `Vertex source;` in c++ code
        >>>     @staticmethod
        >>>     def PEval(frag, context):
        >>>         pass
        >>>     @staticmethod
        >>>     def IncEval(frag, context):
        >>>         pass
    """
    return __PlaceHolder(declare)
