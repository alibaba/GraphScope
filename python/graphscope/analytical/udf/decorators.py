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


import inspect
from collections import OrderedDict
from functools import partial
from typing import Sequence

from graphscope.analytical.udf.utils import CType
from graphscope.analytical.udf.utils import LinesWrapper
from graphscope.analytical.udf.utils import ProgramModel
from graphscope.analytical.udf.wrapper import pyx_codegen
from graphscope.framework.app import AppAssets

__all__ = ["pie", "pregel"]

PREGEL_NECESSARY_DEFS = ["Init", "Compute"]
PREGEL_COMBINE_DEF = "Combine"
PIE_NECESSARY_DEFS = ["Init", "PEval", "IncEval"]
_BASE_MEMBERS = [k for k, _ in inspect.getmembers(AppAssets)]


_common_imports = [
    "from libc.stdint cimport int32_t",
    "from libc.stdint cimport uint32_t",
    "from libc.stdint cimport int64_t",
    "from libc.stdint cimport uint64_t",
    "from libcpp cimport bool",
    "from libcpp.string cimport string",
    "from libc cimport limits",
    "from libc cimport math",
]


def pie(vd_type, md_type):
    """Decorator to mark algorithm class as PIE program model.

    Args:
      vd_type (str): The vertex data type.
      md_type (str): The message type.

    Returns:
        The decorated required class.

    Raises:
      RuntimeError: Decorate on decorated class.
      ValueError:
          1) If the decorator :code:`pie` not be used on a class definition or
          2) Missing necessary definition of method or
          3) Missing `staticmethod` decorator

    Examples:
    Decorate classes like this::

      >>> @pie(vd_type='double', md_type='double')
      >>> class PageRank_PIE():
      >>>     @staticmethod
      >>>     def Init(frag, context):
      >>>         pass
      >>>
      >>>     @staticmethod
      >>>     def PEval(frag, context):
      >>>         pass
      >>>
      >>>     @staticmethod
      >>>     def IncEval(frag, context):
      >>>         pass
    """

    def _pie_wrapper(vd_type, md_type, algo):
        if hasattr(algo, "__decorated__"):
            raise RuntimeError("Can't decorate on decorated class.")
        if not inspect.isclass(algo):
            raise ValueError('The decorator "pie" must be used on a class definition')

        defs = OrderedDict(
            {
                name: member
                for name, member in inspect.getmembers(algo)
                if name not in _BASE_MEMBERS and inspect.isroutine(member)
            }
        )
        _check_and_reorder(PIE_NECESSARY_DEFS, algo, defs)

        pyx_header = LinesWrapper()
        pyx_header.putline("from pie cimport AdjList")
        pyx_header.putline("from pie cimport Context")
        pyx_header.putline("from pie cimport Fragment")
        pyx_header.putline("from pie cimport PIEAggregateType")
        pyx_header.putline("from pie cimport Vertex")
        pyx_header.putline("from pie cimport VertexArray")
        pyx_header.putline("from pie cimport VertexRange")
        pyx_header.putline("from pie cimport MessageStrategy")
        pyx_header.putline("from pie cimport to_string")
        pyx_header.putline("")
        for line in _common_imports:
            pyx_header.putline(line)

        pyx_codegen(algo, defs, ProgramModel.PIE, pyx_header, vd_type, md_type)
        # pyx_codegen(algo, defs, pyx_wrapper, vd_type, md_type)
        return algo

    vd_ctype = str(CType.from_string(vd_type))
    md_ctype = str(CType.from_string(md_type))
    return partial(_pie_wrapper, vd_ctype, md_ctype)


def pregel(vd_type, md_type):
    """Decorator to mark algorithm class as pregel program model.

    Args:
      vd_type (str): The vertex data type.
      md_type (str): The message type.

    Returns:
        The decorated class.

    Raises:
      RuntimeError: Decorate on decorated class.
      ValueError:
          1) If the decorator :code:`pregel` not be used on a class definition or
          2) Missing necessary definition of method or
          3) Missing `staticmethod` decorator

    Examples:
    Decorate classes like this::

      >>> @pregel(vd_type='double', md_type='double')
      >>> class PageRank_Pregel():
      >>>     @staticmethod
      >>>     def Init(v, context):
      >>>         pass
      >>>
      >>>     @staticmethod
      >>>     def Compute(messages, v, context):
      >>>         pass
      >>>
      >>>     @staticmethod
      >>>     def Combine(messages):
      >>>         pass
    """

    def _pregel_wrapper(vd_type, md_type, algo):
        if hasattr(algo, "__decorated__"):
            raise RuntimeError("Can't decorate on decorated class.")
        if not inspect.isclass(algo):
            raise ValueError('The decorator "pie" must be used on a class definition')

        defs = OrderedDict(
            {
                name: member
                for name, member in inspect.getmembers(algo)
                if name not in _BASE_MEMBERS and inspect.isroutine(member)
            }
        )
        _check_and_reorder(PREGEL_NECESSARY_DEFS, algo, defs)

        enable_combine = False
        if PREGEL_COMBINE_DEF in defs.keys():
            enable_combine = True

        pyx_header = LinesWrapper()
        pyx_header.putline("from pregel cimport Context")
        pyx_header.putline("from pregel cimport MessageIterator")
        pyx_header.putline("from pregel cimport PregelAggregatorType")
        pyx_header.putline("from pregel cimport Vertex")
        pyx_header.putline("from pregel cimport to_string")
        pyx_header.putline("")
        for line in _common_imports:
            pyx_header.putline(line)

        pyx_codegen(
            algo,
            defs,
            ProgramModel.Pregel,
            pyx_header,
            vd_type,
            md_type,
            enable_combine,
        )
        return algo

    vd_ctype = str(CType.from_string(vd_type))
    md_ctype = str(CType.from_string(md_type))
    return partial(_pregel_wrapper, vd_ctype, md_ctype)


def _check_and_reorder(necessary_defs: Sequence, algo: type, defs: OrderedDict):
    for d in necessary_defs:
        if d not in defs.keys():
            raise ValueError("Can't find method definition of {}".format(d))
        if type(algo.__dict__[d]) is not staticmethod:
            raise ValueError("Missing staticmethod decorator on method {}".format(d))
        defs.move_to_end(d)
