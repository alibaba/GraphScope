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

import zipfile
from enum import Enum
from io import BytesIO
from typing import List

__all__ = [
    "CType",
    "InMemoryZip",
    "LinesWrapper",
    "PIEAggregateType",
    "PregelAggregatorType",
    "ProgramModel",
]


class ExpectFuncDef(Enum):
    """A enumeration class of expect function define in AST transform."""

    INIT = "Init"
    COMPUTE = "Compute"
    COMBINE = "Combine"
    PEVAL = "PEval"
    INCEVAL = "IncEval"


class LinesWrapper(object):
    def __init__(self):
        self._lines = []
        self._s = u""

    def put(self, s):
        self._s += s

    def newline(self):
        self._lines.append(self._s)
        self._s = u""

    def putline(self, s):
        self.put(s)
        self.newline()

    def dump(self):
        self.newline()
        return self._lines


class ProgramModel(Enum):
    """A enumeration class of supported program model."""

    Pregel = 0
    PIE = 1


class CType(Enum):
    """A Enum class for type transfer from python to c++.

    Examples:

      >>> a = CType.from_string('int')
      >>> print(str(a))
      int32_t

      >>> a = CType.from_string('uint64')
      >>> print(str(a))
      uint64_t
    """

    Undefined = 0
    Int32 = 1
    Int64 = 2
    UInt32 = 3
    UInt64 = 4
    Double = 5
    Float = 6
    Bool = 7
    Char = 8
    String = 9

    def __str__(self):
        if self == self.Int32:
            return "int32_t"
        if self == self.Int64:
            return "int64_t"
        if self == self.UInt32:
            return "uint32_t"
        if self == self.UInt64:
            return "uint64_t"
        if self == self.Double:
            return "double"
        if self == self.Float:
            return "float"
        if self == self.Bool:
            return "bool"
        if self == self.Char:
            return "char"
        if self == self.String:
            return "string"
        return "null"

    @staticmethod
    def belong_stdint(s):
        if s in ("int32_t", "int64_t", "uint32_t", "uint64_t"):
            return True
        return False

    @staticmethod
    def from_string(s):
        if s == "Int32" or s == "int32" or s == "int":
            return CType.Int32
        elif s == "Int64" or s == "int64":
            return CType.Int64
        elif s == "UInt32" or s == "uint32":
            return CType.UInt32
        elif s == "UInt64" or s == "uint64":
            return CType.UInt64
        elif s == "Double" or s == "double":
            return CType.Double
        elif s == "Float" or s == "float":
            return CType.Float
        elif s == "Bool" or s == "bool":
            return CType.Bool
        elif s == "Char" or s == "char":
            return CType.Char
        elif s == "String" or s == "string":
            return CType.String
        else:
            raise ValueError("Wrong type: {}".format(s))


class PregelAggregatorType(Enum):
    """The builtin pregel aggregator type."""

    kBoolAndAggregator = (0,)
    kBoolOrAggregator = (1,)
    kBoolOverwriteAggregator = (2,)
    kDoubleMinAggregator = (3,)
    kDoubleMaxAggregator = (4,)
    kDoubleSumAggregator = (5,)
    kDoubleProductAggregator = (6,)
    kDoubleOverwriteAggregator = (7,)
    kInt64MinAggregator = (8,)
    kInt64MaxAggregator = (9,)
    kInt64SumAggregator = (10,)
    kInt64ProductAggregator = (11,)
    kInt64OverwriteAggregator = (12,)
    kTextAppendAggregator = (13,)
    kEmptyAggregator = 100

    @staticmethod
    def to_ctype(s):
        if s == PregelAggregatorType.kBoolAndAggregator:
            return CType.Bool
        elif s == PregelAggregatorType.kBoolOrAggregator:
            return CType.Bool
        elif s == PregelAggregatorType.kBoolOverwriteAggregator:
            return CType.Bool
        elif s == PregelAggregatorType.kDoubleMinAggregator:
            return CType.Double
        elif s == PregelAggregatorType.kDoubleMaxAggregator:
            return CType.Double
        elif s == PregelAggregatorType.kDoubleSumAggregator:
            return CType.Double
        elif s == PregelAggregatorType.kDoubleProductAggregator:
            return CType.Double
        elif s == PregelAggregatorType.kDoubleOverwriteAggregator:
            return CType.Double
        elif s == PregelAggregatorType.kInt64MinAggregator:
            return CType.Int64
        elif s == PregelAggregatorType.kInt64MaxAggregator:
            return CType.Int64
        elif s == PregelAggregatorType.kInt64SumAggregator:
            return CType.Int64
        elif s == PregelAggregatorType.kInt64ProductAggregator:
            return CType.Int64
        elif s == PregelAggregatorType.kInt64OverwriteAggregator:
            return CType.Int64
        elif s == PregelAggregatorType.kTextAppendAggregator:
            return CType.String
        else:
            return CType.Undefined

    @staticmethod
    def extract_ctype(s):
        if s == "kBoolAndAggregator":
            return CType.Bool
        elif s == "kBoolOrAggregator":
            return CType.Bool
        elif s == "kBoolOverwriteAggregator":
            return CType.Bool
        elif s == "kDoubleMinAggregator":
            return CType.Double
        elif s == "kDoubleMaxAggregator":
            return CType.Double
        elif s == "kDoubleSumAggregator":
            return CType.Double
        elif s == "kDoubleProductAggregator":
            return CType.Double
        elif s == "kDoubleOverwriteAggregator":
            return CType.Double
        elif s == "kInt64MinAggregator":
            return CType.Int64
        elif s == "kInt64MaxAggregator":
            return CType.Int64
        elif s == "kInt64SumAggregator":
            return CType.Int64
        elif s == "kInt64ProductAggregator":
            return CType.Int64
        elif s == "kInt64OverwriteAggregator":
            return CType.Int64
        elif s == "kTextAppendAggregator":
            return CType.String
        else:
            return CType.Undefined


class PIEAggregateType(Enum):
    """The builtin PIE aggregator type."""

    kMinAggregate = (0,)
    kMaxAggregate = (1,)
    kSumAggregate = (2,)
    kProductAggregate = (3,)
    kOverwriteAggregate = (4,)


def _add_if_not_exist(origin_list: List, item):
    if item not in origin_list:
        origin_list.append(item)
    return origin_list


def _merge_and_unique_lists(origin_list: List, *args) -> List:
    origin_list.extend([item for sublist in args for item in sublist])
    return list(set(origin_list))


class InMemoryZip(object):
    """InMemoryZip is used to generate a gar file during runtime, without touching the disk."""

    def __init__(self):
        self.in_memory_buffer = BytesIO()
        self.zip_file = zipfile.ZipFile(
            self.in_memory_buffer, "a", zipfile.ZIP_DEFLATED, False
        )

    def append(self, filepath, content):
        """Append a file to the zip file.

        Parameters
        ----------
        filepath: str
            The path to write the file in the zip file.
        content: str or bytes
            The content that will be written into the zip file.

        See Also
        --------
        ZipFile.writestr: write file to zip files.
        """
        self.zip_file.writestr(zipfile.ZipInfo(filepath), content)

    def read_bytes(self, raw=False):
        """Read bytes value of the zip file.

        Parameters
        ----------
        raw: bool
            If True, return the raw bytes. Otherwise return the BytesIO object.
        """
        # close the file first before reading bytes, close repeatly is OK.
        self.zip_file.close()
        if raw:
            return self.in_memory_buffer.getvalue()
        else:
            return self.in_memory_buffer
