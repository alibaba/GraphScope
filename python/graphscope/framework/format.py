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

from abc import abstractmethod

from aenum import Enum

__all__ = ["VertexInputFormat",
            "EdgeInputFormat",
            "GiraphVertexInputFormat", 
            "GiraphEdgeInputFormat",
            "P2PGiraphVertexInputFormat",
            "P2PGiraphEdgeInputFormat",
            "BuiltInFormats"]



class FormatType(Enum):
    VERTEX_INPUT_FORMAT=1
    EDGE_INPUT_FORMAT=2
    VERTEX_OUTPUT_FORMAT=3
    EDGE_OUTPUT_FORMAT=4

class BuiltInFormats(Enum):
    P2PGiraphVertexInputFormat = 1
    P2PGiraphEdgeInputFormat = 2
    CSVFormat = 3

def format_enum_to_str(format_type : BuiltInFormats):
    if (format_type == BuiltInFormats.P2PGiraphVertexInputFormat):
        return "P2PGiraphVertexInputFormat"
    elif (format_type == BuiltInFormats.P2PGiraphEdgeInputFormat):
        return "P2PGiraphEdgeInputFormat"
    elif (format_type == None):
        return "None"


class Format:
    @abstractmethod
    def format_type(self):
        raise NotImplementedError

class GiraphClass:
    def __init__(self, giraph_class_name : str):
        self._giraph_class_name = giraph_class_name
    @property
    def giraph_class_name(self):
        return self._giraph_class_name


class VertexInputFormat(Format):
    @property
    def format_type(self):
        return FormatType.VERTEX_INPUT_FORMAT


class EdgeInputFormat(Format):
    @property
    def format_type(self):
        return FormatType.EDGE_INPUT_FORMAT

class GiraphVertexInputFormat(VertexInputFormat, GiraphClass):
    def __init__(self, giraph_format_class : str):
        VertexInputFormat.__init__(self)
        GiraphClass.__init__(self, giraph_format_class)

class GiraphEdgeInputFormat(EdgeInputFormat, GiraphClass):
    def __init__(self, giraph_format_class : str):
        EdgeInputFormat.__init__(self)
        GiraphClass.__init__(self, giraph_format_class)
