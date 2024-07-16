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
from enum import Enum

class InputFormat(Enum):
    CPP_ENCODER = 0 # raw bytes encoded by encoder/decoder
    CYPHER_JSON = 1 # json format string
    CYPHER_ADHOC = 2 # adhoc format string
    CYPHER_PROTO = 3 # protobuf format string

def append_format_byte(input: bytes, input_format: InputFormat):
    """
    Append a byte to the end of the input string to denote the input format
    """
    new_bytes = str.encode(input) + bytes([input_format.value])
    return new_bytes
    