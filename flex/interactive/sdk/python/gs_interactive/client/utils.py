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


class Encoder:
    def __init__(self, endian = 'little') -> None:
        self.byte_array = bytearray()
        self.endian = endian
        
    def put_int(self, value: int):
        # put the value in big endian, 4 bytes
        self.byte_array.extend(value.to_bytes(4, byteorder=self.endian))
        
    def put_long(self, value: int):
        self.byte_array.extend(value.to_bytes(8, byteorder=self.endian))
        
    def put_string(self, value: str):
        self.put_int(len(value))
        self.byte_array.extend(value.encode('utf-8'))
        
    def put_byte(self, value: int):
        self.byte_array.extend(value.to_bytes(1, byteorder=self.endian))
        
    def get_bytes(self):
        # return bytes not bytearray
        return bytes(self.byte_array)

class Decoder:
    def __init__(self, byte_array: bytearray) -> None:
        self.byte_array = byte_array
        self.index = 0
        
    def get_int(self) -> int:
        value = int.from_bytes(self.byte_array[self.index:self.index+4], byteorder='big')
        self.index += 4
        return value
    
    def get_long(self) -> int:
        value = int.from_bytes(self.byte_array[self.index:self.index+8], byteorder='big')
        self.index += 8
        return value
    
    def get_string(self) -> str:
        length = self.get_int()
        value = self.byte_array[self.index:self.index+length].decode('utf-8')
        self.index += length
        return value
    

class InputFormat(Enum):
    CPP_ENCODER = 0 # raw bytes encoded by encoder/decoder
    CYPHER_JSON = 1 # json format string
    CYPHER_PROTO_ADHOC = 2 # protobuf adhoc bytes
    CYPHER_PROTO_PROCEDURE = 3 # protobuf procedure bytes

def append_format_byte(input: bytearray, input_format: InputFormat):
    """
    Append a byte to the end of the input string to denote the input format
    """
    new_bytes = input + bytes([input_format.value])
    return new_bytes
    