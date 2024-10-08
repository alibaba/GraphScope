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
    """
    A simple encoder to encode the data into bytes
    """

    def __init__(self, endian="little") -> None:
        self.byte_array = bytearray()
        self.endian = endian

    def put_int(self, value: int):
        """
        Put an integer into the byte array, 4 bytes
        """

        self.byte_array.extend(value.to_bytes(4, byteorder=self.endian))

    def put_long(self, value: int):
        """
        Put a long integer into the byte array, 8 bytes
        """
        self.byte_array.extend(value.to_bytes(8, byteorder=self.endian))

    def put_string(self, value: str):
        """
        Put a string into the byte array, first put the length of the string, then the string
        """

        self.put_int(len(value))
        self.byte_array.extend(value.encode("utf-8"))

    def put_byte(self, value: int):
        """
        Put a single byte into the byte array
        """

        self.byte_array.extend(value.to_bytes(1, byteorder=self.endian))

    def put_bytes(self, value: bytes):
        """
        Put a byte array into the byte array
        """

        self.byte_array.extend(value)

    def put_double(self, value: float):
        """
        Put a double into the byte array, 8 bytes
        """

        self.byte_array.extend(value.to_bytes(8, byteorder=self.endian))

    def get_bytes(self):
        """
        Get the bytes from the byte array
        """

        # return bytes not bytearray
        return bytes(self.byte_array)


class Decoder:
    """
    A simple decoder to decode the bytes into data
    """

    def __init__(self, byte_array: bytearray, endian="little") -> None:
        self.byte_array = byte_array
        self.index = 0
        self.endian = endian

    def get_int(self) -> int:
        """
        Get an integer from the byte array, 4 bytes

        returns: int
        """
        value = int.from_bytes(
            self.byte_array[self.index : self.index + 4],  # noqa E203
            byteorder=self.endian,
        )
        self.index += 4
        return value

    def get_long(self) -> int:
        """
        Get a long integer from the byte array, 8 bytes

        returns: int
        """
        value = int.from_bytes(
            self.byte_array[self.index : self.index + 8],  # noqa E203
            byteorder=self.endian,
        )
        self.index += 8
        return value

    def get_double(self) -> float:
        """
        Get a double from the byte array, 8 bytes

        returns: float
        """
        value = float.from_bytes(
            self.byte_array[self.index : self.index + 8],  # noqa E203
            byteorder=self.endian,
        )
        self.index += 8
        return value

    def get_byte(self) -> int:
        """
        Get a single byte from the byte array

        returns: int
        """

        value = int.from_bytes(
            self.byte_array[self.index : self.index + 1],  # noqa E203
            byteorder=self.endian,
        )
        self.index += 1
        return value

    def get_bytes(self, length: int) -> bytes:
        """
        Get a byte array from the byte array

        returns: A byte array
        """
        value = self.byte_array[self.index : self.index + length]  # noqa E203
        self.index += length
        return value

    def get_string(self) -> str:
        """
        Get a string from the byte array, first get the length of the string, then the string

        returns: str
        """
        length = self.get_int()
        value = self.byte_array[self.index : self.index + length].decode(  # noqa E203
            "utf-8"
        )
        self.index += length
        return value

    def is_empty(self) -> bool:
        """
        Whether the byte array is empty
        """

        return self.index == len(self.byte_array)


class InputFormat(Enum):
    CPP_ENCODER = 0  # raw bytes encoded by encoder/decoder
    CYPHER_JSON = 1  # json format string
    CYPHER_PROTO_ADHOC = 2  # protobuf adhoc bytes
    CYPHER_PROTO_PROCEDURE = 3  # protobuf procedure bytes


def append_format_byte(input: bytes, input_format: InputFormat):
    """
    Append a byte to the end of the input string to denote the input format
    """
    new_bytes = input + bytes([input_format.value])
    return new_bytes
