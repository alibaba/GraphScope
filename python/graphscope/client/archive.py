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


import struct


class OutArchive(object):
    """A python equivalent for the :code:`Archive` serialization protocol."""

    def __init__(self, buffer):
        self._buffer = memoryview(buffer)
        self._buffer = self._buffer.cast("b", shape=(self._buffer.nbytes,))
        self._head = 0
        # assume all size is int64_t
        self._size_of_int64 = struct.calcsize("q")

    @property
    def buffer(self):
        return self._buffer

    @property
    def size(self):
        return self.__len__()

    def __len__(self):
        return len(self._buffer)

    @property
    def isempty(self):
        return len(self._buffer) == 0

    def get_block(self, size):
        """Peek a block of given size."""
        block = self._buffer[self._head : self._head + size]
        self._head += size
        return block

    def get_size(self):
        size = struct.unpack("q", self.get_block(self._size_of_int64))[0]
        return size

    def get_sized_block(self):
        """Peek a block with the size as the prefix bytes."""
        size = self.get_size()
        block = self.get_block(size)
        return block

    def get_string(self):
        """Peek a string.
        Get string's length first (stored as a size_t), then get number of bytes
        equivalents to the length.
        """
        size = self.get_size()
        string = self.get_block(size).tobytes().decode("utf-8")
        return string

    def get_int(self):
        """Peek a int."""
        size_of_int = struct.calcsize("i")
        i = struct.unpack("i", self.get_block(size_of_int))[0]
        return i
