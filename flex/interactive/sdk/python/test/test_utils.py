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

import os
import time
import unittest

import pytest

from gs_interactive.client.driver import Driver
from gs_interactive.models import *
from gs_interactive.client.utils import InputFormat, append_format_byte

class TestUtils(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    def test_append_format_byte(self):
        input = "hello"
        new_bytes = append_format_byte(input, input_format=InputFormat.CPP_ENCODER)
        self.assertEqual(new_bytes, b'hello\x00')
        self.assertEqual(len(new_bytes), len(input) + 1)
