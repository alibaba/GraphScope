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

# For a better test experience, can we use
# 1. Set up a local client, no need to pre-setup grape remotely,
# 2. Use a np array as source data and compare the result with correct result
# 3. Load from odps is slow, use 1 or 2 test cases to test is enough, no need all the test
#    need the data from odps.
# 4. The current test are flawed, some is not complete, some is not runnable, some is not correct
#    under current commit, they are just guidelines that lead us to make then correct.
