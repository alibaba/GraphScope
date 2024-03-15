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

from interactive_sdk.client.common.status import Status


# Generate a python class Result<T>, which has two field status and value. This class can be used to wrap the execution result for interface where exception may happen
class Result:
    def __init__(self, status: Status, value):
        self._status = status
        self._value = value

    def __str__(self):
        return f"Result: {self._status}, value: {self._value}"

    def __repr__(self):
        return f"Result: {self._status}, value: {self._value}"

    @property
    def is_ok(self):
        return self._status.is_ok

    @property
    def is_error(self):
        return self._status.is_error

    @property
    def value(self):
        return self._value

    @property
    def status(self):
        return self._status
