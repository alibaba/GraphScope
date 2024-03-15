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


class StatusCode(Enum):
    OK = 0
    SERVER_INTERNAL_ERROR = 1
    INVALID_ARGUMENT = 2
    PERMISSION_DENIED = 3
    NOT_FOUND = 4
    ALREADY_EXISTS = 5
    UNAUTHENTICATED = 6


class Status:
    def __init__(self, status: StatusCode, message: str):
        self.status = status
        self.message = message

    def __str__(self):
        return f"Status: {self.status}, message: {self.message}"

    def __repr__(self):
        return f"Status: {self.status}, message: {self.message}"

    @property
    def is_ok(self):
        return self.status == StatusCode.OK

    @property
    def is_error(self):
        return self.status != StatusCode.OK

    @property
    def message(self):
        return self.message
