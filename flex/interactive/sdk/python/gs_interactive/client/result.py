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

from typing import Generic, TypeVar

from pydantic import Field

from gs_interactive.api_response import ApiResponse
from gs_interactive.client.status import Status
from gs_interactive.exceptions import ApiException

# Define a generic type placeholder
T = TypeVar("T")


# Generate a python class Result<T>, which has two field status and value. This class can be used to wrap the execution result for interface where exception may happen
class Result(Generic[T]):
    def __init__(self, status: Status, value: T):
        self.status = status
        self.value = value

    def __str__(self):
        return f"Result: {self.status}, value: {self.value}"

    def __repr__(self):
        return f"Result: {self.status}, value: {self.value}"

    def is_ok(self):
        return self.status.is_ok()

    def is_error(self):
        return self.status.is_error()

    def get_value(self):
        return self.value

    def get_status(self):
        return self.status

    def get_status_message(self):
        return self.status.message

    @staticmethod
    def ok(value):
        return Result(Status.ok(), value)

    @staticmethod
    def error(status: Status, msg: str):
        return Result(status, msg)

    @staticmethod
    def from_exception(exception: ApiException):
        return Result(Status.from_exception(exception), None)

    @staticmethod
    def from_response(response: ApiResponse):
        return Result(Status.from_response(response), response.data)
