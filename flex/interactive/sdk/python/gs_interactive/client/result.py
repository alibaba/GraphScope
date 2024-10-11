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

from typing import Generic
from typing import TypeVar

from gs_interactive.api_response import ApiResponse
from gs_interactive.exceptions import ApiException

from gs_interactive.client.status import Status

# Define a generic type placeholder
T = TypeVar("T")


class Result(Generic[T]):
    """
    This is a generic class that wraps the result of an operation,
    It contains the status of the operation and the value returned by the operation.
    """

    def __init__(self, status: Status, value: T):
        """
        Construct a new Result object with the specified status and value.

        Args:
            status: the status of the operation.
            value: the value returned by the operation.
        """
        self.status = status
        self.value = value

    def __str__(self):
        return f"Result: {self.status}, value: {self.value}"

    def __repr__(self):
        return f"Result: {self.status}, value: {self.value}"

    def is_ok(self):
        """
        Whether the operation is successful.
        """
        return self.status.is_ok()

    def is_error(self):
        """
        Whether the operation is failed.
        """
        return self.status.is_error()

    def get_value(self):
        """
        Get the value returned by the operation.
        """
        return self.value

    def get_status(self):
        """
        Get the status of the operation.
        """
        return self.status

    def get_status_message(self):
        """
        Get the detail message of the status.
        """
        return self.status.message

    @staticmethod
    def ok(value):
        """
        A static method to create a successful result.
        """
        return Result(Status.ok(), value)

    @staticmethod
    def error(status: Status, msg: str):
        """
        A static method to create a failed result.
        """
        return Result(status, msg)

    @staticmethod
    def from_exception(exception: ApiException):
        """
        A static method create a Result object from an ApiException.
        """
        return Result(Status.from_exception(exception), None)

    @staticmethod
    def from_response(response: ApiResponse):
        """
        A static method create a Result object from an successful ApiResponse.
        """
        return Result(Status.from_response(response), response.data)
