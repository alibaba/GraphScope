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

from gs_interactive.api_response import ApiResponse
from gs_interactive.exceptions import (
    ApiException,
    BadRequestException,
    ForbiddenException,
    NotFoundException,
    ServiceException,
    UnauthorizedException,
)
from gs_interactive.client.generated.interactive_pb2 import Code as StatusCode
from gs_interactive.models.api_response_with_code import APIResponseWithCode


class Status:
    def __init__(self, status: StatusCode, message: str):
        self.status = status
        self.message = message

    def __str__(self):
        return f"Status: {self.status}, message: {self.message}"

    def __repr__(self):
        return f"Status: {self.status}, message: {self.message}"

    def is_ok(self) -> bool:
        return self.status == StatusCode.OK

    def is_error(self) -> bool:
        return self.status != StatusCode.OK
    
    def get_code(self):
        return self.status

    @property
    def get_message(self):
        return self.message

    # static method create a server internal error object
    @staticmethod
    def server_internal_error(message: str):
        return Status(StatusCode.INTERNAL_ERROR, message)

    @staticmethod
    def from_exception(exception: ApiException):
        # mapping from ApiException to StatusCode
        print("exception: ", exception)
        if isinstance(exception, BadRequestException):
            return Status(StatusCode.BAD_REQUEST, exception.body)
        elif isinstance(exception, ForbiddenException):
            return Status(StatusCode.PERMISSION_DENIED, exception.body)
        elif isinstance(exception, NotFoundException):
            return Status(StatusCode.NOT_FOUND, exception.body)
        elif isinstance(exception, ServiceException):
            if (exception.status == 503):
                return Status(StatusCode.SERVICE_UNAVAILABLE, exception.body)
            else:
                return Status(StatusCode.INTERNAL_ERROR, exception.body)
        return Status(
            StatusCode.UNKNOWN, "Unknown Error from exception " + exception.body
        )

    @staticmethod
    def from_response(response: ApiResponse):
        # mapping from ApiResponse to StatusCode
        if response.status_code == 200:
            return Status(StatusCode.OK, "OK")
        else:
            # If the status_code is not 200, we expect APIReponseWithCode returned from server
            api_response_with_code = response.data
            if isinstance(api_response_with_code, APIResponseWithCode):
                return Status(api_response_with_code.code, api_response_with_code.message)
            return Status(StatusCode.UNKNOWN, "Unknown Error")

    @staticmethod
    def ok():
        return Status(StatusCode.OK, "OK")
