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


class StatusCode(Enum):
    OK = 0
    BAD_REQUEST = 1
    FORBIDDEN = 2
    NOT_FOUND = 3
    SERVER_INTERNAL_ERROR = 4
    SERVICE_UNAVAILABLE = 5
    UNKNOWN = 6


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

    @property
    def get_message(self):
        return self.message

    # static method create a server internal error object
    @staticmethod
    def server_internal_error(message: str):
        return Status(StatusCode.SERVER_INTERNAL_ERROR, message)

    @staticmethod
    def from_exception(exception: ApiException):
        # mapping from ApiException to StatusCode
        if isinstance(exception, BadRequestException):
            return Status(StatusCode.BAD_REQUEST, exception.reason)
        elif isinstance(exception, ForbiddenException):
            return Status(StatusCode.FORBIDDEN, exception.reason)
        elif isinstance(exception, NotFoundException):
            return Status(StatusCode.NOT_FOUND, exception.reason)
        elif isinstance(exception, UnauthorizedException):
            return Status(StatusCode.BAD_REQUEST, exception.reason)
        elif isinstance(exception, ServiceException):
            return Status(StatusCode.SERVER_INTERNAL_ERROR, exception.reason)
        return Status(
            StatusCode.UNKNOWN, "Unknown Error from exception " + str(exception)
        )

    @staticmethod
    def from_response(response: ApiResponse):
        # mapping from ApiResponse to StatusCode
        if response.status_code == 200:
            return Status(StatusCode.OK, "OK")
        if response.status_code == 400:
            return Status(StatusCode.BAD_REQUEST, "Bad Request")
        if response.status_code == 403:
            return Status(StatusCode.FORBIDDEN, "Forbidden")
        if response.status_code == 404:
            return Status(StatusCode.NOT_FOUND, "Not Found")
        if response.status_code == 401:
            return Status(StatusCode.BAD_REQUEST, "Unauthorized")
        if response.status_code == 500:
            return Status(StatusCode.SERVER_INTERNAL_ERROR, "Internal Server Error")
        if response.status_code == 503:
            return Status(StatusCode.SERVICE_UNAVAILABLE, "Service Unavailable")
        return Status(StatusCode.UNKNOWN, "Unknown Error")

    @staticmethod
    def ok():
        return Status(StatusCode.OK, "OK")
