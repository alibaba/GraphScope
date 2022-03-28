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

import inspect

from graphscope.proto import error_codes_pb2
from graphscope.proto import op_def_pb2

# All kinds of Graphscope error.
__all__ = [
    "NotFoundError",
    "VineyardError",
    "CompilationError",
    "AlreadyExistsError",
    "UDFInternalError",
    "UnavailableError",
    "InvalidArgumentError",
    "PermissionDeniedError",
    "UnimplementedError",
    "AnalyticalEngineInternalError",
    "InteractiveEngineInternalError",
    "LearningEngineInternalError",
    "CoordinatorInternalError",
    "NetworkError",
    "K8sError",
    "UnknownError",
    "FatalError",
    "GRPCError",
    "RetriesExceededError",
    "check_argument",
]


class _ReprableString(str):
    """A special class that prevents `repr()` adding extra `""` to `str`.

    It is used to optimize the user experiences to preseve `\n` when printing exceptions.
    """

    def __repr__(self) -> str:
        return self


class GSError(Exception):
    """Base class of GraphScope errors."""

    def __init__(self, message=None, detail=None):
        if message is not None and not isinstance(message, str):
            message = repr(message)
        if isinstance(detail, op_def_pb2.OpDef):
            message = f'{message or ""} op={detail}'
        super().__init__(_ReprableString(message))

    def __str__(self):
        return self.args[0] or ""


class NotFoundError(GSError):
    pass


class VineyardError(GSError):
    pass


class CompilationError(GSError):
    pass


class AlreadyExistsError(GSError):
    pass


class UDFInternalError(GSError):
    pass


class UnavailableError(GSError):
    pass


class InvalidArgumentError(GSError, ValueError, AssertionError):
    def __init__(self, message=None):
        super(InvalidArgumentError, self).__init__(message)


class PermissionDeniedError(GSError):
    pass


class UnimplementedError(GSError):
    pass


class AnalyticalEngineInternalError(GSError):
    pass


class InteractiveEngineInternalError(GSError):
    pass


class LearningEngineInternalError(GSError):
    pass


class CoordinatorInternalError(GSError):
    pass


class NetworkError(GSError):
    pass


class K8sError(GSError):
    pass


class UnknownError(GSError):
    pass


class FatalError(GSError):
    pass


class GRPCError(GSError):
    def __init__(self, message):
        message = "RPC failed: %s" % message
        super().__init__(message)


class RetriesExceededError(GSError):
    pass


_gs_error_types = {
    error_codes_pb2.TIMEOUT_ERROR: TimeoutError,
    error_codes_pb2.NOT_FOUND_ERROR: NotFoundError,
    error_codes_pb2.CONNECTION_ERROR: ConnectionError,
    error_codes_pb2.VINEYARD_ERROR: VineyardError,
    error_codes_pb2.COMPILATION_ERROR: CompilationError,
    error_codes_pb2.ALREADY_EXISTS_ERROR: AlreadyExistsError,
    error_codes_pb2.UDF_INTERNAL_ERROR: UDFInternalError,
    error_codes_pb2.UNAVAILABLE_ERROR: UnavailableError,
    error_codes_pb2.INVALID_ARGUMENT_ERROR: InvalidArgumentError,
    error_codes_pb2.PERMISSION_DENIED_ERROR: PermissionDeniedError,
    error_codes_pb2.NETWORK_ERROR: NetworkError,
    error_codes_pb2.K8S_ERROR: K8sError,
    error_codes_pb2.UNIMPLEMENTED_ERROR: UnimplementedError,
    error_codes_pb2.ANALYTICAL_ENGINE_INTERNAL_ERROR: AnalyticalEngineInternalError,
    error_codes_pb2.INTERACTIVE_ENGINE_INTERNAL_ERROR: InteractiveEngineInternalError,
    error_codes_pb2.LEARNING_ENGINE_INTERNAL_ERROR: LearningEngineInternalError,
    error_codes_pb2.COORDINATOR_INTERNAL_ERROR: CoordinatorInternalError,
    error_codes_pb2.UNKNOWN_ERROR: UnknownError,
    error_codes_pb2.FATAL_ERROR: FatalError,
    error_codes_pb2.RETRIES_EXCEEDED_ERROR: RetriesExceededError,
}


def check_argument(condition, message=None):
    if not condition:
        if message is None:
            message = "in '%s'" % inspect.stack()[1].code_context[0]
        raise InvalidArgumentError("Check failed: %s" % message)
