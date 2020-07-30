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
    "ConnectionError",
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
    "NetworkError",
    "K8sError",
    "UnknownError",
    "FatalError",
    "GRPCError",
    "check_grpc_response",
    "check_argument",
]


class GSError(Exception):
    """Base class of GraphScope errors."""

    def __init__(self, message=None, detail=None):
        message = repr(message) if message is not None else None
        if isinstance(detail, op_def_pb2.OpDef):
            message = f'{message or ""} op={detail}'
        super().__init__(message)

    def __str__(self):
        return self.args[0] or ""


class NotFoundError(GSError):
    pass


class ConnectionError(GSError):  # pylint: disable=redefined-builtin
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
        message = "RPC failed, the engine might have crashed: %s" % message
        super().__init__(message)


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
    error_codes_pb2.UNKNOWN: UnknownError,
    error_codes_pb2.FATAL_ERROR: FatalError,
}


def check_grpc_response(response):
    status = response.status
    if status.code == error_codes_pb2.OK:
        return response

    if status.WhichOneof("detail") is None:
        detail = None
    else:
        detail = getattr(status, status.WhichOneof("detail"), None)

    error_type = _gs_error_types.get(status.code)
    if error_type:
        raise error_type(status.error_msg, detail)
    else:
        raise RuntimeError(
            "Undefined error: {}: {}, {}".format(
                status.code, status.error_msg, status.detail
            )
        )


def check_argument(condition, message=None):
    if not condition:
        if message is None:
            message = "in '%s'" % inspect.stack()[1].code_context[0]
        raise InvalidArgumentError("Check failed: %s" % message)
