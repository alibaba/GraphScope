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

import functools
import inspect
import logging
import os
import signal
import sys
import time
from functools import wraps

import grpc

from graphscope.config import GSConfig as gs_config
from graphscope.framework.errors import GRPCError
from graphscope.framework.errors import RetriesExceededError
from graphscope.proto import attr_value_pb2
from graphscope.proto import message_pb2

logger = logging.getLogger("graphscope")

# 2GB
GS_GRPC_MAX_MESSAGE_LENGTH = 2 * 1024 * 1024 * 1024 - 1

# GRPC max retries by code
GRPC_MAX_RETRIES_BY_CODE = {
    grpc.StatusCode.UNAVAILABLE: 5,
    grpc.StatusCode.DEADLINE_EXCEEDED: 5,
}


class GRPCUtils(object):
    # default to 256MB
    CHUNK_SIZE = (
        int(os.environ["GS_GRPC_CHUNK_SIZE"])
        if "GS_GRPC_CHUNK_SIZE" in os.environ
        else 256 * 1024 * 1024 - 1
    )

    def _generate_chunk_meta(self, chunk):
        chunk_meta = attr_value_pb2.ChunkMeta()
        chunk_meta.size = len(chunk.buffer)
        for k, v in chunk.attr.items():
            chunk_meta.attr[k].CopyFrom(v)
        return chunk_meta

    def split(self, dag_def):
        """Traverse `large_attr` of op and split into a list of chunks.

        Note that this method will modify `large_attr` attribute of op in dag_def.

        Returns:
            Sequence[Sequence[bytes]]: splited chunks.
        """
        chunks_list = []
        for op in dag_def.op:
            large_attr = attr_value_pb2.LargeAttrValue()
            for chunk in op.large_attr.chunk_list.items:
                # construct chunk meta
                large_attr.chunk_meta_list.items.extend(
                    [self._generate_chunk_meta(chunk)]
                )
                # split buffer
                chunks_list.append(
                    (
                        [
                            chunk.buffer[i : i + self.CHUNK_SIZE]
                            for i in range(0, len(chunk.buffer), self.CHUNK_SIZE)
                        ],
                        op.key,
                    )
                )
            # replace chunk with chunk_meta
            op.large_attr.CopyFrom(large_attr)
        return chunks_list

    def generate_runstep_requests(self, session_id, dag_def):
        runstep_requests = []
        chunks_list = self.split(dag_def)
        # head
        runstep_request = message_pb2.RunStepRequest(
            head=message_pb2.RunStepRequestHead(session_id=session_id, dag_def=dag_def)
        )
        runstep_requests.append(runstep_request)
        # bodies
        for chunks, op_key in chunks_list:
            for i, chunk in enumerate(chunks):
                # check the last element
                has_next = True
                if i + 1 == len(chunks):
                    has_next = False
                runstep_request = message_pb2.RunStepRequest(
                    body=message_pb2.RunStepRequestBody(
                        chunk=chunk, op_key=op_key, has_next=has_next
                    )
                )
                runstep_requests.append(runstep_request)
        # return a generator for stream request
        for item in runstep_requests:
            yield item

    def parse_runstep_responses(self, responses):
        chunks = []
        response_head = None
        has_next = True
        for response in responses:
            if response.HasField("head"):
                response_head = response
            else:
                if not chunks or not has_next:
                    chunks.append(response.body.chunk)
                else:
                    chunks[-1] += response.body.chunk
                has_next = response.body.has_next
        cursor = 0
        for op_result in response_head.head.results:
            if op_result.meta.has_large_result:
                op_result.result = chunks[cursor]
                cursor += 1
        return response_head.head


def handle_grpc_error_with_retry(fn, retry=True):
    """Decorator to handle grpc error. If retry is True, the function will
    be retried for certain errors, e.g., network unavailable.

    This function will retry max times with specific GRPC status.
    See detail in `GRPC_MAX_RETRIES_BY_CODE`.

    Refer and specical thanks to:

        https://github.com/googleapis/google-cloud-python/issues/2583
    """

    @functools.wraps(fn)
    def with_grpc_catch(*args, **kwargs):
        retries = 0
        while True:
            try:
                return fn(*args, **kwargs)
            except grpc.RpcError as exc:
                code = exc.code()
                max_retries = GRPC_MAX_RETRIES_BY_CODE.get(code)
                if not retry or max_retries is None:
                    raise GRPCError(
                        "rpc %s failed: status %s" % (str(fn.__name__), exc)
                    )

                if retries > max_retries:
                    raise RetriesExceededError(
                        "rpc %s failed: status %s" % (str(fn.__name__), exc)
                    )

                backoff = min(0.0625 * 2**retries, 1.0)
                logger.debug(
                    "Sleeping for %r before retrying failed request ...", backoff
                )

                retries += 1
                time.sleep(backoff)

    return with_grpc_catch


def handle_grpc_error(fn_or_retry):
    """Decorator to handle grpc error, and accepts an optional arugment to control
    whether the function should be retried for certain errors.

    This decorator can be used as

    .. code-block:: python

        @handle_grpc_error
        def fn(..)
            ...

    or

    .. code-block:: python

        @handle_grpc_error(retry=True)
        def fn(..)
            ...

    The argument 'retry' by default is True, to keep consistent with the previous
    behavior.
    """
    if isinstance(fn_or_retry, bool):
        return functools.partial(handle_grpc_error_with_retry, retry=fn_or_retry)
    else:
        return handle_grpc_error_with_retry(fn_or_retry)


def suppress_grpc_error(fn):
    """Decorator to suppress any grpc error."""

    @functools.wraps(fn)
    def with_grpc_catch(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except grpc.RpcError as exc:
            if isinstance(exc, grpc.Call):
                logger.warning(
                    "Grpc call '%s' failed: %s: %s",
                    fn.__name__,
                    exc.code(),
                    exc.details(),
                )
        except Exception as exc:  # noqa: F841
            logger.warning("RPC call failed", exc_info=exc)

    return with_grpc_catch


class ConditionalFormatter(logging.Formatter):
    """Provide an option to disable format for some messages.
    Taken from https://stackoverflow.com/questions/34954373/disable-format-for-some-messages
    Examples:
       .. code:: python

            >>> import logging
            >>> import sys
            >>> handler = logging.StreamHandler(sys.stdout)
            >>> formatter = ConditionalFormatter('%(asctime)s %(levelname)s - %(message)s')
            >>> handler.setFormatter(formatter)
            >>> logger = logging.getLogger("graphscope")
            >>> logger.setLevel("INFO")
            >>> logger.addHandler(handler)
            >>> logger.info("with formatting")
            2020-12-21 13:44:52,537 INFO - with formatting
            >>> logger.info("without formatting", extra={'simple': True})
            without formatting
    """

    def format(self, record):
        if hasattr(record, "simple") and record.simple:
            return record.getMessage()
        return logging.Formatter.format(self, record)


class GSLogger(object):
    @staticmethod
    def init():
        logging.basicConfig(level=logging.CRITICAL)
        # Default logger configuration
        stdout_handler = logging.StreamHandler(sys.stdout)
        formatter = ConditionalFormatter(
            "%(asctime)s [%(levelname)s][%(module)s:%(lineno)d]: %(message)s"
        )
        stdout_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)
        logger.propagate = False
        GSLogger.update()

        # nice traceback
        try:
            import rich.pretty
            import rich.traceback

            rich.traceback.install()
            rich.pretty.install()
        except ImportError:
            pass

    @staticmethod
    def update():
        if gs_config.show_log:
            log_level = gs_config.log_level
        else:
            log_level = logging.ERROR
        if isinstance(log_level, str):
            log_level = log_level.upper()
        logger.setLevel(log_level)
        for handler in logger.handlers:
            handler.setLevel(log_level)


GSLogger.init()


class CaptureKeyboardInterrupt(object):
    """Context Manager for capture keyboard interrupt

    Args:
        callback: function
            Callback function when KeyboardInterrupt occurs.

    Examples:
        >>> with CaptureKeyboardInterrupt(callback):
        >>>     do_somethings()
    """

    def __init__(self, callback=None):
        self._callback = callback

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is not None:
            if self._callback:
                try:
                    self._callback()
                except:  # noqa: E722
                    pass
            return False


class SignalIgnore(object):
    """Context Manager for signal ignore

    Args:
        signals (list of `signal.signal`):
            A list of signal you want to ignore.

    Examples:

        >>> with SignalIgnore(signal.SIGINT):
        >>>     func_call()

        >>> with SignalIgnore([signal.SIGINT, signal.SIGTERM]):
        >>>     func_call()
    """

    def __init__(self, signal):
        self._signal = list(signal)

    def __enter__(self):
        self._original_handler = [
            signal.signal(s, signal.SIG_IGN) for s in self._signal
        ]

    def __exit__(self, exc_type, exc_value, exc_tb):
        for s, h in zip(self._signal, self._original_handler):
            signal.signal(s, h)


def set_defaults(defaults):
    """Decorator to update default params to the latest defaults value.

    Args:
        defaults: object
            Include the latest values you want to set.

    Returns:
        The decorated function.

    Examples:
        >>> class Config(object):
        >>>     param1 = "new_value1"
        >>>     param2 = "new_value2"
        >>>
        >>> @set_defaults(Config)
        >>> def func(extra_param1, extra_param2=None, param1="old_value1", param2="old_value2", **kwargs):
        >>>     print(extra_param1, extra_param2, param1, param2)
        >>>
        >>> func("extra_value1")
        "extra_value1", None, "new_value1", "new_value2"
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            original_defaults = func.__defaults__

            new_defaults = []
            signature = inspect.signature(func)
            for k, v in signature.parameters.items():
                # filter self and position params
                if k == "self" or v.default is inspect.Parameter.empty:
                    continue
                if hasattr(defaults, k):
                    # don't fill None fields
                    if v.default is None:
                        new_defaults.append(None)
                    else:
                        new_defaults.append(getattr(defaults, k))
                else:
                    new_defaults.append(v.default)

            assert len(original_defaults) == len(new_defaults), "set defaults failed"
            func.__defaults__ = tuple(new_defaults)

            return_value = func(*args, **kwargs)

            # Restore original defaults.
            func.__defaults__ = original_defaults

            return return_value

        return wrapper

    return decorator
