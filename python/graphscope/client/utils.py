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
import logging
import signal
import sys
from functools import wraps

from graphscope.config import GSConfig as gs_config

logger = logging.getLogger("graphscope")


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
        else:
            return logging.Formatter.format(self, record)


class GSLogger(object):
    @staticmethod
    def init():
        # Default logger configuration
        stdout_handler = logging.StreamHandler(sys.stdout)
        formatter = ConditionalFormatter(
            "%(asctime)s [%(levelname)s][%(module)s:%(lineno)d]: %(message)s"
        )
        stdout_handler.setFormatter(formatter)
        if gs_config.show_log:
            stdout_handler.setLevel(gs_config.log_level)
        else:
            stdout_handler.setLevel(logging.ERROR)
        logger.addHandler(stdout_handler)
        logger.propagate = False

    @staticmethod
    def update():
        if gs_config.show_log:
            log_level = gs_config.log_level
        else:
            log_level = logging.ERROR
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
