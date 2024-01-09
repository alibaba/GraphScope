#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited.
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

import datetime
import functools
import logging
import random
import string
import socket

logger = logging.getLogger("graphscope")


def handle_api_exception():
    """Decorator to handle api exception for openapi controllers."""

    def _handle_api_exception(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logger.info(str(e))
                return str(e), 500

        return wrapper

    return _handle_api_exception


def decode_datetimestr(datetime_str):
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d", "%Y-%m-%d-%H-%M-%S"]
    for f in formats:
        try:
            return datetime.datetime.strptime(datetime_str, f)
        except ValueError:
            pass
    raise RuntimeError(
        "Decode '{0}' failed: format should be one of '{1}'".format(
            datetime_str, str(formats)
        )
    )


def encode_datetime(dt):
    if isinstance(dt, datetime.datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


def random_string(nlen):
    return "".join([random.choice(string.ascii_lowercase) for _ in range(nlen)])


def str_to_bool(s):
    if isinstance(s, bool):
        return s
    return s in ["true", "True", "y", "Y", "yes", "Yes", "1"]


def get_internal_ip() -> str:
    hostname = socket.gethostname()
    internal_ip = socket.gethostbyname(hostname)
    return internal_ip
