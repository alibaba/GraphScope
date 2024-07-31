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
import os
import random
import re
import socket
import string
from typing import Union

import requests


def handle_api_exception():
    """Decorator to handle api exception for openapi controllers."""

    def _handle_api_exception(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logging.info(str(e))
                return str(e), 500

        return wrapper

    return _handle_api_exception


def decode_datetimestr(datetime_str):
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
        "%Y-%m-%d-%H-%M-%S",
    ]
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


def get_current_time() -> datetime.datetime:
    return datetime.datetime.now()


def get_internal_ip() -> str:
    hostname = socket.gethostname()
    internal_ip = socket.gethostbyname(hostname)
    return internal_ip


def parse_file_metadata(location: str) -> dict:
    """
    Args:
        location: optional values:
            odps://path/to/file, hdfs://path/to/file, file:///path/to/file
            /home/graphscope/path/to/file
    """
    metadata = {"datasource": "file"}
    path = location
    pattern = r"^(odps|hdfs|file|oss|s3)?://([\w/.-]+)$"
    match = re.match(pattern, location)
    if match:
        datasource = match.group(1)
        metadata["datasource"] = datasource
        if datasource == "file":
            path = match.group(2)
    if metadata["datasource"] == "file":
        _, file_extension = os.path.splitext(path)
        metadata["file_type"] = file_extension[1:]
    return metadata


def get_public_ip() -> Union[str, None]:
    try:
        response = requests.get("https://api.ipify.org?format=json")
        if response.status_code == 200:
            data = response.json()
            return data["ip"]
        else:
            return None
    except requests.exceptions.RequestException as e:
        logging.warn("Failed to get public ip: %s", str(e))
        return None


def data_type_to_groot(t):
    if t == "DT_DOUBLE":
        return "long"
    elif t == "DT_STRING":
        return "str"
    elif t == "DT_SIGNED_INT32":
        return "long"
    elif t == "DT_SIGNED_INT64":
        return "long"
    else:
        raise RuntimeError(f"Data type {t} is not supported yet.")


class GraphInfo(object):
    def __init__(
        self, name, creation_time, update_time=None, last_dataloading_time=None
    ):
        self._name = name
        self._creation_time = creation_time
        self._update_time = update_time
        if self._update_time is None:
            self._update_time = self._creation_time
        self._last_dataloading_time = last_dataloading_time

    @property
    def name(self):
        return self._name

    @property
    def creation_time(self):
        return self._creation_time

    @property
    def update_time(self):
        return self._update_time

    @property
    def last_dataloading_time(self):
        return self._last_dataloading_time

    @update_time.setter
    def update_time(self, new_time):
        self._update_time = new_time

    @last_dataloading_time.setter
    def last_dataloading_time(self, new_time):
        if self._last_dataloading_time is None:
            self._last_dataloading_time = new_time
        elif new_time > self._last_dataloading_time:
            self._last_dataloading_time = new_time

    def to_dict(self):
        return {
            "name": self._name,
            "creation_time": encode_datetime(self._creation_time),
            "update_time": encode_datetime(self._update_time),
            "last_dataloading_time": encode_datetime(self._last_dataloading_time),
        }
