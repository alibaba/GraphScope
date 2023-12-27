#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2023 Alibaba Group Holding Limited. All Rights Reserved.
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

import os
from typing import List

import click
import yaml

from google.protobuf.internal.containers import ScalarMap

from graphscope.proto import flex_pb2


def read_yaml_file(path):
    """Reads YAML file and returns as a python object."""
    try:
        with open(path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        click.secho(str(e), fg="red")


def write_yaml_file(data, path):
    """Writes python object to the YAML file."""
    with open(path, "w") as file:
        yaml.dump(data, file)


def is_valid_file_path(path):
    """Check if the path exists and corresponds to a regular file."""
    return os.path.exists(path) and os.path.isfile(path)


def dict_to_proto_message(values, message):
    """Transform pyhon dict object to protobuf message

    Args:
        values (dict): values to be transformed.
        message (proto): protobuf message, such as graph_def_pb2.GraphMessage()
    """

    def _parse_list(values, message):
        if isinstance(values[0], dict):  # value needs to be further parsed
            for v in values:
                cmd = message.add()
                _parse_dict(v, cmd)
        else:  # value can be set
            message.extend(values)

    def _parse_dict(values, message):
        for k, v in values.items():
            if isinstance(v, dict):  # value needs to be further parsed
                _parse_dict(v, getattr(message, k))
            elif isinstance(v, list):
                _parse_list(v, getattr(message, k))
            else:  # value can be set
                if hasattr(message, k):
                    setattr(message, k, v)
                else:
                    try:
                        # treat as a map
                        if isinstance(v, bool):
                            message[k].CopyFrom(flex_pb2.MapValue(b=v))
                        elif isinstance(v, int):
                            message[k].CopyFrom(flex_pb2.MapValue(i=v))
                        elif isinstance(v, str):
                            message[k].CopyFrom(flex_pb2.MapValue(s=v))
                    except AttributeError as e:
                        click.secho(str(e), fg="red")

    _parse_dict(values, message)
    return message


def terminal_display(data: List[list]):
    """Display tablular data in terminal"""

    # Compute the maximum width for each column
    column_widths = [max(len(str(item)) for item in column) for column in zip(*data)]

    # Display the data with aligned columns
    for row in data:
        print(
            "  ".join(
                "{:<{}}".format(item, width) for item, width in zip(row, column_widths)
            )
        )
