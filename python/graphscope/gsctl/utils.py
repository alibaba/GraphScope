#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited. All Rights Reserved.
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

import click
import yaml


def read_yaml_file(path) -> dict:
    """Reads YAML file and returns as a python object."""
    with open(path, "r") as file:
        return yaml.safe_load(file)


def write_yaml_file(data, path):
    """Writes python object to the YAML file."""
    with open(path, "w") as file:
        yaml.dump(data, file)


def is_valid_file_path(path) -> bool:
    """Check if the path exists and corresponds to a regular file."""
    return os.path.exists(path) and os.path.isfile(path)


def terminal_display(data):
    """Display tablular data in terminal.

    Args:
        data: two dimensional list of string type.
    """
    # Compute the maximum width for each column
    column_widths = [max(len(str(item)) for item in column) for column in zip(*data)]
    # Display the data with aligned columns
    for row in data:
        print(
            "  ".join(
                "{:<{}}".format(item, width) for item, width in zip(row, column_widths)
            )
        )
