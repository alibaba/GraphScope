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
import sys

import click

try:
    import graphscope
except ModuleNotFoundError:
    # if graphscope is not installed, only basic functions or utilities
    # can be used, e.g. install dependencies
    graphscope = None


def cli():
    if graphscope is None:
        sys.path.insert(
            0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "commands")
        )
        from dev_command import cli as dev_cli

        dev_cli()

    from graphscope.gsctl.commands import get_command_collection
    from graphscope.gsctl.config import get_current_context

    context = get_current_context()
    # get the specified commands under the FLEX architecture
    commands = get_command_collection(context)
    # serve the command
    commands()


if __name__ == "__main__":
    cli()
