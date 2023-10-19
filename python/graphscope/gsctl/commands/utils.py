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

import click

from graphscope.gsctl.commands.common_command import cli as common_cli
from graphscope.gsctl.commands.dev_command import cli as dev_cli
from graphscope.gsctl.config import Context


def get_command_collection(context: Context):
    if context is None:
        # treat gsctl as an utility script, providing hepler functions or utilities. e.g.
        # initialize and manage cluster, install the dependencies required to build graphscope locally
        commands = click.CommandCollection(sources=[common_cli, dev_cli])

    elif context.solution == "interactive":
        commands = click.CommandCollection(sources=[common_cli])

    else:
        raise RuntimeError(
            f"Failed to get command collection with context {context.name}"
        )

    return commands
