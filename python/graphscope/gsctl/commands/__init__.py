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

import click

from graphscope.gsctl.commands.common import cli as common
from graphscope.gsctl.commands.dev import cli as dev
from graphscope.gsctl.commands.interactive import cli as interactive
from graphscope.gsctl.config import Context
from graphscope.gsctl.impl import connect_coordinator


def get_command_collection(context: Context):
    # default commands
    commands = click.CommandCollection(sources=[common, dev])

    # treat gsctl as an utility script, providing hepler functions or utilities
    # e.g. initialize and manage cluster, install the dependencies required to
    # build graphscope locally.
    if context is None:
        return commands

    # connect to coordinator and parse the commands with solution
    try:
        response = connect_coordinator(context.coordinator_endpoint)
        solution = response.solution
        if solution == "INTERACTIVE":
            commands = click.CommandCollection(sources=[common, interactive])
    except Exception as e:
        click.secho(
            "Failed to connect to coordinator at {0}: {1}".format(
                context.coordinator_endpoint, str(e)
            ),
            fg="red",
        )
        click.secho(
            "Please check the availability of the service, or close/reconnect the service.",
            fg="blue",
        )
        click.secho("Fall back to the default commands.", fg="blue")
    finally:
        return commands
