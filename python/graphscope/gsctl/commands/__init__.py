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

import sys

import click

from graphscope.gsctl.commands.common import cli as common
from graphscope.gsctl.commands.dev import cli as dev
from graphscope.gsctl.commands.insight.glob import cli as insight
from graphscope.gsctl.commands.interactive.glob import cli as interactive
from graphscope.gsctl.commands.interactive.graph import cli as interactive_graph
from graphscope.gsctl.config import Context
from graphscope.gsctl.config import load_gs_config
from graphscope.gsctl.config import logo
from graphscope.gsctl.impl import connect_coordinator


def get_command_collection(context: Context):
    # default commands
    commands = click.CommandCollection(sources=[common, dev])

    # treat gsctl as an utility script, providing helper functions or utilities
    # e.g. initialize and manage cluster, install the dependencies required to
    # build graphscope locally.
    if context is None:
        if len(sys.argv) == 1:
            click.secho(logo, fg="green", bold=True)
            message = """
Currently, gsctl hasn't connect to any service, you can use gsctl as an utility script.
Or you can connect to a launched GraphScopoe service by `gsctl connect --coordinator-endpoint <address>`.
See more detailed informations at https://graphscope.io/docs/utilities/gs.
            """
            click.secho(message, fg="green")
        return commands

    if context.is_expired():
        try:
            # connect to coordinator and reset the timestamp
            response = connect_coordinator(context.coordinator_endpoint)
            solution = response.solution
        except Exception as e:
            click.secho(
                "Failed to connect to coordinator at {0}: {1}".format(
                    context.coordinator_endpoint, str(e)
                ),
                fg="red",
            )
            click.secho("Please check the availability of the service.", fg="red")
            click.secho("Fall back to the default commands.", fg="red")
            return commands
        else:
            # check consistency
            if solution != context.flex:
                raise RuntimeError(
                    f"Instance changed: {context.flex} -> {solution}, please close and reconnect to the coordinator"
                )
            context.reset_timestamp()
            config = load_gs_config()
            config.update_and_write(context)

    if context.flex == "INTERACTIVE":
        if context.context == "global":
            if len(sys.argv) == 1:
                message = f"Using global, to change to a specific graph context, run `gsctl use graph <graph_identifier>`.\n"
                click.secho(message, fg="green")
            commands = click.CommandCollection(sources=[common, interactive])
        else:
            if len(sys.argv) == 1:
                message = f"Using graph {context.context}, to switch back to the global, run `gsctl use global`.\n"
                click.secho(message, fg="green")
            commands = click.CommandCollection(sources=[common, interactive_graph])
    elif context.flex == "GRAPHSCOPE_INSIGHT":
        if context.context == "global":
            commands = click.CommandCollection(sources=[common, insight])

    return commands
