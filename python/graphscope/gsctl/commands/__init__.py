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
from graphscope.gsctl.commands.insight.graph import cli as insight_graph
from graphscope.gsctl.commands.interactive.glob import cli as interactive
from graphscope.gsctl.commands.interactive.graph import cli as interactive_graph
from graphscope.gsctl.config import Context
from graphscope.gsctl.config import load_gs_config
from graphscope.gsctl.config import logo
from graphscope.gsctl.impl import connect_coordinator
from graphscope.gsctl.utils import err
from graphscope.gsctl.utils import info


def is_interactive_mode(flex):
    return (
        flex["engine"] == "Hiactor"
        and flex["storage"] == "MutableCSR"
        and flex["frontend"] == "Cypher/Gremlin"
    )


def is_insight_mode(flex):
    return (
        flex["engine"] == "Gaia"
        and flex["storage"] == "MutablePersistent"
        and flex["frontend"] == "Cypher/Gremlin"
    )


def get_command_collection(context: Context):
    # default commands
    commands = click.CommandCollection(sources=[common, dev])

    # treat gsctl as an utility script, providing helper functions or utilities
    # e.g. initialize and manage cluster, install the dependencies required to
    # build graphscope locally.
    if context is None:
        if len(sys.argv) == 1:
            info(logo, fg="green", bold=True)
            click.secho("Currently, gsctl hasn't connect to any service.", fg="yellow")
            message = """
you can use gsctl as an utility script.
Or you can connect to a launched GraphScopoe service by `gsctl connect --coordinator-endpoint <address>`.
See more detailed information at https://graphscope.io/docs/utilities/gs.
            """
            info(message)
        return commands

    if context.is_expired():
        try:
            # connect to coordinator and reset the timestamp
            response = connect_coordinator(context.coordinator_endpoint)
            flex = {
                "engine": response.engine,
                "storage": response.storage,
                "frontend": response.frontend,
            }
        except Exception as e:
            err(
                "Failed to connect to coordinator at {0}: {1}".format(
                    context.coordinator_endpoint, str(e)
                )
            )
            info(
                "Please check the availability of the service, fall back to the default commands."
            )
            return commands
        else:
            # check consistency
            if flex != context.flex:
                raise RuntimeError(
                    f"Instance changed: {context.flex} -> {flex}, please close and reconnect to the coordinator"
                )
            context.reset_timestamp()
            config = load_gs_config()
            config.update_and_write(context)

    if is_interactive_mode(context.flex):
        if context.context == "global":
            if len(sys.argv) < 2 or sys.argv[1] != "use":
                info("Using GLOBAL.", fg="green", bold=True)
                info(
                    "Run `gsctl use GRAPH <graph_identifier>` to switch to a specific graph context.\n"
                )
            commands = click.CommandCollection(sources=[common, interactive])
        else:
            if len(sys.argv) < 2 or sys.argv[1] != "use":
                info(
                    f"Using GRAPH {context.graph_name}(id={context.context}).",
                    fg="green",
                    bold=True,
                )
                info("Run `gsctl use GLOBAL` to switch back to GLOBAL context.\n")
            commands = click.CommandCollection(sources=[common, interactive_graph])
    elif is_insight_mode(context.flex):
        if context.context == "global":
            if len(sys.argv) < 2 or sys.argv[1] != "use":
                info("Using GLOBAL.", fg="green", bold=True)
                info(
                    "Run `gsctl use GRAPH <graph_identifier>` to switch to a specific graph context.\n"
                )
            commands = click.CommandCollection(sources=[common, insight])
        else:
            if len(sys.argv) < 2 or sys.argv[1] != "use":
                info(
                    f"Using GRAPH {context.graph_name}(id={context.context}).",
                    fg="green",
                    bold=True,
                )
                info("Run `gsctl use GLOBAL` to switch back to GLOBAL context.\n")
            commands = click.CommandCollection(sources=[common, insight_graph])

    return commands
