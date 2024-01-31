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

"""Group of commands used by all products under the FLEX architecture"""

import click

from graphscope.gsctl.config import Context
from graphscope.gsctl.config import get_current_context
from graphscope.gsctl.config import load_gs_config
from graphscope.gsctl.impl import connect_coordinator
from graphscope.gsctl.impl import disconnect_coordinator


@click.group()
def cli():
    # nothing happens
    pass


@click.command()
@click.option(
    "--coordinator-endpoint",
    help="Coordinator endpoint which gsctl connect to, e.g. http://127.0.0.1:9527",
)
def connect(coordinator_endpoint):
    """Connect to the launched coordinator.

    By default, it will read context from  ~/.graphscope/config. If '--coordinator-endpoint'
    is specified, use it as the current context and override the config file.
    """
    if coordinator_endpoint is None:
        context = get_current_context()
        if context is None:
            click.secho(
                "No available context found, try to connect to coordinator with --coordinator-endpoint",
                fg="blug",
            )
            return
        coordinator_endpoint = context.coordinator_endpoint
    # connect
    try:
        connect_coordinator(coordinator_endpoint)
    except Exception as e:
        click.secho(f"Unable to connect to server: {str(e)}", fg="red")
    else:
        click.secho(f"Coordinator at {coordinator_endpoint} connected.", fg="green")


@click.command()
def close():
    """Close the connection from the coordinator."""
    try:
        context = disconnect_coordinator()
    except Exception as e:
        click.secho(f"Disconnect to server failed: {str(e)}", fg="red")
    else:
        if context is not None:
            click.secho(f"Coordinator disconnected: {context.to_dict()}.", fg="green")


cli.add_command(connect)
cli.add_command(close)


if __name__ == "__main__":
    cli()
