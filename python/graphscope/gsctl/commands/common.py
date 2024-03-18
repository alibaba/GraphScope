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


@cli.command()
@click.option(
    "-c",
    "--coordinator-endpoint",
    help="Coordinator endpoint which gsctl connect to, e.g. http://127.0.0.1:9527",
)
def connect(coordinator_endpoint):
    """Connect to a launched coordinator

    By default, it will read context from  ~/.gsctl. If '--coordinator-endpoint'
    is specified, use it as the current context and override the configuration file.
    """
    if coordinator_endpoint is None:
        context = get_current_context()
        if context is None:
            click.secho(
                "No available context, try to connect to coordinator with --coordinator-endpoint",
                fg="blue",
            )
            return
        coordinator_endpoint = context.coordinator_endpoint
    # connect
    try:
        resp = connect_coordinator(coordinator_endpoint)
    except Exception as e:
        click.secho(f"Unable to connect to server: {str(e)}", fg="red")
    else:
        click.secho(
            f"Coordinator at {coordinator_endpoint} connected, {resp.solution} service is serving.",
            fg="green",
        )


@cli.command()
def close():
    """Disconnect from coordinator"""
    try:
        context = disconnect_coordinator()
    except Exception as e:
        click.secho(f"Disconnect to server failed: {str(e)}", fg="red")
    else:
        if context is not None:
            click.secho(f"Coordinator disconnected: {context.to_dict()}.", fg="green")


if __name__ == "__main__":
    cli()
