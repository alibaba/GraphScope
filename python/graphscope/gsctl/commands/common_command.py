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

"""Group of commands used by all products under the FLEX architecture"""

import click

from graphscope.gsctl.config import Context
from graphscope.gsctl.config import load_gs_config
from graphscope.gsctl.rpc import get_grpc_client


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
    """Connect to the launched coordinator by ~/.graphscope/config. If '--coordinator-endpoint' is specified,
    use it as the current context and override the config file.
    """
    if coordinator_endpoint is not None:
        click.secho(
            f"Connect to the coordinator at {coordinator_endpoint}.", fg="green"
        )

    grpc_client = get_grpc_client(coordinator_endpoint)
    solution = grpc_client.connect()

    if coordinator_endpoint is not None:
        context = Context(solution=solution, coordinator_endpoint=coordinator_endpoint)
        config = load_gs_config()
        config.set_and_write(context)

    click.secho("Coordinator service connected.", fg="green")


@click.command()
def close():
    """Close the connection from the coordinator."""
    config = load_gs_config()

    current_context = config.current_context()
    if current_context is None:
        return

    config.remove_and_write(current_context)
    click.secho(f"Disconnect from the {current_context.to_dict()}.", fg="green")


cli.add_command(connect)
cli.add_command(close)


if __name__ == "__main__":
    cli()
