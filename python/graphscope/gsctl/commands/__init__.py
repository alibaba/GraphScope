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
import sys

from graphscope.gsctl.config import Context

from graphscope.gsctl.commands.common import cli as common
from graphscope.gsctl.commands.dev import cli as dev
from graphscope.gsctl.commands.interactive import cli as interactive
from graphscope.gsctl.client.rpc import get_grpc_client


def get_command_collection(context: Context):
    if context is None:
        # treat gsctl as an utility script, providing hepler functions or utilities. e.g.
        # initialize and manage cluster, install the dependencies required to build graphscope locally
        return click.CommandCollection(sources=[common, dev])

    grpc_client = get_grpc_client(context.coordinator_endpoint)
    solution = grpc_client.connection_available()
    # in general, we should use 'solution' returned from the coordinator
    # to determine the behavior in gsctl, but sometimes coordinator may crash
    # or be closed manually, thus, we use the 'solution' exists in the client
    # as default.
    if solution is None:
        solution = context.solution

    if solution == "interactive":
        commands = click.CommandCollection(sources=[common, interactive])
    else:
        raise RuntimeError(
            f"Failed to get command collection with context {context.name}"
        )

    return commands
