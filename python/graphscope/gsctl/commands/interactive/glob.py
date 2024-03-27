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
import yaml

from graphscope.gsctl.impl import create_graph
from graphscope.gsctl.impl import delete_graph_by_name
from graphscope.gsctl.impl import get_dataloading_config
from graphscope.gsctl.impl import get_service_status
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import list_jobs
from graphscope.gsctl.impl import list_procedures
from graphscope.gsctl.impl import restart_service
from graphscope.gsctl.impl import start_service
from graphscope.gsctl.impl import stop_service
from graphscope.gsctl.impl import switch_context
from graphscope.gsctl.utils import TreeDisplay
from graphscope.gsctl.utils import err
from graphscope.gsctl.utils import info
from graphscope.gsctl.utils import is_valid_file_path
from graphscope.gsctl.utils import read_yaml_file
from graphscope.gsctl.utils import succ
from graphscope.gsctl.utils import terminal_display


@click.group()
def cli():
    pass


@cli.group()
def create():
    """Create a new graph in database"""
    pass


@cli.group()
def delete():
    """Delete a graph by identifier"""
    pass


@cli.group()
def service():
    """Start, stop, and restart the database service"""
    pass


@cli.command()
@click.argument(
    "context", type=click.Choice(["GRAPH"], case_sensitive=False), required=True
)
@click.argument("GRAPH_IDENTIFIER", required=True)
def use(context, graph_identifier):
    """Switch to GRAPH context, see identifier with `ls` command"""
    try:
        graphs = list_graphs()
        graph_exist = False
        for g in graphs:
            if graph_identifier == g.name:
                graph_exist = True
                break
        if not graph_exist:
            raise RuntimeError(
                f"Graph '{graph_identifier}' not exists, see graph identifier with `ls` command."
            )
        switch_context(graph_identifier)
    except Exception as e:
        err(f"Failed to switch context: {str(e)}")
    else:
        click.secho(f"Using GRAPH {graph_identifier}", fg="green")


@cli.command()
@click.option("-l", is_flag=True, help="List sub resources recursively")
def ls(l):  # noqa: F811, E741
    """Display graph resources in database"""
    tree = TreeDisplay()
    try:
        graphs = list_graphs()
        for g in graphs:
            # schema
            tree.create_graph_node(g, recursive=l)
            if l:
                # get data source from job configuration
                job_config = get_dataloading_config(g.name)
                tree.create_datasource_node_for_interactive(g, job_config)
                # stored procedure
                procedures = list_procedures(g.name)
                tree.create_procedure_node(g, procedures)
                # job
                jobs = list_jobs()
                tree.create_job_node(g, jobs)
    except Exception as e:
        err(f"Failed to list resources: {str(e)}")
    else:
        tree.show()
        if not l:
            info("Run `gsctl ls -l` to list all resources recursively.")


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def graph(filename):  # noqa: F811
    """Create a new graph in database"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    try:
        graph = read_yaml_file(filename)
        create_graph(graph)
    except Exception as e:
        err(f"Failed to create graph: {str(e)}")
    else:
        succ(f"Create graph {graph['name']} successfully.")


@delete.command()
@click.argument("graph_identifier", required=True)
def graph(graph_identifier):  # noqa: F811
    """Delete a graph, see graph identifier with `ls` command"""
    try:
        delete_graph_by_name(graph_identifier)
    except Exception as e:
        err(f"Failed to delete graph {graph_identifier}: {str(e)}")
    else:
        succ(f"Delete graph {graph_identifier} successfully.")


@service.command
def stop():  # noqa: F811
    """Stop current database service"""
    try:
        stop_service()
    except Exception as e:
        err(f"Failed to stop service: {str(e)}")
    else:
        succ("Service stopped.")


@service.command
@click.option(
    "-g",
    "--graph_identifier",
    required=True,
    help="See graph identifier with `ls` command",
)
def start(graph_identifier):  # noqa: F811
    """Start database service on a certain graph"""
    try:
        start_service(graph_identifier)
    except Exception as e:
        err(f"Failed to start service on graph {graph_identifier}: {str(e)}")
    else:
        succ(f"Start service on graph {graph_identifier} successfully")


@service.command
def restart():  # noqa: F811
    """Restart database service on current graph"""
    try:
        restart_service()
    except Exception as e:
        err(f"Failed to restart service: {str(e)}")
    else:
        succ("Service restarted.")


@service.command
def ls():  # noqa: F811
    """Display current service status"""

    def _construct_and_display_data(status):
        head = ["STATUS", "SERVING_GRAPH", "CYPHER_ENDPOINT", "HQPS_ENDPOINT"]
        data = [head]
        data.append(
            [
                status.status,
                status.graph_name,
                status.sdk_endpoints.cypher,
                status.sdk_endpoints.hqps,
            ]
        )
        terminal_display(data)

    try:
        status = get_service_status()
    except Exception as e:
        err(f"Failed to get service status: {str(e)}")
    else:
        _construct_and_display_data(status)


if __name__ == "__main__":
    cli()
