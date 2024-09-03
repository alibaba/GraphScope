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

from graphscope.gsctl.config import get_current_context
from graphscope.gsctl.impl import create_stored_procedure
from graphscope.gsctl.impl import delete_stored_procedure_by_id
from graphscope.gsctl.impl import get_datasource_by_id
from graphscope.gsctl.impl import get_graph_name_by_id
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import list_service_status
from graphscope.gsctl.impl import list_stored_procedures
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
    """Create stored procedure from file"""
    pass


@cli.group()
def delete():
    """Delete stored procedure by id"""
    pass


@cli.group()
def desc():
    """Show stored procedure's details by id"""
    pass


@cli.group()
def service():
    """Start, stop, and restart the database service"""
    pass


@cli.group()
def use():
    """Switch back to the global scope"""
    pass


@cli.command()
def ls():  # noqa: F811
    """Display schema and stored procedure information"""
    tree = TreeDisplay()
    # context
    current_context = get_current_context()
    try:
        graphs = list_graphs()
        using_graph = None
        for g in graphs:
            if g.id == current_context.context:
                using_graph = g
                break
        # schema
        tree.create_graph_node(using_graph)
        # data source mapping
        datasource_mapping = get_datasource_by_id(using_graph.id)
        tree.create_datasource_mapping_node(using_graph, datasource_mapping)
        # stored procedure
        stored_procedures = list_stored_procedures(using_graph.id)
        tree.create_stored_procedure_node(using_graph, stored_procedures)
    except Exception as e:
        err(f"Failed to display graph information: {str(e)}")
    else:
        tree.show(graph_identifier=current_context.context)


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def storedproc(filename):
    """Create a stored procedure from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        stored_procedure = read_yaml_file(filename)
        create_stored_procedure(graph_identifier, stored_procedure)
    except Exception as e:
        err(f"Failed to create stored procedure: {str(e)}")
    else:
        succ(f"Create stored procedure {stored_procedure['name']} successfully.")


@delete.command()
@click.argument("identifier", required=True)
def storedproc(identifier):  # noqa: F811
    """Delete a stored procedure, see identifier with `ls` command"""
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        if click.confirm("Do you want to continue?"):
            delete_stored_procedure_by_id(graph_identifier, identifier)
            succ(f"Delete stored procedure {identifier} successfully.")
    except Exception as e:
        err(f"Failed to delete stored procedure: {str(e)}")


@desc.command()
@click.argument("identifier", required=True)
def storedproc(identifier):  # noqa: F811
    """Show details of stored procedure, see identifier with `ls` command"""
    current_context = get_current_context()
    graph_id = current_context.context
    try:
        stored_procedures = list_stored_procedures(graph_id)
    except Exception as e:
        err(f"Failed to list stored procedures: {str(e)}")
    else:
        if not stored_procedures:
            info(f"No stored procedures found on {graph_id}.")
            return
        specific_stored_procedure_exist = False
        for stored_procedure in stored_procedures:
            if identifier == stored_procedure.id:
                info(yaml.dump(stored_procedure.to_dict()))
                specific_stored_procedure_exist = True
                break
        if not specific_stored_procedure_exist:
            err(f"Stored Procedure {identifier} not found on {graph_id}.")


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
def start():  # noqa: F811
    """Start current database service"""
    try:
        current_context = get_current_context()
        graph_identifier = current_context.context

        status = list_service_status()
        for s in status:
            if s.graph_id == graph_identifier:
                if s.status != "Running":
                    info(f"Starting service on graph {graph_identifier}...")
                    start_service(graph_identifier)
                    succ("Service restarted.")
                else:
                    info("Service is running...")
    except Exception as e:
        err(f"Failed to start service: {str(e)}")


@service.command
def restart():  # noqa: F811
    """Start current database service"""
    try:
        restart_service()
    except Exception as e:
        err(f"Failed to restart service: {str(e)}")
    else:
        succ("Service restarted.")


@service.command
def status():  # noqa: F811
    """Display current service status"""

    def _construct_and_display_data(status):
        current_context = get_current_context()
        graph_identifier = current_context.context
        graph_name = current_context.graph_name

        head = [
            "STATUS",
            "SERVING_GRAPH(IDENTIFIER)",
            "CYPHER_ENDPOINT",
            "HQPS_ENDPOINT",
            "GREMLIN_ENDPOINT",
        ]
        data = [head]
        for s in status:
            if s.graph_id == graph_identifier:
                if s.status == "Stopped":
                    data.append(
                        [s.status, f"{graph_name}(id={s.graph_id})", "-", "-", "-"]
                    )
                else:
                    data.append(
                        [
                            s.status,
                            f"{graph_name}(id={s.graph_id})",
                            s.sdk_endpoints.cypher,
                            s.sdk_endpoints.hqps,
                            s.sdk_endpoints.gremlin,
                        ]
                    )
        terminal_display(data)

    try:
        status = list_service_status()
    except Exception as e:
        err(f"Failed to list service status: {str(e)}")
    else:
        _construct_and_display_data(status)


@use.command(name="GLOBAL")
def _global():
    """Switch back to the global scope"""
    switch_context("global")
    click.secho("Using GLOBAL", fg="green")


if __name__ == "__main__":
    cli()
