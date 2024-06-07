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
from graphscope.gsctl.impl import bind_datasource_in_batch
from graphscope.gsctl.impl import create_stored_procedure
from graphscope.gsctl.impl import delete_job_by_id
from graphscope.gsctl.impl import delete_stored_procedure_by_id
from graphscope.gsctl.impl import get_datasource_by_id
from graphscope.gsctl.impl import get_graph_id_by_name
from graphscope.gsctl.impl import get_job_by_id
from graphscope.gsctl.impl import list_graphs
from graphscope.gsctl.impl import list_jobs
from graphscope.gsctl.impl import list_stored_procedures
from graphscope.gsctl.impl import submit_dataloading_job
from graphscope.gsctl.impl import switch_context
from graphscope.gsctl.impl import unbind_edge_datasource
from graphscope.gsctl.impl import unbind_vertex_datasource
from graphscope.gsctl.utils import TreeDisplay
from graphscope.gsctl.utils import err
from graphscope.gsctl.utils import info
from graphscope.gsctl.utils import is_valid_file_path
from graphscope.gsctl.utils import read_yaml_file
from graphscope.gsctl.utils import succ


@click.group()
def cli():
    pass


@cli.group()
def create():
    """Create stored procedure, data source, loader job from file"""
    pass


@cli.group()
def delete():
    """Delete stored procedure, data source, loader job by id"""
    pass


@cli.group()
def desc():
    """Show details of job status and stored procedure by id"""
    pass


@cli.group()
def use():
    """Switch back to the global scope"""
    pass


@cli.command()
def ls():  # noqa: F811
    """Display schema, stored procedure, and job information"""
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
        # job
        jobs = list_jobs()
        tree.create_job_node(using_graph, jobs)
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
        delete_stored_procedure_by_id(graph_identifier, identifier)
    except Exception as e:
        err(f"Failed to delete stored procedure: {str(e)}")
    else:
        succ(f"Delete stored procedure {identifier} successfully.")


@create.command
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def datasource(filename):  # noqa: F811
    """Bind data source mapping from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        datasource = read_yaml_file(filename)
        bind_datasource_in_batch(graph_identifier, datasource)
    except Exception as e:
        err(f"Failed to bind data source: {str(e)}")
    else:
        succ("Bind data source successfully.")


@delete.command
@click.option(
    "-t",
    "--type",
    required=True,
    help="Vertex or edge type",
)
@click.option(
    "-s",
    "--source_vertex_type",
    required=False,
    help="Source vertex type of the edge [edge only]",
)
@click.option(
    "-d",
    "--destination_vertex_type",
    required=False,
    help="Destination vertex type of the edge [edge only]",
)
def datasource(type, source_vertex_type, destination_vertex_type):  # noqa: F811
    """Unbind data source mapping on vertex or edge type"""
    try:
        current_context = get_current_context()
        graph_identifier = current_context.context
        if source_vertex_type is not None and destination_vertex_type is not None:
            unbind_edge_datasource(
                graph_identifier, type, source_vertex_type, destination_vertex_type
            )
        else:
            unbind_vertex_datasource(graph_identifier, type)
    except Exception as e:
        err(f"Failed to unbind data source: {str(e)}")
    else:
        succ("Unbind data source successfully.")


@create.command()
@click.option(
    "-f",
    "--filename",
    required=True,
    help="Path of yaml file",
)
def loaderjob(filename):  # noqa: F811
    """Create a data loading job from file"""
    if not is_valid_file_path(filename):
        err(f"Invalid file: {filename}")
        return
    current_context = get_current_context()
    graph_identifier = current_context.context
    try:
        config = read_yaml_file(filename)
        jobid = submit_dataloading_job(graph_identifier, config)
    except Exception as e:
        err(f"Failed to create a job: {str(e)}")
    else:
        succ(f"Create job {jobid} successfully.")


@delete.command()
@click.argument("identifier", required=True)
def job(identifier):  # noqa: F811
    """Cancel a job, see identifier with `ls` command"""
    try:
        delete_job_by_id(identifier)
    except Exception as e:
        err(f"Failed to delete job {identifier}: {str(e)}")
    else:
        succ(f"Delete job {identifier} successfully.")


@desc.command()
@click.argument("identifier", required=True)
def job(identifier):  # noqa: F811
    """Show details of job, see identifier with `ls` command"""
    try:
        job = get_job_by_id(identifier)
    except Exception as e:
        err(f"Failed to get job: {str(e)}")
    else:
        info(yaml.dump(job.to_dict()))


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


@use.command(name="GLOBAL")
def _global():
    """Switch back to the global scope"""
    switch_context("global")
    click.secho("Using GLOBAL", fg="green")


if __name__ == "__main__":
    cli()
